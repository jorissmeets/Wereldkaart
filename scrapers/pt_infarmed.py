"""Scraper for Portugal INFARMED medicine shortage data via SIATS system."""

import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class PtInfarmedScraper(BaseScraper):
    """Scraper for INFARMED (Portugal) SIATS shortage data.

    Uses form POST searches by authorization holder (TAIM) to retrieve
    current shortage records from the ASP.NET WebForms page.
    """

    PAGE_URL = "https://extranet.infarmed.pt/siats/Publico/Rupturas.aspx"
    TAIM_API = "https://extranet.infarmed.pt/siats/WebService.asmx/GetTaimRuturasPublico"

    # Expected column order in results grid (non-empty cells only)
    COLUMNS = [
        "dci", "medicine_name", "holder", "reg_no",
        "cnpem", "chnm", "start_date", "expected_end",
        "actual_end", "reason", "mitigation",
    ]

    def __init__(self):
        super().__init__(
            country_code="PT",
            country_name="Portugal",
            source_name="INFARMED",
            base_url="https://extranet.infarmed.pt",
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def _get_all_taims(self) -> list[dict]:
        """Get all unique authorization holders by querying each letter a-z."""
        seen = set()
        taims = []
        for letter in "abcdefghijklmnopqrstuvwxyz":
            try:
                resp = self.session.post(
                    self.TAIM_API,
                    json={"prefixText": letter, "count": 1000, "contextKey": "pt-PT"},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json().get("d", [])
                for item in data:
                    if isinstance(item, str):
                        item = json.loads(item)
                    name = item.get("First", "")
                    guid = item.get("Second", "")
                    if name and guid and guid not in seen:
                        seen.add(guid)
                        taims.append({"name": name, "guid": guid})
            except Exception:
                continue
        return taims

    def _get_form_state(self) -> dict:
        """Load the page and extract ASP.NET viewstate fields."""
        resp = self.session.get(self.PAGE_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        fields = {}
        for inp in soup.find_all("input"):
            name = inp.get("name", "")
            itype = (inp.get("type") or "").lower()
            # Only collect hidden and text fields, not buttons
            if name and itype in ("hidden", "text"):
                fields[name] = inp.get("value", "")
        return fields

    def _search_by_taim(self, taim_name: str, taim_guid: str, form_fields: dict) -> list[list[str]]:
        """Search shortages for a specific TAIM and return parsed rows."""
        prefix = "ctl00$ContentPlaceHolder1$"
        data = dict(form_fields)
        # Clear all text/hidden search fields first
        for key in list(data.keys()):
            if "txt" in key or "hf" in key.lower():
                data[key] = ""
        # Set search parameters
        data[f"{prefix}txtTitularAIMRuturas"] = taim_name
        data[f"{prefix}hfTaimId"] = taim_guid
        data[f"{prefix}btnPesquisarRuturas"] = "Pesquisar"

        resp = self.session.post(self.PAGE_URL, data=data, timeout=30)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        grid = soup.find(id="ctl00_ContentPlaceHolder1_gridViewPesquisas")
        if not grid:
            return []

        text = grid.get_text(strip=True)
        if "existem" in text.lower():
            return []

        rows = grid.find_all("tr")
        if len(rows) < 2:
            return []

        results = []
        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            # Every other cell is a spacer — take even-indexed cells only
            values = [cells[i].get_text(strip=True) for i in range(0, len(cells), 2)]
            if len(values) >= 5:
                results.append(values)

        return results

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str or date_str == "-":
            return None
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Step 1: Get all authorization holders
        taims = self._get_all_taims()
        print(f"  Found {len(taims)} unique authorization holders")

        # Step 2: Search for each TAIM
        all_rows = []
        seen_keys = set()
        errors = 0

        for i, taim in enumerate(taims):
            try:
                # Get fresh form state for each request (viewstate changes)
                form_fields = self._get_form_state()
                results = self._search_by_taim(taim["name"], taim["guid"], form_fields)

                for row in results:
                    # Deduplicate by reg_no + medicine_name
                    key = tuple(row[:4])
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    all_rows.append(row)

            except Exception:
                errors += 1
                if errors > 30:
                    print(f"  Too many errors ({errors}), stopping")
                    break
                continue

            if (i + 1) % 50 == 0:
                print(f"  Searched {i + 1}/{len(taims)} holders, {len(all_rows)} records...")

        print(f"  Collected {len(all_rows)} unique records from {len(taims)} holders")

        # Step 3: Normalize records
        records = []
        for row in all_rows:
            # Pad row to expected length
            while len(row) < len(self.COLUMNS):
                row.append("")

            status = "shortage"
            if row[8]:  # actual_end date present
                status = "resolved"

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": row[1],
                "active_substance": row[0],
                "strength": "",
                "package_size": "",
                "product_no": row[3],
                "cnpem": row[4],
                "chnm": row[5],
                "marketing_auth_holder": row[2],
                "shortage_start": self._parse_date(row[6]),
                "estimated_end": self._parse_date(row[7]),
                "actual_end": self._parse_date(row[8]),
                "status": status,
                "reason": row[9],
                "mitigation": row[10] if len(row) > 10 else "",
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
