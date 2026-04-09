"""Scraper for Switzerland drugshortage.ch medicine shortage data."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class ChDrugShortageScraper(BaseScraper):
    """Scraper for drugshortage.ch (Switzerland) current medicine shortages."""

    URL = "https://drugshortage.ch/UebersichtaktuelleLieferengpaesse2.aspx"

    def __init__(self):
        super().__init__(
            country_code="CH",
            country_name="Switzerland",
            source_name="drugshortage",
            base_url="https://drugshortage.ch",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str or date_str.lower() in ("unbestimmt", "kontigentiert", "kontingentiert"):
            return None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _scrape_detail_substance(self, detail_url: str) -> str:
        """Scrape active substance (Wirkstoff) from a detail page."""
        if not detail_url:
            return ""
        try:
            resp = requests.get(detail_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)
            # Look for "Wirkstoff:" or "Wirkstoff(e):" pattern
            m = re.search(r"Wirkstoff(?:e|\(e\))?:\s*([^\n;]+?)(?:\s*(?:ATC|Galenik|Zulassung|Firma|$))", text)
            if m:
                return m.group(1).strip().rstrip(",. ")
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(self.URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", id="GridView1")
        if not table:
            raise ValueError("Could not find GridView1 table")

        rows = table.find_all("tr")
        print(f"  Found {len(rows) - 1} table rows")

        parsed_rows = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 11:
                continue

            designation = cells[0].get_text(strip=True)
            delivery_date = cells[1].get_text(strip=True)
            status_text = cells[3].get_text(strip=True)
            last_mutation = cells[4].get_text(strip=True)
            company = cells[5].get_text(strip=True)
            gtin = cells[6].get_text(strip=True)
            pharmacode = cells[7].get_text(strip=True)
            days_since_first = cells[8].get_text(strip=True)
            atc = cells[9].get_text(strip=True)
            gengrp = cells[10].get_text(strip=True)

            # Extract detail link
            detail_link = cells[0].find("a")
            detail_url = detail_link.get("href", "") if detail_link else ""
            full_detail_url = f"{self.base_url}/{detail_url}" if detail_url else ""

            # Parse status number from status text (e.g. "1 aktuell keine Lieferungen")
            status_code = status_text[0] if status_text and status_text[0].isdigit() else ""

            parsed_rows.append({
                "designation": designation,
                "delivery_date": delivery_date,
                "status_text": status_text,
                "last_mutation": last_mutation,
                "company": company,
                "gtin": gtin,
                "pharmacode": pharmacode,
                "atc": atc,
                "gengrp": gengrp,
                "status_code": status_code,
                "days_since_first": days_since_first,
                "detail_url": full_detail_url,
            })

        # Use gengrp as substance; for rows without it, scrape detail pages
        missing = [r for r in parsed_rows if not r["gengrp"] and r["detail_url"]]
        if missing:
            print(f"  Scraping {len(missing)} detail pages for missing substances...")
            for i, r in enumerate(missing):
                substance = self._scrape_detail_substance(r["detail_url"])
                r["_detail_substance"] = substance
                if (i + 1) % 20 == 0:
                    print(f"    ... {i + 1}/{len(missing)} detail pages done")
                time.sleep(0.2)

        records = []
        for r in parsed_rows:
            substance = r["gengrp"] or r.get("_detail_substance", "")
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": r["designation"],
                "active_substance": substance,
                "strength": "",
                "package_size": "",
                "dosage_form": "",
                "company_name": r["company"],
                "gtin": r["gtin"],
                "pharmacode": r["pharmacode"],
                "atc_code": r["atc"],
                "gengrp": r["gengrp"],
                "status": r["status_text"],
                "status_code": r["status_code"],
                "delivery_date": r["delivery_date"],
                "estimated_end": self._parse_date(r["delivery_date"]),
                "days_since_first_report": r["days_since_first"],
                "last_updated": self._parse_date(r["last_mutation"]),
                "detail_url": r["detail_url"],
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
