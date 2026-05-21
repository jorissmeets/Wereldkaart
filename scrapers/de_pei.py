"""Scraper for Germany Paul-Ehrlich-Institut (PEI) vaccine supply shortages.

Includes current shortages (4 category pages) and historical archive
(per-year iframe pages 2015 - current year, 2 categories each).
"""

import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class DePeiScraper(BaseScraper):
    """Scraper for PEI human vaccine Lieferengpässe (current + archive)."""

    CURRENT_URLS = {
        "grundimmunisierung": "https://abvl-public.pei.de/grundimmunisierung-de.html",
        "standard": "https://abvl-public.pei.de/standard-de.html",
        "reise_indikation": "https://abvl-public.pei.de/reise_indikation-de.html",
        "verknappungen": "https://abvl-public.pei.de/verknappungen-de.html",
    }

    ARCHIVE_FIRST_YEAR = 2015
    ARCHIVE_URL_TEMPLATE = "https://abvl-public.pei.de/archiv-{year}-kategorie-{cat}-de.html"
    ARCHIVE_CATEGORIES = {
        "1": "archive_grundimmunisierung",
        "2+3": "archive_standard_reise",
    }

    DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")

    def __init__(self):
        super().__init__(
            country_code="DE",
            country_name="Germany",
            source_name="PEI",
            base_url="https://www.pei.de",
        )

    def _parse_date(self, text: str) -> str | None:
        if not text:
            return None
        m = self.DATE_RE.search(text)
        if not m:
            return None
        try:
            return datetime.strptime(m.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _cell_text(self, cell) -> str:
        return cell.get_text(" ", strip=True) if cell else ""

    def _parse_name_cell(self, cell) -> tuple[str, str, str]:
        paragraphs = cell.find_all("p")
        name = paragraphs[0].get_text(" ", strip=True) if paragraphs else ""
        substance = paragraphs[1].get_text(" ", strip=True) if len(paragraphs) > 1 else ""
        age = paragraphs[2].get_text(" ", strip=True) if len(paragraphs) > 2 else ""
        return name, substance, age

    def _fetch_table(self, url: str) -> BeautifulSoup | None:
        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        return soup.find("table", id="notifications-table")

    def _parse_category(self, category: str, url: str) -> list[dict]:
        table = self._fetch_table(url)
        if table is None:
            return []

        tbody = table.find("tbody")
        if tbody is None:
            return []

        records: list[dict] = []
        parent: dict = {}

        for row in tbody.find_all("tr", recursive=False):
            cells = row.find_all("td", recursive=False)

            if len(cells) >= 6:
                name, substance, age = self._parse_name_cell(cells[0])
                pzn = self._cell_text(cells[1])
                start_text = self._cell_text(cells[2])
                end_text = self._cell_text(cells[3])
                mah = self._cell_text(cells[4])
                extra = self._cell_text(cells[5])

                if not pzn and not start_text and not end_text:
                    # Parent row: cache MAH + extra for following child rows
                    parent = {
                        "name": name,
                        "substance": substance,
                        "age": age,
                        "mah": mah,
                        "extra": extra,
                    }
                    continue

                records.append(self._build_record(
                    category=category, name=name, substance=substance, age=age,
                    pzn=pzn, start_text=start_text, end_text=end_text,
                    mah=mah, extra=extra,
                ))
            elif len(cells) == 4 and parent:
                detail = self._cell_text(cells[0])
                pzn = self._cell_text(cells[1])
                start_text = self._cell_text(cells[2])
                end_text = self._cell_text(cells[3])

                # Skip empty header row like "Lieferengpässe bestanden für:"
                if not pzn and not start_text and not end_text:
                    continue

                combined_extra = " | ".join(p for p in [parent.get("extra", ""), detail] if p)

                records.append(self._build_record(
                    category=category,
                    name=parent.get("name", ""),
                    substance=parent.get("substance", ""),
                    age=parent.get("age", ""),
                    pzn=pzn, start_text=start_text, end_text=end_text,
                    mah=parent.get("mah", ""),
                    extra=combined_extra,
                ))

        return records

    def _build_record(self, *, category: str, name: str, substance: str, age: str,
                      pzn: str, start_text: str, end_text: str, mah: str, extra: str) -> dict:
        shortage_start = self._parse_date(start_text)
        estimated_end = self._parse_date(end_text)
        today = datetime.now().strftime("%Y-%m-%d")

        if category.startswith("archive_"):
            status = "resolved"
        elif estimated_end and estimated_end < today:
            status = "resolved"
        elif estimated_end:
            status = "shortage"
        else:
            status = "shortage - end date unknown"

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": name,
            "active_substance": substance,
            "strength": "",
            "package_size": "",
            "product_no": pzn,
            "enr": "",
            "atc_code": "",
            "shortage_start": shortage_start,
            "estimated_end": estimated_end,
            "status": status,
            "reason": "",
            "category": category,
            "age_group": age,
            "mah": mah,
            "availability_text": end_text,
            "extra_info": extra,
            "scraped_at": datetime.now().isoformat(),
        }

    def _archive_year_urls(self) -> list[tuple[str, str]]:
        current_year = datetime.now().year
        pairs: list[tuple[str, str]] = []
        for year in range(self.ARCHIVE_FIRST_YEAR, current_year + 1):
            for cat_key, cat_label in self.ARCHIVE_CATEGORIES.items():
                url = self.ARCHIVE_URL_TEMPLATE.format(year=year, cat=cat_key)
                pairs.append((f"{cat_label}_{year}", url))
        return pairs

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_records: list[dict] = []

        for category, url in self.CURRENT_URLS.items():
            try:
                records = self._parse_category(category, url)
                print(f"  {category}: {len(records)} records")
                all_records.extend(records)
            except Exception as e:
                print(f"  ERROR fetching {category}: {e}")

        archive_total = 0
        for category, url in self._archive_year_urls():
            try:
                records = self._parse_category(category, url)
                archive_total += len(records)
                all_records.extend(records)
            except Exception as e:
                print(f"  ERROR fetching {category}: {e}")
        print(f"  archive (all years): {archive_total} records")

        df = pd.DataFrame(all_records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
