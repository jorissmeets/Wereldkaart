"""Scraper for Norway DMP (Direktoratet for medisinske produkter) shortage data."""

import json
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class NoDmpScraper(BaseScraper):
    """Scraper for https://www.dmp.no/ (formerly legemiddelverket.no)"""

    URL = "https://www.dmp.no/forsyningssikkerhet/legemiddelmangel/oversikt-over-legemiddelmangel---for-pasienter-og-helsepersonell"

    STATUS_MAP = {
        "pågående": "shortage",
        "kommende": "upcoming",
        "avsluttet": "resolved",
    }

    def __init__(self):
        super().__init__(
            country_code="NO",
            country_name="Norway",
            source_name="DMP",
            base_url="https://www.dmp.no",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        # Dates come as "10.03.2026 00:00:00" or "10.03.2026"
        date_str = re.sub(r"\s+\d{2}:\d{2}:\d{2}$", "", date_str)
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _find_status_column(self, columns):
        """Find the status column which changes name with date (e.g. 'Status pr. 11.03.2026')."""
        for col in columns:
            if col.lower().startswith("status pr"):
                return col
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.URL, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        excel_data_input = soup.find("input", id="excelData")
        if not excel_data_input:
            raise RuntimeError("No excelData input found on DMP page")

        data = json.loads(excel_data_input["value"])
        columns = list(data.keys())
        num_records = len(data[columns[0]])
        print(f"  Found {num_records} records with columns: {columns}")

        status_col = self._find_status_column(columns)

        records = []
        for i in range(num_records):
            row = {col: data[col][i] if i < len(data[col]) else "" for col in columns}

            raw_status = str(row.get(status_col, "")) if status_col else ""
            status = self.STATUS_MAP.get(raw_status.lower().strip(), raw_status or "shortage")

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(row.get("Legemiddelnavn", "")).strip(),
                "active_substance": str(row.get("Virkestoff(er)", "")).strip(),
                "strength": "",
                "package_size": "",
                "product_no": "",
                "shortage_start": self._parse_date(row.get("Mangelperiode fra", "")),
                "estimated_end": self._parse_date(row.get("Mangelperiode til", "")),
                "status": status,
                "notes": str(row.get("Informasjon/tiltak", "")).strip(),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
