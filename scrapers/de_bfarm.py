"""Scraper for Germany BfArM (Bundesinstitut für Arzneimittel) Lieferengpass database."""

import requests
import pandas as pd
from datetime import datetime
from io import StringIO

from scrapers.base_scraper import BaseScraper


class DeBfarmScraper(BaseScraper):
    """Scraper for BfArM Lieferengpass CSV export."""

    CSV_URL = "https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/public/csv"

    def __init__(self):
        super().__init__(
            country_code="DE",
            country_name="Germany",
            source_name="BfArM",
            base_url="https://anwendungen.pharmnet-bund.de",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or pd.isna(date_str):
            return None
        date_str = str(date_str).strip()
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.CSV_URL, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        raw_df = pd.read_csv(StringIO(response.text), sep=";", encoding="utf-8")
        print(f"  Downloaded {len(raw_df)} rows from CSV")

        records = []
        for _, row in raw_df.iterrows():
            shortage_start = self._parse_date(row.get("Beginn"))
            estimated_end = self._parse_date(row.get("Ende"))

            if estimated_end and estimated_end < datetime.now().strftime("%Y-%m-%d"):
                status = "resolved"
            elif estimated_end:
                status = "shortage"
            else:
                status = "shortage - end date unknown"

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(row.get("Arzneimittlbezeichnung", "")).strip(),
                "active_substance": str(row.get("Wirkstoffe", "")).strip(),
                "strength": "",
                "package_size": "",
                "product_no": str(row.get("PZN", "")).strip(),
                "enr": str(row.get("ENR", "")).strip(),
                "atc_code": str(row.get("Atc Code", "")).strip(),
                "shortage_start": shortage_start,
                "estimated_end": estimated_end,
                "status": status,
                "reason": str(row.get("Art des Grundes", "")).strip(),
                "meldungsart": str(row.get("Meldungsart", "")).strip(),
                "mah": str(row.get("Zulassungsinhaber", "")).strip(),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
