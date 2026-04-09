"""Scraper for Italy AIFA (Agenzia Italiana del Farmaco) via CSV download."""

import requests
import pandas as pd
from datetime import datetime
from io import StringIO

from scrapers.base_scraper import BaseScraper


class ItAifaScraper(BaseScraper):
    """Scraper for AIFA farmaci carenti CSV."""

    CSV_URL = "https://www.aifa.gov.it/documents/20142/847339/elenco_medicinali_carenti.csv"

    def __init__(self):
        super().__init__(
            country_code="IT",
            country_name="Italy",
            source_name="AIFA",
            base_url="https://www.aifa.gov.it",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or pd.isna(date_str):
            return None
        date_str = str(date_str).strip()
        if not date_str:
            return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
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

        # CSV uses semicolons as separator, first 2 lines are header text
        lines = response.text.split("\n")
        # Find the header row (contains "Nome medicinale")
        header_idx = 0
        for i, line in enumerate(lines):
            if "Nome medicinale" in line:
                header_idx = i
                break

        csv_text = "\n".join(lines[header_idx:])
        raw_df = pd.read_csv(StringIO(csv_text), sep=";", encoding="utf-8")
        print(f"  Downloaded {len(raw_df)} rows from CSV")

        records = []
        for _, row in raw_df.iterrows():
            shortage_start = self._parse_date(row.get("Data inizio"))
            estimated_end = self._parse_date(row.get("Fine presunta"))

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
                "medicine_name": str(row.get("Nome medicinale", "")).strip(),
                "active_substance": str(row.get("Principio attivo", "")).strip(),
                "strength": str(row.get("Forma farmaceutica e dosaggio", "")).strip(),
                "package_size": "",
                "product_no": str(row.get("Codice AIC", "")).strip(),
                "shortage_start": shortage_start,
                "estimated_end": estimated_end,
                "status": status,
                "reason": str(row.get("Motivazioni", "")).strip(),
                "notes": str(row.get("Suggerimenti/Indicazioni AIFA", "")).strip(),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
