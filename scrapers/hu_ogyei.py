"""Scraper for Hungary OGYÉI medicine shortage data via CSV export."""

import requests
import pandas as pd
from datetime import datetime
from io import StringIO

from scrapers.base_scraper import BaseScraper


class HuOgyeiScraper(BaseScraper):
    """Scraper for OGYÉI (Országos Gyógyszerészeti és Élelmezés-egészségügyi Intézet) shortage CSV."""

    CSV_URL = "https://ogyei.gov.hu/generalt_listak/shortage_lista.csv"

    def __init__(self):
        super().__init__(
            country_code="HU",
            country_name="Hungary",
            source_name="OGYEI",
            base_url="https://ogyei.gov.hu",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str or date_str.lower() == "nan":
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.CSV_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        # CSV uses latin-1 encoding and semicolon separator
        df_raw = pd.read_csv(
            StringIO(response.content.decode("latin-1")),
            sep=";",
        )
        print(f"  Downloaded {len(df_raw)} records")

        records = []
        for _, row in df_raw.iterrows():
            name = str(row.get("Termék neve", "")).strip()
            if not name or name == "nan":
                continue

            atc = str(row.get("ATC kód 1/ATC kód 2", "")).strip()
            if atc == "nan":
                atc = ""

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name,
                "active_substance": str(row.get("Hatóanyag", "")).strip().replace("nan", ""),
                "strength": "",
                "package_size": str(row.get("Kiszerelés neve", "")).strip().replace("nan", ""),
                "product_no": str(row.get("TK szám", "")).strip().replace("nan", ""),
                "atc_code": atc,
                "marketing_auth_holder": str(row.get("Forg Eng Jog", "")).strip().replace("nan", ""),
                "shortage_start": self._parse_date(str(row.get("A hiány kezdete", ""))),
                "estimated_end": self._parse_date(str(row.get("A hiány tervezett vége", ""))),
                "status": "shortage",
                "reason": str(row.get("A hiány oka", "")).strip().replace("nan", ""),
                "alternative": str(row.get("Javaslat a hiánykészítmény pótlására", "")).strip().replace("nan", ""),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
