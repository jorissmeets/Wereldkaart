"""Scraper for Slovenia CBZ (Centralna baza zdravil) medicine shortage data."""

import requests
import pandas as pd
from io import BytesIO
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class SiCbzScraper(BaseScraper):
    """Scraper for CBZ (Central Medicine Database) shortage data.

    Downloads the full medicine register CSV and filters for medicines
    with active supply disruptions (market presence codes 3-6).
    """

    CSV_URL = "https://www.cbz.si/cbz2/sif22.csv"

    # Market presence codes indicating supply issues
    SHORTAGE_CODES = {3, 4, 5, 6}

    # Status mapping from market presence codes
    STATUS_MAP = {
        3: "upcoming",     # Napovedana motnja v preskrbi (Announced disruption)
        4: "upcoming",     # Napovedano začasno prenehanje (Announced temporary stop)
        5: "shortage",     # Potekajoča motnja v preskrbi (Ongoing disruption)
        6: "shortage",     # Potekajoče začasno prenehanje (Ongoing temporary stop)
    }

    def __init__(self):
        super().__init__(
            country_code="SI",
            country_name="Slovenia",
            source_name="CBZ",
            base_url="https://www.cbz.si",
        )

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.CSV_URL, timeout=120,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        print(f"  Downloaded {len(response.content) / 1024 / 1024:.1f} MB CSV")

        df_raw = pd.read_csv(
            BytesIO(response.content),
            sep=";",
            encoding="cp1252",
            low_memory=False,
        )
        print(f"  Total medicines in register: {len(df_raw)}")

        # Filter for active supply disruptions
        shortage = df_raw[df_raw["Šifra prisotnosti na trgu"].isin(self.SHORTAGE_CODES)].copy()
        print(f"  Medicines with supply issues: {len(shortage)}")

        records = []
        for _, row in shortage.iterrows():
            name = str(row.get("Ime zdravila", "")).strip()
            if not name or name == "nan":
                continue

            atc = str(row.get("ATC oznaka", "")).strip()
            if atc == "nan":
                atc = ""

            code = int(row.get("Šifra prisotnosti na trgu", 5))
            status = self.STATUS_MAP.get(code, "shortage")

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name,
                "full_name": str(row.get("Poimenovanje zdravila", "")).strip().replace("nan", ""),
                "active_substance": str(row.get("Latinski opis ATC", "")).strip().replace("nan", ""),
                "strength": "",
                "package_size": str(row.get("Pakiranje", "")).strip().replace("nan", ""),
                "dosage_form": str(row.get("Slovenski naziv farmacevtske oblike", "")).strip().replace("nan", ""),
                "atc_code": atc,
                "marketing_auth_holder": str(row.get("Naziv imetnika dovoljenja", "")).strip().replace("nan", ""),
                "market_status": str(row.get("Naziv prisotnosti na trgu", "")).strip().replace("nan", ""),
                "market_status_code": code,
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
