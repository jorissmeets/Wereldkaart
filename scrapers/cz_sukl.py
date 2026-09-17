"""Scraper for Czech Republic SÚKL (Státní ústav pro kontrolu léčiv) shortage data."""

import time
import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class CzSuklScraper(BaseScraper):
    """Scraper for SÚKL unavailable medicines API at prehledy.sukl.cz"""

    API_URL = "https://prehledy.sukl.cz/hsz/v1/nedostupne-lp"
    DRUG_API_URL = "https://prehledy.sukl.cz/dlp/v1/lecive-pripravky"

    # typ values: 1 = ?, 2 = ?
    TYPE_MAP = {
        1: "shortage",
        2: "shortage - monitored",
    }

    def __init__(self):
        super().__init__(
            country_code="CZ",
            country_name="Czech Republic",
            source_name="SUKL",
            base_url="https://prehledy.sukl.cz",
        )

    def _lookup_atc(self, kod_sukl: str) -> str:
        """Haal de ATC-code op via de SÚKL geneesmiddel-detail-API (veld 'ATCkod').

        De lijst-API (nedostupne-lp) geeft geen ATC; de detail-API wel. De oude
        stof-lookup gaf alleen stof-ID's ([936]) terug, waardoor CZ zonder ATC bleef.
        """
        if not kod_sukl:
            return ""
        try:
            resp = requests.get(
                f"{self.DRUG_API_URL}/{kod_sukl}",
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            if resp.status_code != 200:
                return ""
            data = resp.json()
            if isinstance(data, list):
                data = data[0] if data else {}
            return str(data.get("ATCkod", "")).strip().upper()
        except Exception:
            return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.API_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0",
                                         "Accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        print(f"  Downloaded {len(data)} records from API")

        # Per uniek product de ATC-code ophalen via de detail-API
        unique_kods = {str(item.get("kodSUKL", "")).strip() for item in data if item.get("kodSUKL")}
        print(f"  ATC opzoeken voor {len(unique_kods)} unieke producten...")
        kod_atc_map: dict[str, str] = {}
        for i, kod in enumerate(unique_kods):
            kod_atc_map[kod] = self._lookup_atc(kod)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_kods)}")
            time.sleep(0.1)
        found = sum(1 for v in kod_atc_map.values() if v)
        print(f"  ATC gevonden voor {found}/{len(unique_kods)} producten")

        records = []
        for item in data:
            kod = str(item.get("kodSUKL", "")).strip()
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(item.get("nazev", "")).strip(),
                "active_substance": "",
                "strength": str(item.get("doplnek", "")).strip(),
                "package_size": "",
                "product_no": kod,
                "shortage_start": item.get("platOd", ""),
                "estimated_end": item.get("platDo", ""),
                "status": self.TYPE_MAP.get(item.get("typ"), "shortage"),
                "atc_code": kod_atc_map.get(kod, ""),
                "reference_no": str(item.get("cisloJednaciOd", "")).strip(),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
