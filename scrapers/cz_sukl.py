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

    def _lookup_substance(self, kod_sukl: str) -> str:
        """Look up active substance via SÚKL drug database API."""
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
            # Try common field names for active substance
            for field in ("leciveLatkyCZ", "leciveLatky", "aktivniLatka", "ucinnaLatka"):
                val = data.get(field, "")
                if val:
                    return str(val).strip()
            # Try nested substances list
            substances = data.get("leciveLatky", data.get("substances", []))
            if isinstance(substances, list) and substances:
                return ", ".join(
                    str(s.get("nazev", s.get("name", ""))).strip()
                    for s in substances
                    if s.get("nazev") or s.get("name")
                )
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.API_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0",
                                         "Accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        print(f"  Downloaded {len(data)} records from API")

        # Batch-lookup unique kodSUKL values for active substances
        unique_kods = {str(item.get("kodSUKL", "")).strip() for item in data if item.get("kodSUKL")}
        print(f"  Looking up active substances for {len(unique_kods)} unique products...")
        kod_substance_map: dict[str, str] = {}
        for i, kod in enumerate(unique_kods):
            kod_substance_map[kod] = self._lookup_substance(kod)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_kods)} lookups done")
            time.sleep(0.1)
        found = sum(1 for v in kod_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(unique_kods)} products")

        records = []
        for item in data:
            kod = str(item.get("kodSUKL", "")).strip()
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(item.get("nazev", "")).strip(),
                "active_substance": kod_substance_map.get(kod, ""),
                "strength": str(item.get("doplnek", "")).strip(),
                "package_size": "",
                "product_no": kod,
                "shortage_start": item.get("platOd", ""),
                "estimated_end": item.get("platDo", ""),
                "status": self.TYPE_MAP.get(item.get("typ"), "shortage"),
                "reference_no": str(item.get("cisloJednaciOd", "")).strip(),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
