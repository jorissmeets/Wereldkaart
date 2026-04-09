"""Scraper for Australia TGA medicine shortage data."""

import json
import re
import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class AuTgaScraper(BaseScraper):
    """Scraper for TGA (Australia) Medicine Shortages Information Initiative."""

    URL = "https://apps.tga.gov.au/Prod/msi/search"

    # Status codes
    STATUS_MAP = {
        "C": "current",
        "R": "resolved",
        "A": "anticipated",
    }

    def __init__(self):
        super().__init__(
            country_code="AU",
            country_name="Australia",
            source_name="TGA",
            base_url="https://apps.tga.gov.au",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str:
            return None
        for fmt in ("%d-%m-%Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(self.URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()

        # Extract embedded JSON tabularData
        m = re.search(r'var\s+tabularData\s*=\s*(\{.*?\});', resp.text, re.DOTALL)
        if not m:
            raise ValueError("Could not find tabularData in page")

        data = json.loads(m.group(1))
        raw_records = data.get("records", [])
        print(f"  Found {len(raw_records)} embedded records")

        records = []
        for rec in raw_records:
            status_code = rec.get("status", "")
            status = self.STATUS_MAP.get(status_code, status_code)

            other_ingredients = rec.get("other_ingredients", [])
            if isinstance(other_ingredients, list):
                other_ingredients = "; ".join(other_ingredients)

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": rec.get("trade_names", ""),
                "active_substance": rec.get("active_ingredients", ""),
                "strength": "",
                "package_size": "",
                "dosage_form": rec.get("dose_form", ""),
                "artg_number": rec.get("artg_numb", ""),
                "atc_level1": rec.get("atc_level1", ""),
                "other_ingredients": other_ingredients,
                "availability": rec.get("availability", ""),
                "shortage_impact": rec.get("shortage_impact", ""),
                "tga_action": rec.get("tga_shortage_management_action", ""),
                "status": status,
                "shortage_start": self._parse_date(rec.get("shortage_start")),
                "estimated_end": self._parse_date(rec.get("shortage_end")),
                "last_updated": self._parse_date(rec.get("last_updated")),
                "deleted_date": self._parse_date(rec.get("deleted_date")),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
