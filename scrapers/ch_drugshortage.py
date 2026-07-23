"""Scraper for Switzerland drugshortage.ch — publieke JSON-API v1.

De oude .aspx-pagina is verdwenen (410). De site draait nu op WordPress met een
open JSON-API: https://www.drugshortage.ch/api/v1/drugshortage.php?endpoint=shortages
(werkt zonder api_key). Levert naam, ATC, firma en status per artikel.
"""

import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class ChDrugShortageScraper(BaseScraper):
    """Scraper for drugshortage.ch (Switzerland) via de publieke JSON-API."""

    API_URL = "https://www.drugshortage.ch/api/v1/drugshortage.php?endpoint=shortages"

    def __init__(self):
        super().__init__(
            country_code="CH",
            country_name="Switzerland",
            source_name="drugshortage",
            base_url="https://www.drugshortage.ch",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str or date_str.lower() in ("offen", "unbestimmt", "kontingentiert", "kontigentiert"):
            return None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(self.API_URL, timeout=30,
                            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("data", []) if isinstance(payload, dict) else (payload or [])
        print(f"  API returned {len(items)} items")

        records = []
        for it in items:
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(it.get("bezeichnung") or "").strip(),
                "active_substance": "",
                "strength": "",
                "package_size": "",
                "atc_code": str(it.get("atcCode") or "").strip(),
                "company_name": str(it.get("firma") or "").strip(),
                "gtin": str(it.get("gtin") or ""),
                "pharmacode": str(it.get("pharmacode") or ""),
                "status": str(it.get("statusText") or "").strip(),
                "status_code": str(it.get("statusCode") or ""),
                "shortage_start": None,
                "estimated_end": self._parse_date(it.get("lieferfaehigkeitDate")),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
