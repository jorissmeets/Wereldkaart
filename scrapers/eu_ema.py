"""Scraper for EMA (European Medicines Agency) centrally-authorized shortage data."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class EuEmaScraper(BaseScraper):
    """Scraper for EMA shortage catalogue at ema.europa.eu"""

    SEARCH_URL = (
        "https://www.ema.europa.eu/en/search"
        "?f[0]=ema_medicine_bundle:ema_shortage"
        "&f[1]=ema_search_categories:83"
        "&f[2]=ema_search_custom_entity_bundle:001_ema_medicines_and_related"
        "&f[3]=shortage_status:{status}"
    )

    def __init__(self):
        super().__init__(
            country_code="EU",
            country_name="EU (EMA)",
            source_name="EMA",
            base_url="https://www.ema.europa.eu",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str:
            return None
        date_str = str(date_str).strip()
        for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _scrape_status(self, status: str) -> list[dict]:
        url = self.SEARCH_URL.format(status=status)
        response = requests.get(url, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        articles = soup.find_all("article")
        records = []

        for article in articles:
            text = article.get_text(separator="|", strip=True)
            if "INN or common name:" not in text:
                continue

            # Parse fields from pipe-separated text
            fields = {}
            parts = text.split("|")
            title = parts[0] if parts else ""

            for i, part in enumerate(parts):
                part = part.strip()
                if part.endswith(":") and i + 1 < len(parts):
                    key = part.rstrip(":")
                    fields[key] = parts[i + 1].strip()

            medicine_name = re.sub(r"\s*-\s*supply shortage$", "", title).strip()

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "active_substance": fields.get("INN or common name", ""),
                "strength": fields.get("Strengths affected", ""),
                "package_size": "",
                "product_no": "",
                "pharmaceutical_form": fields.get("Pharmaceutical form(s)", ""),
                "shortage_start": "",
                "estimated_end": "",
                "last_updated": self._parse_date(fields.get("Last updated", "")),
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        return records

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_records = []
        for status in ("ongoing", "resolved"):
            records = self._scrape_status(status)
            print(f"  Found {len(records)} {status} shortages")
            all_records.extend(records)

        df = pd.DataFrame(all_records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
