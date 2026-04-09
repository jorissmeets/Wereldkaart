"""Scraper for Iceland Lyfjastofnun shortage data via HTML parsing."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class IsLyfjaScraper(BaseScraper):
    """Scraper for Lyfjastofnun (Icelandic Medicines Agency) shortage list."""

    URL = "https://www.lyfjastofnun.is/lyf/lyfjaskortur/tilkynntur-lyfjaskortur/"

    STATUS_MAP = {
        "í skorti": "shortage",
        "lokið": "resolved",
    }

    def __init__(self):
        super().__init__(
            country_code="IS",
            country_name="Iceland",
            source_name="Lyfjastofnun",
            base_url="https://www.lyfjastofnun.is",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        for fmt in ("%d.%m.%Y", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _get_field(self, item_div, label: str) -> str:
        for li in item_div.find_all("li", class_="apotek__list__li"):
            strong = li.find("strong")
            if strong and label in strong.get_text(strip=True):
                return li.get_text(strip=True).replace(strong.get_text(strip=True), "", 1).strip()
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.URL, timeout=120,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        container = soup.find(class_="apoteklist")
        if not container:
            print("  ERROR: Could not find apoteklist container")
            return pd.DataFrame()

        items = container.find_all("div", class_="apotek__item")
        print(f"  Found {len(items)} items on page")

        records = []
        for item in items:
            status_el = item.find(class_="apotek__title--region")
            status_raw = status_el.get_text(strip=True).lower() if status_el else ""
            status = self.STATUS_MAP.get(status_raw, status_raw)

            medicine_name = self._get_field(item, "Lyfjaheiti:")
            strength = self._get_field(item, "Styrkur:")
            package_size = self._get_field(item, "Magn:")
            form = self._get_field(item, "Lyfjaform:")
            product_no = self._get_field(item, "Vörunúmer:")
            atc_code = self._get_field(item, "ATC flokkur:")
            mah = self._get_field(item, "Markaðsleyfishafi:")
            agent = self._get_field(item, "Umboðsaðili:")
            expected_end = self._get_field(item, "Áætluð lok:")
            expected_start = self._get_field(item, "Áætlað upphaf:")
            notified = self._get_field(item, "Tilkynnt:")
            substance = self._get_field(item, "Innihaldsefni:")
            recommendations = self._get_field(item, "Ráðleggingar:")

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "active_substance": substance,
                "strength": strength,
                "package_size": package_size,
                "dosage_form": form,
                "product_no": product_no,
                "atc_code": atc_code,
                "marketing_auth_holder": mah,
                "agent": agent,
                "shortage_start": self._parse_date(expected_start),
                "estimated_end": self._parse_date(expected_end),
                "notified_date": self._parse_date(notified),
                "status": status,
                "recommendations": recommendations,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
