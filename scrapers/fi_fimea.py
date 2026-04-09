"""Scraper for Finland FIMEA shortage data via RSS feed."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class FiFimeaScraper(BaseScraper):
    """Scraper for FIMEA shortage RSS feed."""

    RSS_URL = "https://fimea.fi/c/laakehaut_ja_luettelot/saatavuushairio-uusi/rss"

    def __init__(self):
        super().__init__(
            country_code="FI",
            country_name="Finland",
            source_name="FIMEA",
            base_url="https://fimea.fi",
        )

    def _parse_dates(self, content_text):
        """Parse start/end dates from content like 'Saatavuushäiriö alkaa: 2025-12-15, saatavuushäiriö päättyy: 2026-04-06'."""
        start = None
        end = None
        m = re.search(r"alkaa:\s*(\d{4}-\d{2}-\d{2})", content_text)
        if m:
            start = m.group(1)
        m = re.search(r"p(?:ä|a)(?:ä|a)ttyy:\s*(\d{4}-\d{2}-\d{2})", content_text)
        if m:
            end = m.group(1)
        return start, end

    @staticmethod
    def _extract_substance(content: str, categories: dict) -> str:
        """Extract active substance from RSS content or categories.

        Checks for 'vaikuttavaAine' category first, then parses content for
        'Vaikuttava aine: ...' pattern.
        """
        # Check category fields
        for key in ("vaikuttavaAine", "vaikuttava_aine", "substance"):
            if key in categories and categories[key]:
                return categories[key].strip()

        # Parse from content: "Vaikuttava aine: ..." pattern
        m = re.search(r"[Vv]aikuttava\s+aine:\s*([^,<\n]+)", content)
        if m:
            return m.group(1).strip()

        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.RSS_URL, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "xml")

        items = soup.find_all("item")
        print(f"  Found {len(items)} items in RSS feed")

        records = []
        for item in items:
            title = item.find("title").get_text(strip=True) if item.find("title") else ""
            desc = item.find("description").get_text(strip=True) if item.find("description") else ""
            content_el = item.find("content:encoded") or item.find("encoded")
            content = content_el.get_text(strip=True) if content_el else ""
            categories = {c.get("domain", ""): c.get_text(strip=True) for c in item.find_all("category")}
            guid = item.find("guid").get_text(strip=True) if item.find("guid") else ""
            creator_el = item.find("dc:creator") or item.find("creator")
            creator = creator_el.get_text(strip=True) if creator_el else ""

            start, end = self._parse_dates(content)

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": title,
                "active_substance": self._extract_substance(content, categories),
                "strength": "",
                "package_size": categories.get("pakkauskoko", ""),
                "product_no": guid,
                "atc_code": categories.get("atc", ""),
                "shortage_start": start,
                "estimated_end": end,
                "status": "shortage",
                "marketing_auth_holder": creator,
                "notes": desc,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
