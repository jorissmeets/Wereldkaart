"""Scraper for Latvia ZVA (Zāļu valsts aģentūra) medicine shortages."""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class LvZvaScraper(BaseScraper):
    """Scraper for https://dati.zva.gov.lv/zr-med-availability/"""

    API_URL = "https://dati.zva.gov.lv/zr-med-availability/api/med-avail-zp/"

    def __init__(self):
        super().__init__(
            country_code="LV",
            country_name="Latvia",
            source_name="ZVA",
            base_url="https://www.zva.gov.lv",
        )

    def _fetch_page(self, page: int) -> str:
        """Fetch a single page from the API."""
        params = {"p": page, "lang": "en"}
        response = requests.get(self.API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data.get("success"):
            raise RuntimeError(f"API returned success=false for page {page}")
        return data["data"]

    def _get_total_pages(self, html: str) -> int:
        """Extract total number of pages from pagination HTML."""
        soup = BeautifulSoup(html, "lxml")
        pagination = soup.select("ul.pagination li a")
        max_page = 1
        for link in pagination:
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
        return max_page

    def _parse_page(self, html: str) -> list[dict]:
        """Parse a page of HTML and return list of records."""
        soup = BeautifulSoup(html, "lxml")
        # Rows are direct children of <table>, no <tbody> wrapper
        table = soup.select_one("table.table")
        if not table:
            return []
        rows = table.find_all("tr")
        records = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            # First cell has rich HTML: <strong>name</strong><br/>Package size: X<br/>No.: Y
            first_cell = cells[0]
            medicine_name = first_cell.find("strong")
            medicine_name = medicine_name.get_text(strip=True) if medicine_name else first_cell.get_text(strip=True)

            # Extract package size and product number from cell text
            cell_text = first_cell.get_text("\n", strip=True)
            package_size = ""
            product_no = ""
            for line in cell_text.split("\n"):
                if "Package size:" in line or "Iepakojuma" in line:
                    package_size = line.split(":", 1)[-1].strip()
                elif "No.:" in line or "Nr.:" in line:
                    product_no = line.split(":", 1)[-1].strip()

            strength = cells[1].get_text(strip=True)
            active_substance = cells[2].get_text(strip=True)
            shortage_start_raw = cells[3].get_text(strip=True)
            estimated_end_raw = cells[4].get_text(strip=True)

            # Check if product is leaving the market
            leaving_market = "leaving the market" in shortage_start_raw.lower()

            # Parse dates
            shortage_start = self._parse_date(shortage_start_raw)
            estimated_end = self._parse_date(estimated_end_raw)

            # Determine status
            if leaving_market:
                status = "leaving the market"
            elif estimated_end and estimated_end < datetime.now().strftime("%Y-%m-%d"):
                status = "resolved"
            elif estimated_end:
                status = "shortage"
            else:
                status = "shortage - end date unknown"

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "active_substance": active_substance,
                "strength": strength,
                "package_size": package_size,
                "product_no": product_no,
                "shortage_start": shortage_start,
                "estimated_end": estimated_end,
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        return records

    def _parse_date(self, date_str: str) -> str | None:
        """Parse date string from ZVA format to YYYY-MM-DD."""
        if not date_str or date_str.lower() in ("not notified", "nav paziņots", "-", ""):
            return None

        import re
        # Normalize whitespace (non-breaking spaces, etc.)
        date_str = re.sub(r"\s+", " ", date_str).strip()

        # Skip garbled single characters
        if len(date_str) <= 1:
            return None

        # Handle "Leaving the market(from Mon DD, YYYY)" format
        leaving_match = re.search(r"Leaving the market\(from (.+?)\)", date_str)
        if leaving_match:
            date_str = leaving_match.group(1).strip()

        # Try common formats
        for fmt in ("%b %d, %Y", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        print(f"  Warning: Could not parse date '{date_str}'")
        return None

    def scrape(self) -> pd.DataFrame:
        """Scrape all pages of ZVA shortage data."""
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Fetch first page to determine total pages
        first_page_html = self._fetch_page(1)
        total_pages = self._get_total_pages(first_page_html)
        print(f"  Found {total_pages} pages")

        all_records = self._parse_page(first_page_html)
        print(f"  Page 1/{total_pages} - {len(all_records)} records")

        for page in range(2, total_pages + 1):
            html = self._fetch_page(page)
            records = self._parse_page(html)
            all_records.extend(records)
            print(f"  Page {page}/{total_pages} - {len(records)} records")

        df = pd.DataFrame(all_records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
