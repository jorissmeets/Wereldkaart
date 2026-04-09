"""Scraper for Estonia Ravimiamet medicine shortage data via ravimiregister.ee."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class EeRavimiametScraper(BaseScraper):
    """Scraper for Ravimiamet (Estonian State Agency of Medicines) shortage data.

    Uses the ASP.NET WebForms search on www.ravimiregister.ee with the
    'tarneraskus' (supply disruption) radio filter to find all medicines
    with current supply issues or discontinued marketing.
    """

    SEARCH_URL = "https://www.ravimiregister.ee/Default.aspx?pv=HumRavimid.Otsing"

    def __init__(self):
        super().__init__(
            country_code="EE",
            country_name="Estonia",
            source_name="RAVIMIAMET",
            base_url="https://www.ravimiregister.ee",
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })

    def _get_hidden_fields(self, soup) -> dict:
        """Extract ASP.NET hidden fields from a parsed page."""
        data = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name", "")
            if name:
                data[name] = inp.get("value", "")
        return data

    def _parse_results_table(self, soup) -> list[dict]:
        """Parse the results table from a search results page."""
        records = []

        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if "Ravimi nimetus" not in headers:
                continue

            rows = table.find_all("tr")
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 7:
                    continue

                name = cells[1].get_text(strip=True)
                if not name:
                    continue

                link = cells[1].find("a")
                detail_url = ""
                if link and link.get("href"):
                    href = link["href"]
                    if href.startswith("/"):
                        detail_url = f"https://www.ravimiregister.ee{href}"
                    elif not href.startswith(("http", "javascript")):
                        detail_url = f"https://www.ravimiregister.ee/{href}"
                    elif href.startswith("http"):
                        detail_url = href

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": name,
                    "atc_code": cells[2].get_text(strip=True),
                    "active_substance": cells[3].get_text(strip=True),
                    "dosage_form": cells[4].get_text(strip=True),
                    "strength": cells[5].get_text(strip=True),
                    "marketing_auth_holder": cells[6].get_text(strip=True),
                    "status": "shortage",
                    "detail_url": detail_url,
                    "scraped_at": datetime.now().isoformat(),
                })

            break

        return records

    def _find_next_page_target(self, soup) -> str | None:
        """Find the 'Next page' (Järgmine) postback target."""
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a.get("href", "")
            if text == "Järgmine" and "paging1" in href:
                match = re.search(r'PostBackOptions\("([^"]+)"', href)
                if match:
                    return match.group(1)
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Step 1: GET the search page
        resp = self.session.get(self.SEARCH_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Step 2: POST with supply disruption filter
        data = self._get_hidden_fields(soup)
        data["ctl04$ctl00$ctl00$Detailotsing$tarneraskus"] = "Mõlemad"
        data["ctl04$ctl00$ctl00$Detailotsing$search2"] = "Otsi \u00bb"

        time.sleep(1)
        resp2 = self.session.post(self.SEARCH_URL, data=data, timeout=60)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "lxml")

        # Parse first page
        all_records = self._parse_results_table(soup2)
        print(f"  Page 1: {len(all_records)} records")

        # Step 3: Paginate using "Järgmine" (Next) button
        page_num = 2
        max_pages = 20  # Safety limit

        while page_num <= max_pages:
            next_target = self._find_next_page_target(soup2)
            if not next_target:
                break

            time.sleep(1.5)  # Be polite
            page_data = self._get_hidden_fields(soup2)
            page_data["__EVENTTARGET"] = next_target
            page_data["__EVENTARGUMENT"] = ""

            try:
                resp_page = self.session.post(self.SEARCH_URL, data=page_data, timeout=60)
                resp_page.raise_for_status()
                soup2 = BeautifulSoup(resp_page.text, "lxml")

                page_records = self._parse_results_table(soup2)
                if not page_records:
                    break

                all_records.extend(page_records)
                print(f"  Page {page_num}: {len(page_records)} records (total: {len(all_records)})")
                page_num += 1
            except Exception as e:
                print(f"  Error on page {page_num}: {e}")
                break

        df = pd.DataFrame(all_records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
