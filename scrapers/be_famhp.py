"""Scraper for Belgium FAMHP/FAGG shortage data via farmastatus.be API."""

import json
import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class BeFamhpScraper(BaseScraper):
    """Scraper for farmastatus.be public packs API."""

    API_URL = "https://farmastatus.be/api/packs/info/public"
    PAGE_SIZE = 100

    def __init__(self):
        super().__init__(
            country_code="BE",
            country_name="Belgium",
            source_name="FAMHP",
            base_url="https://farmastatus.be",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_items = []
        start_row = 1

        while True:
            params = {
                "startRow": start_row,
                "endRow": start_row + self.PAGE_SIZE - 1,
                "language": "en",
                "searchSTR": json.dumps({"notificationStatus": "unavailable"}),
            }
            response = requests.get(self.API_URL, params=params, timeout=30,
                                    headers={"User-Agent": "Mozilla/5.0",
                                             "Accept": "application/json"})
            response.raise_for_status()
            data = response.json()

            items = data.get("data", [])
            total = data.get("count", 0)

            if start_row == 1:
                print(f"  Total unavailable packs: {total}")

            all_items.extend(items)

            if not items or start_row + len(items) > total:
                break
            start_row += len(items)

        print(f"  Downloaded {len(all_items)} records")

        records = []
        for item in all_items:
            # Get the active unavailable notification
            active_notif = None
            for notif in item.get("notARR", []):
                if notif.get("notificationStatus") == "unavailable":
                    active_notif = notif
                    break

            if not active_notif:
                continue

            # Parse active substances from JSON string
            substances = ""
            raw_subst = item.get("activeSubstancesLongEn", "")
            if raw_subst:
                try:
                    subst_list = json.loads(raw_subst)
                    substances = ", ".join(subst_list)
                except (json.JSONDecodeError, TypeError):
                    substances = raw_subst

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": item.get("prescriptionName", ""),
                "active_substance": substances,
                "strength": "",
                "package_size": "",
                "product_no": item.get("cnkCode", ""),
                "atc_code": item.get("atcCode", ""),
                "authorisation_number": item.get("authorisationNumber", ""),
                "marketing_auth_holder": item.get("packCompanyName", ""),
                "shortage_start": self._parse_date(active_notif.get("startDate")),
                "estimated_end": self._parse_date(active_notif.get("presumedEndDate")),
                "actual_end": self._parse_date(active_notif.get("endDate")),
                "status": active_notif.get("notificationStatus", "unavailable"),
                "reason": active_notif.get("notificationReason", ""),
                "impact": active_notif.get("impactString", ""),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
