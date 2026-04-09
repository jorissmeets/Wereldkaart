"""Scraper for Ireland HPRA shortage data via Azure Functions API."""

import base64
import requests
import pandas as pd
from datetime import datetime, timezone

from scrapers.base_scraper import BaseScraper


class IeHpraScraper(BaseScraper):
    """Scraper for HPRA medicine shortage API."""

    API_URL = "https://sfapi.hpra.ie/api/Shortages?code=kYYwqHIJAeykzxEtQ18r4w1VPuIkg5KRDQn1vv-kA679AzFuxwo03g=="
    PAGE_SIZE = 100

    def __init__(self):
        super().__init__(
            country_code="IE",
            country_name="Ireland",
            source_name="HPRA",
            base_url="https://www.hpra.ie",
        )

    def _get_client_header(self) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
             datetime.now(timezone.utc).strftime("%f")[:3] + "Z"
        return base64.b64encode(ts.encode()).decode()

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
        skip = 0

        while True:
            headers = {
                "Content-Type": "application/json",
                "X-Client-App": self._get_client_header(),
                "User-Agent": "Mozilla/5.0",
            }
            payload = {
                "id": None,
                "skip": skip,
                "take": self.PAGE_SIZE,
                "query": None,
                "order": "productname",
                "filter": "All",
            }

            response = requests.post(self.API_URL, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            items = data.get("items", [])
            total = data.get("currentFilterCount", 0)

            if skip == 0:
                print(f"  Total shortages: {total}")

            all_items.extend(items)

            if not items or skip + len(items) >= total:
                break
            skip += len(items)

        print(f"  Downloaded {len(all_items)} records")

        records = []
        for item in all_items:
            substances = ", ".join(
                ing.get("substanceName", "")
                for ing in item.get("activeProductIngredient", [])
            )
            license_info = item.get("productLicense", {}) or {}

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": item.get("productName", ""),
                "active_substance": substances,
                "dosage_form": item.get("combinedDosageForm", ""),
                "strength": "",
                "package_size": item.get("packSize", ""),
                "product_no": item.get("shortageID", ""),
                "licence_number": license_info.get("licenseNumber", ""),
                "marketing_auth_holder": license_info.get("licenseHolderName", ""),
                "shortage_start": self._parse_date(item.get("expectedDateToImpact")),
                "estimated_end": self._parse_date(item.get("expectedResolutionDate")),
                "actual_end": self._parse_date(item.get("shortageResolutionDate")),
                "status": "resolved" if item.get("shortageResolutionDate") else "shortage",
                "reason": item.get("shortageReason", ""),
                "countries_impacted": item.get("countriesImpacted", ""),
                "therapeutic_alternative": item.get("therapeuticAlternative", ""),
                "last_updated": self._parse_date(item.get("lastUpdated")),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
