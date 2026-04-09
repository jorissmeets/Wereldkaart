"""Scraper for US FDA drug shortage data via openFDA API."""

import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class UsFdaScraper(BaseScraper):
    """Scraper for FDA (United States) drug shortage data via openFDA API."""

    API_URL = "https://api.fda.gov/drug/shortages.json"
    PAGE_SIZE = 100  # openFDA max limit per request

    def __init__(self):
        super().__init__(
            country_code="US",
            country_name="United States",
            source_name="FDA",
            base_url="https://api.fda.gov",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str:
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_records = []
        skip = 0

        # First request to get total count
        resp = requests.get(
            self.API_URL,
            params={"limit": self.PAGE_SIZE, "skip": skip},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        total = data["meta"]["results"]["total"]
        print(f"  Total records: {total}")

        results = data.get("results", [])
        all_records.extend(results)
        skip += self.PAGE_SIZE

        # Paginate through all records
        while skip < total:
            resp = requests.get(
                self.API_URL,
                params={"limit": self.PAGE_SIZE, "skip": skip},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  Warning: status {resp.status_code} at skip={skip}")
                break
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break
            all_records.extend(results)
            skip += self.PAGE_SIZE

            if skip % 500 == 0:
                print(f"  Fetched {len(all_records)}/{total}...")

        print(f"  Fetched {len(all_records)} raw records")

        # Normalize records
        records = []
        for rec in all_records:
            openfda = rec.get("openfda", {})

            brand_names = openfda.get("brand_name", [])
            brand_name = brand_names[0] if brand_names else ""

            substances = openfda.get("substance_name", [])
            substance = ", ".join(substances) if substances else ""

            generic_name = rec.get("generic_name", "")
            manufacturer_names = openfda.get("manufacturer_name", [])
            manufacturer = manufacturer_names[0] if manufacturer_names else rec.get("company_name", "")

            routes = openfda.get("route", [])
            route = ", ".join(routes) if routes else ""

            categories = rec.get("therapeutic_category", [])
            category = ", ".join(categories) if categories else ""

            pharm_classes = openfda.get("pharm_class_epc", [])
            pharm_class = ", ".join(pharm_classes) if pharm_classes else ""

            product_ndcs = openfda.get("product_ndc", [])
            app_numbers = openfda.get("application_number", [])

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": generic_name or brand_name,
                "brand_name": brand_name,
                "active_substance": substance,
                "generic_name": generic_name,
                "strength": "",
                "package_size": "",
                "dosage_form": rec.get("dosage_form", ""),
                "presentation": rec.get("presentation", ""),
                "package_ndc": rec.get("package_ndc", ""),
                "product_ndc": ", ".join(product_ndcs) if product_ndcs else "",
                "application_number": ", ".join(app_numbers) if app_numbers else "",
                "route": route,
                "therapeutic_category": category,
                "pharmacologic_class": pharm_class,
                "manufacturer": manufacturer,
                "company_name": rec.get("company_name", ""),
                "contact_info": rec.get("contact_info", ""),
                "status": rec.get("status", ""),
                "update_type": rec.get("update_type", ""),
                "shortage_start": self._parse_date(rec.get("initial_posting_date")),
                "estimated_end": None,
                "discontinued_date": self._parse_date(rec.get("discontinued_date")),
                "update_date": self._parse_date(rec.get("update_date")),
                "related_info": rec.get("related_info", ""),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
