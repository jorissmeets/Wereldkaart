"""Scraper for Lithuania VVKT medicine shortage data via open data API."""

import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class LtVvktScraper(BaseScraper):
    """Scraper for VVKT (State Medicines Control Agency of Lithuania) shortage data.

    Uses the Lithuanian open data API (get.data.gov.lt) to retrieve
    registered medicines that are currently not being supplied.
    """

    API_URL = "https://get.data.gov.lt/datasets/gov/vvkt/vaistiniai_preparatai/PreparatasPakuote/:format/json"
    PAGE_SIZE = 100

    def __init__(self):
        super().__init__(
            country_code="LT",
            country_name="Lithuania",
            source_name="VVKT",
            base_url="https://vvkt.lrv.lt",
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        base_params = (
            f"pak_tiekimo_busena='Netiekiama'"
            f"&stadija='Registruotas'"
            f"&limit({self.PAGE_SIZE})"
        )

        all_items = []
        next_token = None
        page_num = 0
        max_pages = 200  # Safety limit

        while page_num < max_pages:
            if next_token:
                url = f"{self.API_URL}?{base_params}&page('{next_token}')"
            else:
                url = f"{self.API_URL}?{base_params}"

            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            data = resp.json()
            items = data.get("_data", [])
            if not items:
                break

            all_items.extend(items)
            page_num += 1

            page_info = data.get("_page", {})
            next_token = page_info.get("next")
            if not next_token:
                break

            if page_num % 20 == 0:
                print(f"  Fetched {len(all_items)} records...")

        print(f"  Downloaded {len(all_items)} records across {page_num} pages")

        records = []
        for item in all_items:
            name = (item.get("preparato_pav") or "").strip()
            if not name:
                continue

            atc = (item.get("atc_kodas") or "").strip()

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name,
                "active_substance": (item.get("veikl_angl_pavad") or item.get("veiklioji_medz_lt") or "").strip(),
                "active_substance_lt": (item.get("veiklioji_medz_lt") or "").strip(),
                "strength": (item.get("stiprumas") or "").strip(),
                "dosage_form": (item.get("farmacine_forma_lt") or "").strip(),
                "atc_code": atc,
                "product_no": (item.get("pak_reg_nr") or "").strip(),
                "marketing_auth_holder": (item.get("registruotojas") or "").strip(),
                "status": "shortage",
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
