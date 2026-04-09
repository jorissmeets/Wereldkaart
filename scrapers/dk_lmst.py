"""Scraper for Denmark Lægemiddelstyrelsen shortage data."""

import re
import json
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class DkLmstScraper(BaseScraper):
    """Scraper for Lægemiddelstyrelsen medicine shortage notices."""

    URL = "https://laegemiddelstyrelsen.dk/da/godkendelse/kontrol-og-inspektion/mangel-paa-medicin/meddelelser-om-forsyning-af-medicin/"
    PAGE_SIZE = 20

    # GUIDs for table fields (from data-data attribute)
    FIELD_PRODUCT = "{4BE8272E-F07F-4CD6-BDEB-D175115B5B47}"
    FIELD_PERIOD = "{D05F2686-DFE8-4E1F-BD45-4D48E7D9A266}"
    FIELD_REASON = "{27F80008-4F2B-4D25-8520-AF8681A909BB}"

    def __init__(self):
        super().__init__(
            country_code="DK",
            country_name="Denmark",
            source_name="LMST",
            base_url="https://laegemiddelstyrelsen.dk",
        )

    def _scrape_detail_substance(self, detail_url: str) -> str:
        """Scrape active substance from a detail page."""
        if not detail_url:
            return ""
        url = detail_url if detail_url.startswith("http") else f"{self.base_url}{detail_url}"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # Look for "Aktivt stof:" or "Indholdsstof:" or "Active substance:" patterns
            for pattern in (
                r"[Aa]ktivt?\s+stof(?:fer)?:\s*([^\n.;]+)",
                r"[Ii]ndholdsstof(?:fer)?:\s*([^\n.;]+)",
                r"[Aa]ctive\s+substance:\s*([^\n.;]+)",
                r"[Gg]enerisk\s+navn:\s*([^\n.;]+)",
            ):
                m = re.search(pattern, text)
                if m:
                    substance = m.group(1).strip()
                    if substance and len(substance) >= 3:
                        return substance
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_results = []
        page = 0

        total_expected = None
        while True:
            url = f"{self.URL}?page={page}&pageSize={self.PAGE_SIZE}"
            response = requests.get(url, timeout=30,
                                    headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            el = soup.find(attrs={"data-results": True})
            if not el:
                break

            results = json.loads(el.get("data-results", "[]"))
            if not results:
                break

            if page == 0:
                counter = json.loads(el.get("data-counter", "{}"))
                total_expected = counter.get("totalResults")
                print(f"  Total results: {total_expected}")

            all_results.extend(results)
            page += 1

            if len(results) < self.PAGE_SIZE:
                break

            # Safety: stop when we've fetched more than the reported total
            if total_expected and len(all_results) >= int(total_expected):
                break

        print(f"  Downloaded {len(all_results)} records across {page} pages")

        # Batch-lookup substances from detail pages
        detail_urls = {item.get("url", "") for item in all_results if item.get("url")}
        print(f"  Scraping {len(detail_urls)} detail pages for active substances...")
        url_substance_map: dict[str, str] = {}
        for i, durl in enumerate(detail_urls):
            url_substance_map[durl] = self._scrape_detail_substance(durl)
            if (i + 1) % 20 == 0:
                print(f"    ... {i + 1}/{len(detail_urls)} detail pages done")
            time.sleep(0.2)
        found = sum(1 for v in url_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(detail_urls)} products")

        records = []
        for item in all_results:
            table_data = item.get("dynamicTableData", {})
            detail_url = item.get("url", "")

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": table_data.get(self.FIELD_PRODUCT, item.get("name", "")),
                "active_substance": url_substance_map.get(detail_url, ""),
                "strength": "",
                "package_size": "",
                "product_no": "",
                "shortage_start": "",
                "estimated_end": "",
                "expected_period": table_data.get(self.FIELD_PERIOD, ""),
                "status": "shortage",
                "reason": table_data.get(self.FIELD_REASON, ""),
                "detail_url": detail_url,
                "published_date": item.get("date", ""),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
