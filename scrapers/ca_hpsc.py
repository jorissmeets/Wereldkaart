"""Scraper for Canada Drug Shortages via healthproductshortages.ca."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class CaHpscScraper(BaseScraper):
    """Scraper for Health Product Shortages Canada (HPSC)."""

    SEARCH_URL = "https://healthproductshortages.ca/search"
    PAGE_SIZE = 100

    def __init__(self):
        super().__init__(
            country_code="CA",
            country_name="Canada",
            source_name="HPSC",
            base_url="https://healthproductshortages.ca",
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def _scrape_drug_substance(self, drug_url: str) -> str:
        """Scrape active ingredient from a drug detail page on HPSC."""
        if not drug_url:
            return ""
        try:
            resp = self.session.get(drug_url, timeout=10)
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "lxml")

            # Look for "Active ingredient" or "Ingredient" label
            for label_text in ("Active ingredient", "Ingredient", "Ingrédient actif"):
                label = soup.find(string=re.compile(label_text, re.I))
                if label:
                    # The value is typically in the next sibling or parent's next sibling
                    parent = label.find_parent(["dt", "th", "td", "strong", "b", "span", "div"])
                    if parent:
                        sibling = parent.find_next_sibling(["dd", "td", "span", "div"])
                        if sibling:
                            substance = sibling.get_text(strip=True)
                            if substance and len(substance) >= 2:
                                return substance

            # Fallback: search page text for pattern
            text = soup.get_text(" ", strip=True)
            m = re.search(r"[Aa]ctive\s+[Ii]ngredient[s]?:?\s*([^\n;]+?)(?:\s*(?:DIN|Strength|Company|$))", text)
            if m:
                return m.group(1).strip().rstrip(",. ")
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_records = []
        page = 1

        while True:
            resp = self.session.get(
                self.SEARCH_URL,
                params={"perform": 1, "page": page, "limit": self.PAGE_SIZE},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  Warning: status {resp.status_code} at page {page}")
                break

            soup = BeautifulSoup(resp.text, "lxml")
            table = soup.find("table")
            if not table:
                break

            rows = table.find_all("tr")
            if len(rows) < 2:
                break

            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue

                status = cells[0].get_text(strip=True)
                brand_name = cells[1].get_text(strip=True)
                company = cells[2].get_text(strip=True)
                strengths = cells[3].get_text(strip=True).replace("\n", "; ").replace("\r", "")
                date_updated = cells[4].get_text(strip=True)
                report_id = cells[5].get_text(strip=True)

                # Get drug link for DIN info
                drug_link = cells[1].find("a")
                drug_url = drug_link.get("href", "") if drug_link else ""

                # Get report link
                report_link = cells[5].find("a")
                report_url = report_link.get("href", "") if report_link else ""
                report_type = "shortage" if "/shortage/" in report_url else "discontinuation" if "/discontinuance/" in report_url else ""

                all_records.append({
                    "brand_name": brand_name,
                    "company": company,
                    "strengths": strengths,
                    "report_id": report_id,
                    "report_type": report_type,
                    "report_url": f"{self.base_url}{report_url}" if report_url else "",
                    "drug_url": f"{self.base_url}{drug_url}" if drug_url else "",
                    "status": status,
                    "date_updated": date_updated,
                })

            data_rows = len(rows) - 1
            if page == 1:
                for text in soup.find_all(string=True):
                    if "of" in text and "showing" in text.lower():
                        m = re.search(r"of\s+([\d,]+)", text)
                        if m:
                            total = int(m.group(1).replace(",", ""))
                            print(f"  Total records: {total}")

            if data_rows < self.PAGE_SIZE:
                break

            page += 1
            if page % 50 == 0:
                print(f"  Fetched {len(all_records)} records (page {page})...")

        print(f"  Fetched {len(all_records)} records, looking up active substances...")

        # Batch-lookup unique drug URLs for substances
        unique_drug_urls = {r["drug_url"] for r in all_records if r["drug_url"]}
        url_substance_map: dict[str, str] = {}
        for i, durl in enumerate(unique_drug_urls):
            url_substance_map[durl] = self._scrape_drug_substance(durl)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_drug_urls)} drug pages done")
            time.sleep(0.15)
        found = sum(1 for v in url_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(unique_drug_urls)} products")

        final_records = []
        for r in all_records:
            final_records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": r["brand_name"],
                "active_substance": url_substance_map.get(r["drug_url"], ""),
                "strength": r["strengths"],
                "package_size": "",
                "company_name": r["company"],
                "report_id": r["report_id"],
                "report_type": r["report_type"],
                "report_url": r["report_url"],
                "drug_url": r["drug_url"],
                "status": r["status"],
                "update_date": r["date_updated"],
                "shortage_start": None,
                "estimated_end": None,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(final_records)
        print(f"  Total: {len(df)} shortage/discontinuation records scraped")
        return df
