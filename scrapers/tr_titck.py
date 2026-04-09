"""Scraper for Turkey TITCK licensed pharmaceutical products with suspension status."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import tempfile
import os

from scrapers.base_scraper import BaseScraper


class TrTitckScraper(BaseScraper):
    """Scraper for TITCK (Turkey) licensed pharmaceutical products list.

    Downloads the weekly XLSX file containing all licensed products,
    including their suspension (askıda) status which indicates supply issues.
    """

    LIST_URL = "https://titck.gov.tr/dinamikmodul/85"

    # Suspension status codes
    STATUS_MAP = {
        0: "active",
        1: "suspended_article_23",
        2: "suspended_pharmacovigilance",
        3: "suspended_article_22",
    }

    def __init__(self):
        super().__init__(
            country_code="TR",
            country_name="Turkey",
            source_name="TITCK",
            base_url="https://titck.gov.tr",
        )

    def _get_latest_xlsx_url(self) -> str:
        resp = requests.get(
            self.LIST_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table")
        if not table:
            raise ValueError("Could not find file list table")

        # First XLSX link is the latest
        link = table.find("a", href=re.compile(r"\.xlsx$"))
        if not link:
            raise ValueError("Could not find XLSX download link")

        return link["href"]

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        xlsx_url = self._get_latest_xlsx_url()
        print(f"  Downloading: {xlsx_url[:80]}...")

        resp = requests.get(
            xlsx_url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=120,
        )
        resp.raise_for_status()

        tmp_path = os.path.join(tempfile.gettempdir(), "tr_titck.xlsx")
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        # Read Excel — first row is title, second row has actual headers
        raw = pd.read_excel(tmp_path, header=1)
        print(f"  Downloaded {len(raw)} total products")

        # Standardize column names
        col_map = {}
        for c in raw.columns:
            cl = str(c).strip().upper()
            if "SIRA" in cl:
                col_map[c] = "row_no"
            elif "BARKOD" in cl:
                col_map[c] = "barcode"
            elif "ÜRÜN ADI" in cl:
                col_map[c] = "product_name"
            elif "ETKİN MADDE" in cl:
                col_map[c] = "active_substance"
            elif "ATC" in cl:
                col_map[c] = "atc_code"
            elif "RUHSAT SAHİBİ" in cl:
                col_map[c] = "license_holder"
            elif "RUHSAT TARİHİ" in cl:
                col_map[c] = "license_date"
            elif "RUHSAT NUMARASI" in cl:
                col_map[c] = "license_number"
            elif "DEĞİŞİKLİK TAR" in cl:
                col_map[c] = "change_date"
            elif "DEĞİŞİKLİK" in cl:
                col_map[c] = "change_flag"
            elif "ASKIYA" in cl and "TAR" in cl:
                col_map[c] = "suspension_date"
            elif "ASKIDA" in cl or "ASKIYA" in cl:
                col_map[c] = "suspension_status"

        raw = raw.rename(columns=col_map)

        # Filter to only suspended products (status > 0)
        if "suspension_status" in raw.columns:
            suspended = raw[raw["suspension_status"].fillna(0).astype(int) > 0].copy()
            print(f"  Suspended products: {len(suspended)}")
        else:
            suspended = raw.copy()

        records = []
        for _, row in suspended.iterrows():
            status_code = int(row.get("suspension_status", 0))
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(row.get("product_name", "")),
                "active_substance": str(row.get("active_substance", "")),
                "strength": "",
                "package_size": "",
                "barcode": str(row.get("barcode", "")),
                "atc_code": str(row.get("atc_code", "")),
                "license_holder": str(row.get("license_holder", "")),
                "license_number": str(row.get("license_number", "")),
                "suspension_status": status_code,
                "status": self.STATUS_MAP.get(status_code, f"unknown_{status_code}"),
                "suspension_date": str(row.get("suspension_date", "")) if pd.notna(row.get("suspension_date")) else None,
                "shortage_start": None,
                "estimated_end": None,
                "scraped_at": datetime.now().isoformat(),
            })

        # Clean up temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} suspended product records scraped")
        return df
