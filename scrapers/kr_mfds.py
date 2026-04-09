"""Scraper for South Korea MFDS drug shortage data."""

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import tempfile
import os

from scrapers.base_scraper import BaseScraper


class KrMfdsScraper(BaseScraper):
    """Scraper for MFDS (South Korea) drug supply shortage data via open data CSV."""

    CSV_URL = "https://nedrug.mfds.go.kr/cmn/xls/downc/OpenData_PotOpenMdcinSl"
    DRUG_DETAIL_URL = "https://nedrug.mfds.go.kr/pbp/CCBGA01/getItemDetail"

    # Column mapping: Korean -> English
    COLUMN_MAP = {
        "보고번호": "report_no",
        "진행단계": "progress_stage",
        "업소일련번호": "company_serial",
        "업체명": "company_name",
        "업체 영문명": "company_name_en",
        "업체허가번호": "company_license_no",
        "업체소재지": "company_address",
        "부서접수번호": "dept_receipt_no",
        "품목일련번호": "product_serial",
        "품목명": "product_name",
        "품목 영문명": "product_name_en",
        "표준코드": "standard_code",
        "공급부족발생예상일자": "expected_shortage_date",
        "공급부족사유": "shortage_reason",
        "마지막생산수입공급일자": "last_supply_date",
        "생산수입공급구분": "supply_type",
        "자사재고량기준일": "inventory_date",
        "자사재고량": "inventory_quantity",
        "환자치료에미치는영향": "patient_impact",
        "공급정상화추진계획": "normalization_plan",
        "공급정상화예상일자": "expected_normalization_date",
        "보고일자": "report_date",
        "처리일자": "process_date",
        "전자민원창구공개여부": "public_disclosure",
        "사업자번호": "business_no",
    }

    def __init__(self):
        super().__init__(
            country_code="KR",
            country_name="South Korea",
            source_name="MFDS",
            base_url="https://nedrug.mfds.go.kr",
        )

    def _parse_date(self, val) -> str | None:
        if pd.isna(val) or not val:
            return None
        val = str(val).strip().replace("-", "")
        if not val or val == "-":
            return None
        try:
            if len(val) == 8:
                return datetime.strptime(val, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
        return None

    def _lookup_substance(self, item_seq: str) -> str:
        """Look up active substance from MFDS drug detail page."""
        if not item_seq:
            return ""
        try:
            resp = requests.get(
                self.DRUG_DETAIL_URL,
                params={"itemSeq": item_seq},
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # Look for "주성분" (main ingredient) or "유효성분" (active ingredient)
            for pattern in (
                r"주성분[:\s]*([^\n]+?)(?:\s*(?:첨가제|성상|효능|$))",
                r"유효성분[:\s]*([^\n]+?)(?:\s*(?:첨가제|성상|효능|$))",
                r"[Aa]ctive\s+[Ii]ngredient[s]?[:\s]*([^\n]+?)(?:\s*(?:Inactive|Appearance|$))",
            ):
                m = re.search(pattern, text)
                if m:
                    substance = m.group(1).strip().rstrip(",. ")
                    if substance and len(substance) >= 2:
                        return substance
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(
            self.CSV_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=60,
        )
        resp.raise_for_status()

        tmp_path = os.path.join(tempfile.gettempdir(), "kr_mfds_shortage.csv")
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        raw = pd.read_csv(tmp_path, encoding="utf-8")
        print(f"  Downloaded {len(raw)} records")

        # Rename columns to English
        raw = raw.rename(columns=self.COLUMN_MAP)

        # Drop unnamed columns
        raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]

        # Batch-lookup unique product serials for active substances
        unique_serials = set()
        if "product_serial" in raw.columns:
            unique_serials = {
                str(v).strip() for v in raw["product_serial"].dropna().unique()
                if str(v).strip() and str(v).strip() != "nan"
            }
        print(f"  Looking up active substances for {len(unique_serials)} unique products...")
        serial_substance_map: dict[str, str] = {}
        for i, serial in enumerate(unique_serials):
            serial_substance_map[serial] = self._lookup_substance(serial)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_serials)} lookups done")
            time.sleep(0.15)
        found = sum(1 for v in serial_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(unique_serials)} products")

        # Add standard fields
        raw.insert(0, "country_code", self.country_code)
        raw.insert(1, "country_name", self.country_name)
        raw.insert(2, "source", self.source_name)

        # Rename product fields for consistency
        raw["medicine_name"] = raw["product_name"]
        raw["active_substance"] = raw.get("product_serial", pd.Series(dtype=str)).apply(
            lambda x: serial_substance_map.get(str(x).strip(), "") if pd.notna(x) else ""
        )
        raw["strength"] = ""
        raw["package_size"] = ""
        raw["status"] = raw["progress_stage"]

        # Parse dates
        raw["shortage_start"] = raw["expected_shortage_date"].apply(self._parse_date)
        raw["estimated_end"] = raw["expected_normalization_date"].apply(self._parse_date)

        raw["scraped_at"] = datetime.now().isoformat()

        # Clean up temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass

        print(f"  Total: {len(raw)} shortage records scraped")
        return raw
