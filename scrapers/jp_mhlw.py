"""Scraper for Japan MHLW (Ministry of Health, Labour and Welfare) drug shortage data."""

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import tempfile
import os

from scrapers.base_scraper import BaseScraper


class JpMhlwScraper(BaseScraper):
    """Scraper for MHLW (Japan) pharmaceutical supply status data via Excel download.

    MHLW publishes a comprehensive Excel file listing the supply status of all
    medical pharmaceuticals in Japan. The file is updated regularly and linked
    from the supply shortage information page. Each row represents one product
    with its current shipment status, shortage reason, and resolution outlook.

    Source page: https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/iryou/kouhatu-iyaku/04_00003.html
    """

    SUPPLY_PAGE_URL = (
        "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/iryou/"
        "kouhatu-iyaku/04_00003.html"
    )

    # Column indices in the Excel file (header row 1, 0-indexed after header)
    # These are the Japanese column names with their circled-number prefixes.
    COL_DRUG_CATEGORY = 0       # ①薬剤区分
    COL_THERAPEUTIC_CLASS = 1   # ②薬効分類
    COL_SUBSTANCE = 2           # ③成分名
    COL_STRENGTH = 3            # ④規格単位
    COL_YJ_CODE = 4             # ⑤YJコード
    COL_PRODUCT_NAME = 5        # ⑥品名
    COL_MANUFACTURER = 6        # ⑦製造販売業者名
    COL_PRODUCT_TYPE = 7        # ⑧製品区分
    COL_ESSENTIAL_DRUG = 8      # ⑨基礎的医薬品
    COL_SUPPLY_SECURE = 9       # ⑩供給確保医薬品
    COL_PRICE_LIST_DATE = 10    # ⑪薬価収載年月日
    COL_SHIPMENT_STATUS = 11    # ⑫出荷対応の状況
    COL_STATUS_UPDATE = 12      # ⑬⑫の情報を更新した日
    COL_SHORTAGE_REASON = 13    # ⑭出荷停止等の理由
    COL_RESOLUTION_PROSPECT = 14  # ⑮出荷停止等の解消見込み
    COL_RESOLUTION_DATE = 15    # ⑯解消見込み時期
    COL_OUTPUT_STATUS = 16      # ⑰出荷量の現在の状況
    COL_OUTPUT_IMPROVE_DATE = 17  # ⑱出荷量の改善見込み時期
    COL_OUTPUT_IMPROVE_VOL = 18   # ⑲改善見込み量
    COL_OTHER_UPDATE_DATE = 19  # ⑳⑫以外の情報を更新した日
    COL_NEW_FLAG = 20           # 今回掲載時の更新有無

    # Shipment status mapping (Japanese -> English)
    SHIPMENT_STATUS_MAP = {
        "①通常出荷": "normal_shipment",
        "②限定出荷（自社の事情）": "limited_shipment_own_reasons",
        "③限定出荷（他社品の影響）": "limited_shipment_other_company",
        "④限定出荷（その他）": "limited_shipment_other",
        "⑤供給停止": "supply_suspended",
    }

    # Shortage reason mapping
    SHORTAGE_REASON_MAP = {
        "１．需要増": "increased_demand",
        "２．原材料調達上の問題": "raw_material_procurement",
        "３．製造トラブル（製造委託を含む）": "manufacturing_trouble",
        "４．品質トラブル（製造委託を含む）": "quality_trouble",
        "５．行政処分（製造委託を含む）": "administrative_action",
        "６．薬価削除": "price_delisting",
        "７．ー": "none",
        "８．その他の理由": "other",
    }

    # Resolution prospect mapping
    RESOLUTION_PROSPECT_MAP = {
        "ア． あり": "expected",
        "イ． なし": "not_expected",
        "ウ． 未定": "undetermined",
        "エ． －": "not_applicable",
    }

    # Output volume status mapping
    OUTPUT_STATUS_MAP = {
        "A．出荷量通常": "output_normal",
        "Aプラス．出荷量増加": "output_increased",
        "B．出荷量減少": "output_decreased",
        "C．出荷停止": "output_halted",
        "D．薬価削除予定": "delisting_planned",
    }

    def __init__(self):
        super().__init__(
            country_code="JP",
            country_name="Japan",
            source_name="MHLW",
            base_url="https://www.mhlw.go.jp",
        )

    def _find_excel_url(self) -> str:
        """Scrape the MHLW supply status page to find the latest Excel download URL.

        The page typically has a single .xlsx link pointing to the current
        supply status file. The filename encodes the date (e.g. 260318iyakuhinkyoukyu.xlsx).
        """
        print("  Finding latest Excel download URL...")
        resp = requests.get(
            self.SUPPLY_PAGE_URL,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=30,
        )
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for .xlsx links on the page
        xlsx_links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if href.endswith(".xlsx") or href.endswith(".xls"):
                # Build absolute URL
                if href.startswith("http"):
                    full_url = href
                elif href.startswith("/"):
                    full_url = self.base_url + href
                else:
                    full_url = self.base_url + "/" + href
                xlsx_links.append(full_url)

        if not xlsx_links:
            raise RuntimeError(
                f"No Excel files found on {self.SUPPLY_PAGE_URL}. "
                "The page structure may have changed."
            )

        # Prefer the file that contains 'iyakuhinkyoukyu' (pharmaceutical supply) in the name
        for url in xlsx_links:
            if "iyakuhinkyoukyu" in url.lower():
                print(f"  Found supply status Excel: {url}")
                return url

        # Fall back to the first xlsx link if no specific match
        print(f"  Using first Excel link found: {xlsx_links[0]}")
        return xlsx_links[0]

    def _download_excel(self, url: str) -> str:
        """Download the Excel file and return the local temp file path."""
        print(f"  Downloading Excel from {url}...")
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=120,
            stream=True,
        )
        resp.raise_for_status()

        tmp_path = os.path.join(tempfile.gettempdir(), "jp_mhlw_supply.xlsx")
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(tmp_path) / 1024
        print(f"  Downloaded {size_kb:.0f} KB")
        return tmp_path

    def _parse_date(self, val) -> str | None:
        """Parse a date value from the Excel file.

        Handles:
        - pandas Timestamp / datetime objects
        - Strings like '2026年3月頃' (approximate dates, extract YYYY-MM if possible)
        - Strings with embedded dates like '2025年6月3日回収開始'
        - NaN / None / dash values
        """
        if pd.isna(val) or val is None:
            return None

        # If it is already a datetime/Timestamp, format it directly
        if isinstance(val, (datetime, pd.Timestamp)):
            return val.strftime("%Y-%m-%d")

        s = str(val).strip()
        if not s or s in ("-", "ー", "－", "―", "nan"):
            return None

        # Try ISO-style datetime string first (e.g. "2026-04-01 00:00:00")
        try:
            return pd.to_datetime(s).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

        # Try Japanese date format: 2026年3月15日...
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", s)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try partial Japanese date: 2026年3月頃 -> first of month
        m = re.search(r"(\d{4})年(\d{1,2})月", s)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), 1).strftime("%Y-%m-%d")
            except ValueError:
                pass

        return None

    def _map_shipment_status(self, val) -> str:
        """Map Japanese shipment status to a standardized status string."""
        if pd.isna(val) or val is None:
            return "unknown"
        s = str(val).strip()
        return self.SHIPMENT_STATUS_MAP.get(s, s)

    def _map_shortage_reason(self, val) -> str:
        """Map Japanese shortage reason to English."""
        if pd.isna(val) or val is None:
            return ""
        s = str(val).strip()
        return self.SHORTAGE_REASON_MAP.get(s, s)

    def _map_resolution_prospect(self, val) -> str:
        """Map Japanese resolution prospect to English."""
        if pd.isna(val) or val is None:
            return ""
        s = str(val).strip()
        return self.RESOLUTION_PROSPECT_MAP.get(s, s)

    def _map_output_status(self, val) -> str:
        """Map Japanese output volume status to English."""
        if pd.isna(val) or val is None:
            return ""
        s = str(val).strip()
        return self.OUTPUT_STATUS_MAP.get(s, s)

    def _derive_status(self, shipment_status: str, output_status: str) -> str:
        """Derive a simple standardized status from shipment and output status.

        Returns one of: 'shortage', 'limited', 'normal', 'suspended', 'delisting', 'unknown'.
        """
        if shipment_status == "supply_suspended" or output_status == "output_halted":
            return "suspended"
        if shipment_status in (
            "limited_shipment_own_reasons",
            "limited_shipment_other_company",
            "limited_shipment_other",
        ):
            return "limited"
        if output_status == "output_decreased":
            return "shortage"
        if output_status == "delisting_planned":
            return "delisting"
        if shipment_status == "normal_shipment":
            return "normal"
        return "unknown"

    def scrape(self) -> pd.DataFrame:
        """Scrape the MHLW pharmaceutical supply status Excel file.

        Returns a DataFrame with standardized columns plus Japan-specific extras.
        """
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Step 1: Find the latest Excel URL from the supply status page
        try:
            excel_url = self._find_excel_url()
        except Exception as e:
            print(f"  Error finding Excel URL: {e}")
            raise

        time.sleep(1)  # Rate limiting between page fetch and file download

        # Step 2: Download the Excel file
        tmp_path = self._download_excel(excel_url)

        try:
            # Step 3: Read the Excel file
            # The file has one sheet named '公表' (published), header in row 1 (0-indexed)
            raw = pd.read_excel(tmp_path, sheet_name=0, header=1)
            print(f"  Loaded {len(raw)} records from Excel")

            # Get actual column names (they include circled numbers and line breaks)
            cols = raw.columns.tolist()

            # Build the output DataFrame using column positions
            records = []
            for _, row in raw.iterrows():
                shipment_status_jp = row.iloc[self.COL_SHIPMENT_STATUS] if len(cols) > self.COL_SHIPMENT_STATUS else None
                output_status_jp = row.iloc[self.COL_OUTPUT_STATUS] if len(cols) > self.COL_OUTPUT_STATUS else None

                shipment_status_en = self._map_shipment_status(shipment_status_jp)
                output_status_en = self._map_output_status(output_status_jp)
                status = self._derive_status(shipment_status_en, output_status_en)

                # Parse the resolution / estimated end date
                resolution_date_raw = row.iloc[self.COL_RESOLUTION_DATE] if len(cols) > self.COL_RESOLUTION_DATE else None
                estimated_end = self._parse_date(resolution_date_raw)

                # The MHLW data does not have an explicit "shortage start" date.
                # Use the status-update date or the other-update date as a proxy.
                status_update_raw = row.iloc[self.COL_STATUS_UPDATE] if len(cols) > self.COL_STATUS_UPDATE else None
                other_update_raw = row.iloc[self.COL_OTHER_UPDATE_DATE] if len(cols) > self.COL_OTHER_UPDATE_DATE else None
                shortage_start = self._parse_date(status_update_raw) or self._parse_date(other_update_raw)

                record = {
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": str(row.iloc[self.COL_PRODUCT_NAME]).strip() if pd.notna(row.iloc[self.COL_PRODUCT_NAME]) else "",
                    "active_substance": str(row.iloc[self.COL_SUBSTANCE]).strip() if pd.notna(row.iloc[self.COL_SUBSTANCE]) else "",
                    "strength": str(row.iloc[self.COL_STRENGTH]).strip() if pd.notna(row.iloc[self.COL_STRENGTH]) else "",
                    "package_size": "",
                    "shortage_start": shortage_start,
                    "estimated_end": estimated_end,
                    "status": status,
                    "scraped_at": datetime.now().isoformat(),
                    # Japan-specific additional fields
                    "drug_category": str(row.iloc[self.COL_DRUG_CATEGORY]).strip() if pd.notna(row.iloc[self.COL_DRUG_CATEGORY]) else "",
                    "therapeutic_class": str(row.iloc[self.COL_THERAPEUTIC_CLASS]).strip() if pd.notna(row.iloc[self.COL_THERAPEUTIC_CLASS]) else "",
                    "yj_code": str(row.iloc[self.COL_YJ_CODE]).strip() if pd.notna(row.iloc[self.COL_YJ_CODE]) else "",
                    "manufacturer": str(row.iloc[self.COL_MANUFACTURER]).strip() if pd.notna(row.iloc[self.COL_MANUFACTURER]) else "",
                    "product_type": str(row.iloc[self.COL_PRODUCT_TYPE]).strip() if pd.notna(row.iloc[self.COL_PRODUCT_TYPE]) else "",
                    "shipment_status": shipment_status_en,
                    "shipment_status_jp": str(shipment_status_jp).strip() if pd.notna(shipment_status_jp) else "",
                    "shortage_reason": self._map_shortage_reason(
                        row.iloc[self.COL_SHORTAGE_REASON] if len(cols) > self.COL_SHORTAGE_REASON else None
                    ),
                    "shortage_reason_jp": str(row.iloc[self.COL_SHORTAGE_REASON]).strip() if len(cols) > self.COL_SHORTAGE_REASON and pd.notna(row.iloc[self.COL_SHORTAGE_REASON]) else "",
                    "resolution_prospect": self._map_resolution_prospect(
                        row.iloc[self.COL_RESOLUTION_PROSPECT] if len(cols) > self.COL_RESOLUTION_PROSPECT else None
                    ),
                    "output_status": output_status_en,
                    "output_status_jp": str(output_status_jp).strip() if pd.notna(output_status_jp) else "",
                    "is_new": str(row.iloc[self.COL_NEW_FLAG]).strip().upper() == "NEW" if len(cols) > self.COL_NEW_FLAG and pd.notna(row.iloc[self.COL_NEW_FLAG]) else False,
                }
                records.append(record)

            df = pd.DataFrame(records)

            # Summary statistics
            status_counts = df["status"].value_counts()
            print(f"  Status breakdown:")
            for s, c in status_counts.items():
                print(f"    {s}: {c}")
            new_count = df["is_new"].sum()
            if new_count:
                print(f"  New/updated entries: {new_count}")

            print(f"  Total: {len(df)} records scraped")
            return df

        finally:
            # Clean up temp file
            try:
                os.remove(tmp_path)
            except OSError:
                pass
