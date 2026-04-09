"""Scraper for Saudi Arabia SFDA (Saudi Food and Drug Authority) medicine shortage data."""

import requests
import pandas as pd
from datetime import datetime
import tempfile
import os

from scrapers.base_scraper import BaseScraper


class SaSfdaScraper(BaseScraper):
    """Scraper for SFDA (Saudi Arabia) current medicine shortage data.

    Downloads the Excel file of currently-in-shortage medicines directly
    from the SFDA website.
    """

    EXCEL_URL = "https://www.sfda.gov.sa/GetExcel.php?ftype=CurrentlyInShortage"

    # Column mapping: Arabic headers -> English standard names.
    # Keys are substrings matched case-insensitively against the column header.
    # The SFDA file may use Arabic or English headers depending on the version.
    ARABIC_COLUMN_HINTS = {
        # Arabic hints
        "اسم المنتج": "medicine_name",
        "اسم المستحضر": "medicine_name",
        "الاسم التجاري": "medicine_name",
        "المادة الفعالة": "active_substance",
        "التركيز": "strength",
        "القوة": "strength",
        "الجرعة": "strength",
        "الحالة": "status",
        "حالة": "status",
        "تاريخ البداية": "shortage_start",
        "تاريخ بداية النقص": "shortage_start",
        "تاريخ بداية": "shortage_start",
        "تاريخ النقص": "shortage_start",
        "تاريخ التوفر المتوقع": "estimated_end",
        "تاريخ الانتهاء المتوقع": "estimated_end",
        "تاريخ العودة": "estimated_end",
        "الشركة المصنعة": "manufacturer",
        "المصنع": "manufacturer",
        "الشركة": "company",
        "حجم العبوة": "package_size",
        "العبوة": "package_size",
        "الشكل الصيدلاني": "dosage_form",
        "شكل الجرعة": "dosage_form",
        "رقم التسجيل": "registration_no",
        "سبب النقص": "shortage_reason",
        # English hints (SFDA sometimes uses English headers)
        "product name": "medicine_name",
        "trade name": "medicine_name",
        "tradename": "medicine_name",
        "brand name": "medicine_name",
        "active substance": "active_substance",
        "active ingredient": "active_substance",
        "scientificname": "active_substance",
        "strength": "strength",
        "concentration": "strength",
        "status": "status",
        "shortage_type": "status",
        "shortage start": "shortage_start",
        "shortage_start": "shortage_start",
        "start date": "shortage_start",
        "expected availability": "estimated_end",
        "expected end": "estimated_end",
        "estimated end": "estimated_end",
        "expected date": "estimated_end",
        "resolution date": "estimated_end",
        "update_time": "estimated_end",
        "manufacturer": "manufacturer",
        "manufacturer_name": "manufacturer",
        "company": "company",
        "agent": "company",
        "package size": "package_size",
        "dosage form": "dosage_form",
        "registration": "registration_no",
        "registration_no": "registration_no",
        "shortage reason": "shortage_reason",
        "shortage_reason": "shortage_reason",
        "reason": "shortage_reason",
    }

    def __init__(self):
        super().__init__(
            country_code="SA",
            country_name="Saudi Arabia",
            source_name="SFDA",
            base_url="https://www.sfda.gov.sa",
        )

    def _map_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Map raw column names to standard names using substring matching."""
        col_map = {}
        for raw_col in df.columns:
            raw_lower = str(raw_col).strip().lower()
            raw_stripped = str(raw_col).strip()
            for hint, standard in self.ARABIC_COLUMN_HINTS.items():
                if hint in raw_stripped or hint in raw_lower:
                    # Only map if not already mapped (first match wins)
                    if standard not in col_map.values():
                        col_map[raw_col] = standard
                    break
        return col_map

    def _find_header_row(self, df_raw: pd.DataFrame) -> int | None:
        """Detect which row contains the actual headers.

        Some SFDA exports have a title row or blank rows before the
        real column headers. We scan the first 15 rows for one that
        contains at least two recognizable header keywords.
        """
        keywords = [
            "اسم", "المادة", "التركيز", "الحالة", "تاريخ",
            "product", "active", "strength", "status", "date",
            "name", "substance", "shortage",
        ]
        for i in range(min(15, len(df_raw))):
            row_text = " ".join(
                str(v).strip().lower() for v in df_raw.iloc[i].values if pd.notna(v)
            )
            matches = sum(1 for kw in keywords if kw in row_text)
            if matches >= 2:
                return i
        return None

    def _parse_date(self, val) -> str | None:
        """Parse a date value from various formats into YYYY-MM-DD."""
        if pd.isna(val) or not val:
            return None
        val_str = str(val).strip()
        if not val_str or val_str.lower() in ("nan", "-", "n/a", "غير محدد", ""):
            return None

        # Try common date formats
        for fmt in (
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%Y%m%d",
        ):
            try:
                return datetime.strptime(val_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try pandas date parsing as fallback
        try:
            parsed = pd.to_datetime(val_str, dayfirst=True)
            if pd.notna(parsed):
                return parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        print(f"  Downloading Excel from: {self.EXCEL_URL}")

        resp = requests.get(
            self.EXCEL_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=120,
        )
        resp.raise_for_status()

        # Determine file extension from content type or default to .xlsx
        content_type = resp.headers.get("Content-Type", "")
        if "spreadsheetml" in content_type or "xlsx" in content_type:
            ext = ".xlsx"
        elif "ms-excel" in content_type or "xls" in content_type:
            ext = ".xls"
        else:
            # Detect from magic bytes
            if resp.content[:4] == b"PK\x03\x04":
                ext = ".xlsx"
            elif resp.content[:4] == b"\xd0\xcf\x11\xe0":
                ext = ".xls"
            else:
                ext = ".xlsx"

        tmp_path = os.path.join(tempfile.gettempdir(), f"sa_sfda_shortage{ext}")
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        print(f"  Downloaded {len(resp.content)} bytes")

        # First pass: read without header to detect header row
        try:
            df_raw = pd.read_excel(tmp_path, header=None)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {e}") from e

        header_row = self._find_header_row(df_raw)
        if header_row is not None:
            print(f"  Detected header at row {header_row}")
            data = pd.read_excel(tmp_path, header=header_row)
        else:
            # Fall back to first row as header
            print("  Using first row as header")
            data = pd.read_excel(tmp_path, header=0)

        # Drop fully empty rows
        data = data.dropna(how="all")
        # Drop unnamed columns
        data = data.loc[:, ~data.columns.astype(str).str.startswith("Unnamed")]

        print(f"  Raw rows: {len(data)}")
        print(f"  Columns found: {list(data.columns)}")

        # Map columns to standard names
        col_map = self._map_columns(data)
        print(f"  Column mapping: {col_map}")
        data = data.rename(columns=col_map)

        # Build output records
        records = []
        for _, row in data.iterrows():
            medicine_name = str(row.get("medicine_name", "")).strip() if pd.notna(row.get("medicine_name")) else ""
            if not medicine_name or medicine_name.lower() == "nan":
                continue

            active_substance = (
                str(row.get("active_substance", "")).strip()
                if pd.notna(row.get("active_substance"))
                else ""
            )
            strength = (
                str(row.get("strength", "")).strip()
                if pd.notna(row.get("strength"))
                else ""
            )
            status_raw = (
                str(row.get("status", "")).strip()
                if pd.notna(row.get("status"))
                else "shortage"
            )
            status = status_raw if status_raw and status_raw.lower() != "nan" else "shortage"

            record = {
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "active_substance": active_substance,
                "strength": strength,
                "package_size": (
                    str(row.get("package_size", "")).strip()
                    if pd.notna(row.get("package_size"))
                    else ""
                ),
                "dosage_form": (
                    str(row.get("dosage_form", "")).strip()
                    if pd.notna(row.get("dosage_form"))
                    else ""
                ),
                "status": status,
                "shortage_start": self._parse_date(row.get("shortage_start")),
                "estimated_end": self._parse_date(row.get("estimated_end")),
                "shortage_reason": (
                    str(row.get("shortage_reason", "")).strip()
                    if pd.notna(row.get("shortage_reason"))
                    else ""
                ),
                "manufacturer": (
                    str(row.get("manufacturer", "")).strip()
                    if pd.notna(row.get("manufacturer"))
                    else ""
                ),
                "company": (
                    str(row.get("company", "")).strip()
                    if pd.notna(row.get("company"))
                    else ""
                ),
                "registration_no": (
                    str(row.get("registration_no", "")).strip()
                    if pd.notna(row.get("registration_no"))
                    else ""
                ),
                "scraped_at": datetime.now().isoformat(),
            }

            # Keep any unmapped columns as-is for transparency
            for orig_col in data.columns:
                if orig_col not in col_map.values() and orig_col not in record:
                    val = row.get(orig_col)
                    if pd.notna(val):
                        record[orig_col] = str(val).strip()

            records.append(record)

        # Clean up temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
