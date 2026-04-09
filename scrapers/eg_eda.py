"""Scraper for Egypt EDA (Egyptian Drug Authority) medicine shortage data."""

import re
import tempfile
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class EgEdaScraper(BaseScraper):
    """Scraper for EDA (Egypt) drug shortage / supply bulletin data.

    The Egyptian Drug Authority publishes periodic PDF bulletins about
    medicine shortages and supply disruptions. This scraper:
    1. Searches the EDA website for shortage/supply bulletin pages
    2. Finds PDF download links for the latest bulletins
    3. Downloads and parses PDF tables using tabula
    4. Maps Arabic/English column headers to standard fields

    Scrapeerbaarheid: 2 stars - periodic PDF bulletins, structure may vary.
    """

    # Known and candidate URLs for shortage/supply bulletins
    BULLETIN_URLS = [
        "https://www.edaegypt.gov.eg/en/services/drug-shortages/",
        "https://www.edaegypt.gov.eg/en/services/drug-shortage/",
        "https://www.edaegypt.gov.eg/en/media-corner/publications/",
        "https://www.edaegypt.gov.eg/en/media-corner/news/",
        "https://www.edaegypt.gov.eg/en/services/",
        "https://www.edaegypt.gov.eg/ar/services/drug-shortages/",
        "https://www.edaegypt.gov.eg/ar/services/",
        "https://www.edaegypt.gov.eg/en/publications/",
        "https://www.edaegypt.gov.eg/ar/publications/",
    ]

    # Fallback: search main pages for links to shortage bulletins
    SEARCH_URLS = [
        "https://www.edaegypt.gov.eg/en/",
        "https://www.edaegypt.gov.eg/ar/",
        "https://www.edaegypt.gov.eg",
        "https://www.edaegypt.gov.eg/en/media-corner/",
        "https://www.edaegypt.gov.eg/ar/media-corner/",
    ]

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    }

    # Arabic and English keywords for identifying shortage-related links
    SHORTAGE_KEYWORDS_AR = [
        "نقص",          # shortage
        "نواقص",        # shortages
        "عجز",          # deficit
        "توفر",         # availability
        "إمداد",        # supply
        "نشرة",         # bulletin
        "أدوية",        # medicines
        "متاح",         # available
        "غير متوفر",    # unavailable
    ]

    SHORTAGE_KEYWORDS_EN = [
        "shortage", "shortages", "supply", "bulletin", "unavailable",
        "out of stock", "drug supply", "medicine supply", "availability",
        "disruption", "deficit",
    ]

    # Column mapping: Arabic/English header hints -> standard field names
    COLUMN_HINTS = {
        # Arabic hints
        "اسم المنتج": "medicine_name",
        "اسم الدواء": "medicine_name",
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
        "البديل": "therapeutic_alternative",
        "البدائل": "therapeutic_alternative",
        "البدائل العلاجية": "therapeutic_alternative",
        "ملاحظات": "notes",
        # English hints
        "product name": "medicine_name",
        "trade name": "medicine_name",
        "brand name": "medicine_name",
        "medicine name": "medicine_name",
        "drug name": "medicine_name",
        "active substance": "active_substance",
        "active ingredient": "active_substance",
        "inn": "active_substance",
        "strength": "strength",
        "concentration": "strength",
        "dose": "strength",
        "status": "status",
        "shortage start": "shortage_start",
        "start date": "shortage_start",
        "date of shortage": "shortage_start",
        "expected availability": "estimated_end",
        "expected end": "estimated_end",
        "estimated end": "estimated_end",
        "resolution date": "estimated_end",
        "manufacturer": "manufacturer",
        "company": "company",
        "mah": "company",
        "marketing auth": "company",
        "package size": "package_size",
        "pack size": "package_size",
        "dosage form": "dosage_form",
        "form": "dosage_form",
        "registration": "registration_no",
        "reg no": "registration_no",
        "shortage reason": "shortage_reason",
        "reason": "shortage_reason",
        "alternative": "therapeutic_alternative",
        "alternatives": "therapeutic_alternative",
        "notes": "notes",
        "remarks": "notes",
    }

    def __init__(self):
        super().__init__(
            country_code="EG",
            country_name="Egypt",
            source_name="EDA",
            base_url="https://www.edaegypt.gov.eg",
        )

    def _fetch_page(self, url: str) -> requests.Response | None:
        """Fetch a page with error handling. Returns None on failure."""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30, verify=True)
            if resp.status_code == 200:
                return resp
            print(f"    HTTP {resp.status_code} for {url}")
        except requests.exceptions.SSLError:
            # Retry without SSL verification for government sites with cert issues
            try:
                resp = requests.get(url, headers=self.HEADERS, timeout=30, verify=False)
                if resp.status_code == 200:
                    print(f"    Warning: SSL verification disabled for {url}")
                    return resp
            except requests.RequestException as e:
                print(f"    Request failed (no SSL) for {url}: {e}")
        except requests.RequestException as e:
            print(f"    Request failed for {url}: {e}")
        return None

    def _parse_date(self, val) -> str | None:
        """Parse a date value from various formats into YYYY-MM-DD.

        Handles Arabic date formats, European date formats, and standard ISO dates.
        """
        if pd.isna(val) or not val:
            return None
        val_str = str(val).strip()
        if not val_str or val_str.lower() in ("nan", "-", "n/a", "غير محدد", "غير معروف", ""):
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
            "%d/%m/%y",
            "%d-%m-%y",
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

    def _is_shortage_link(self, href: str, link_text: str) -> bool:
        """Check if a link is related to drug shortage bulletins."""
        href_lower = href.lower()
        text_lower = link_text.lower()
        combined = f"{href_lower} {text_lower}"

        for kw in self.SHORTAGE_KEYWORDS_EN:
            if kw in combined:
                return True
        for kw in self.SHORTAGE_KEYWORDS_AR:
            if kw in link_text:
                return True
        return False

    def _find_pdf_links(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        """Extract PDF links from a page that are related to shortages."""
        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            link_text = a.get_text(strip=True)

            # Direct PDF links
            if href.lower().endswith(".pdf"):
                full_url = urljoin(page_url, href)
                if self._is_shortage_link(href, link_text) or not pdf_links:
                    # Prefer shortage-related PDFs, but collect all PDFs as fallback
                    pdf_links.append(full_url)

            # Links that might lead to a PDF download (common on gov sites)
            elif any(kw in href.lower() for kw in ("download", "attachment", "file", "document")):
                full_url = urljoin(page_url, href)
                if self._is_shortage_link(href, link_text):
                    pdf_links.append(full_url)

        return pdf_links

    def _find_bulletin_page(self) -> tuple[str, BeautifulSoup, list[str]] | None:
        """Find the EDA page with shortage bulletins and extract PDF links.

        Returns (page_url, soup, pdf_links) or None if nothing found.
        """
        all_pdf_links = []

        # Try known bulletin URLs
        for url in self.BULLETIN_URLS:
            print(f"    Trying: {url}")
            resp = self._fetch_page(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(strip=True).lower()

            # Check if page has shortage-related content
            has_content = any(kw in text for kw in self.SHORTAGE_KEYWORDS_EN)
            if not has_content:
                has_content = any(kw in soup.get_text(strip=True) for kw in self.SHORTAGE_KEYWORDS_AR)

            if has_content:
                pdf_links = self._find_pdf_links(soup, url)
                if pdf_links:
                    print(f"    Found shortage page with {len(pdf_links)} PDF link(s): {url}")
                    return url, soup, pdf_links
                all_pdf_links_from_page = self._find_pdf_links(soup, url)
                all_pdf_links.extend(all_pdf_links_from_page)

        # Search main pages for shortage-related links
        print("    Direct URLs not found, searching main site...")
        for search_url in self.SEARCH_URLS:
            resp = self._fetch_page(search_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                link_text = a.get_text(strip=True)
                if self._is_shortage_link(href, link_text):
                    full_url = urljoin(search_url, href)
                    print(f"    Found shortage link: {full_url}")
                    resp2 = self._fetch_page(full_url)
                    if resp2:
                        soup2 = BeautifulSoup(resp2.text, "lxml")
                        pdf_links = self._find_pdf_links(soup2, full_url)
                        if pdf_links:
                            return full_url, soup2, pdf_links

                        # The linked page might have HTML table data instead of PDFs
                        tables = soup2.find_all("table")
                        if tables:
                            return full_url, soup2, []

        if all_pdf_links:
            return self.base_url, None, all_pdf_links

        return None

    def _map_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Map raw column names to standard names using substring matching."""
        col_map = {}
        for raw_col in df.columns:
            raw_lower = str(raw_col).strip().lower()
            raw_stripped = str(raw_col).strip()
            for hint, standard in self.COLUMN_HINTS.items():
                if hint in raw_stripped or hint in raw_lower:
                    if standard not in col_map.values():
                        col_map[raw_col] = standard
                    break
        return col_map

    def _find_header_row(self, df_raw: pd.DataFrame) -> int | None:
        """Detect which row contains the actual column headers.

        PDF-extracted tables often have title rows or blank rows before
        the real headers. Scan the first 15 rows for recognizable keywords.
        """
        keywords = [
            # Arabic
            "اسم", "المادة", "التركيز", "الحالة", "تاريخ", "الدواء", "المستحضر",
            # English
            "product", "active", "strength", "status", "date",
            "name", "substance", "shortage", "medicine", "drug",
        ]
        for i in range(min(15, len(df_raw))):
            row_text = " ".join(
                str(v).strip().lower() for v in df_raw.iloc[i].values if pd.notna(v)
            )
            matches = sum(1 for kw in keywords if kw in row_text)
            if matches >= 2:
                return i
        return None

    def _parse_pdf_tables(self, pdf_path: str) -> list[dict]:
        """Extract shortage records from a PDF bulletin using tabula."""
        try:
            import tabula
        except ImportError:
            print("    WARNING: tabula-py not installed. Install with: pip install tabula-py")
            print("    Falling back to pdfplumber...")
            return self._parse_pdf_pdfplumber(pdf_path)

        records = []

        # Try lattice mode first (for PDFs with gridlines), then stream mode
        for mode_name, kwargs in [
            ("lattice", {"lattice": True}),
            ("stream", {"stream": True}),
        ]:
            try:
                dfs = tabula.read_pdf(pdf_path, pages="all", **kwargs)
                if dfs:
                    print(f"    tabula ({mode_name}): found {len(dfs)} table(s)")
                    break
            except Exception as e:
                print(f"    tabula ({mode_name}) failed: {e}")
                dfs = []

        if not dfs:
            print("    tabula found no tables, trying pdfplumber...")
            return self._parse_pdf_pdfplumber(pdf_path)

        for table_idx, df in enumerate(dfs):
            if df.empty or len(df) < 2:
                continue

            # Try to detect header row
            header_row = self._find_header_row(df)
            if header_row is not None:
                # Use the detected row as header
                new_headers = [str(v).strip() if pd.notna(v) else f"col_{i}"
                               for i, v in enumerate(df.iloc[header_row])]
                df = df.iloc[header_row + 1:].copy()
                df.columns = new_headers
            else:
                # Check if current column names look like headers
                col_text = " ".join(str(c).lower() for c in df.columns)
                if not any(kw in col_text for kw in ("name", "اسم", "product", "medicine")):
                    # Columns are not meaningful headers, use positional mapping
                    df.columns = [f"col_{i}" for i in range(len(df.columns))]

            # Drop fully empty rows
            df = df.dropna(how="all")

            # Map columns
            col_map = self._map_columns(df)
            if col_map:
                print(f"    Table {table_idx + 1}: mapped columns: {col_map}")
                df = df.rename(columns=col_map)

            # If no medicine_name column found via mapping, try positional assignment
            if "medicine_name" not in df.columns:
                # In many EDA bulletins, first meaningful text column is medicine name
                for col in df.columns:
                    sample_vals = df[col].dropna().astype(str).head(5)
                    if any(len(v) > 3 and not v.replace(".", "").isdigit() for v in sample_vals):
                        df = df.rename(columns={col: "medicine_name"})
                        break

            if "medicine_name" not in df.columns:
                print(f"    Table {table_idx + 1}: no medicine_name column found, skipping")
                continue

            # Extract records
            for _, row in df.iterrows():
                medicine = str(row.get("medicine_name", "")).strip() if pd.notna(row.get("medicine_name")) else ""
                medicine = medicine.replace("\r", " ").replace("\n", " ").strip()
                if not medicine or medicine.lower() in ("nan", "-", ""):
                    continue
                # Skip rows that look like headers
                if any(kw in medicine.lower() for kw in ("product name", "medicine name", "اسم المنتج", "اسم الدواء")):
                    continue

                def _get_field(field: str) -> str:
                    val = row.get(field)
                    if pd.notna(val):
                        s = str(val).strip().replace("\r", " ").replace("\n", " ")
                        return s if s.lower() not in ("nan", "-", "n/a") else ""
                    return ""

                status_raw = _get_field("status")
                status = status_raw if status_raw else "shortage"

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": medicine,
                    "active_substance": _get_field("active_substance"),
                    "strength": _get_field("strength"),
                    "package_size": _get_field("package_size"),
                    "dosage_form": _get_field("dosage_form"),
                    "status": status,
                    "shortage_start": self._parse_date(row.get("shortage_start")),
                    "estimated_end": self._parse_date(row.get("estimated_end")),
                    "shortage_reason": _get_field("shortage_reason"),
                    "manufacturer": _get_field("manufacturer"),
                    "company": _get_field("company"),
                    "therapeutic_alternative": _get_field("therapeutic_alternative"),
                    "notes": _get_field("notes"),
                    "registration_no": _get_field("registration_no"),
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    def _parse_pdf_pdfplumber(self, pdf_path: str) -> list[dict]:
        """Fallback PDF parser using pdfplumber (better for Arabic text)."""
        try:
            import pdfplumber
        except ImportError:
            print("    WARNING: pdfplumber not installed. Install with: pip install pdfplumber")
            return []

        records = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                print(f"    pdfplumber: {len(pdf.pages)} page(s)")
                all_rows = []
                header_found = False
                col_map = {}

                for page_num, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if not tables:
                        continue

                    for table in tables:
                        for row in table:
                            if not row or all(cell is None or str(cell).strip() == "" for cell in row):
                                continue

                            row_text = " ".join(str(c).strip().lower() for c in row if c)

                            # Detect header row
                            if not header_found:
                                header_keywords = ["اسم", "name", "المادة", "substance",
                                                   "التركيز", "strength", "product", "medicine"]
                                if sum(1 for kw in header_keywords if kw in row_text) >= 2:
                                    # This is the header row - map columns
                                    for i, cell in enumerate(row):
                                        if cell:
                                            cell_str = str(cell).strip()
                                            for hint, standard in self.COLUMN_HINTS.items():
                                                if hint in cell_str or hint in cell_str.lower():
                                                    if standard not in col_map.values():
                                                        col_map[i] = standard
                                                    break
                                    header_found = True
                                    print(f"    Header found on page {page_num + 1}: {col_map}")
                                    continue

                            if header_found:
                                all_rows.append(row)

                # If no header found, try positional mapping
                if not header_found and all_rows:
                    print("    No header detected, using positional mapping")
                    # Assume: col 0 = medicine_name, col 1 = active_substance, col 2 = strength
                    col_map = {0: "medicine_name"}
                    if len(all_rows[0]) > 1:
                        col_map[1] = "active_substance"
                    if len(all_rows[0]) > 2:
                        col_map[2] = "strength"

                # Build records from rows
                for row in all_rows:
                    def _cell(idx: int) -> str:
                        if idx < len(row) and row[idx]:
                            s = str(row[idx]).strip().replace("\r", " ").replace("\n", " ")
                            return s if s.lower() not in ("nan", "-", "n/a", "") else ""
                        return ""

                    medicine_idx = None
                    for idx, field in col_map.items():
                        if field == "medicine_name":
                            medicine_idx = idx
                            break

                    if medicine_idx is None:
                        continue

                    medicine = _cell(medicine_idx)
                    if not medicine:
                        continue

                    def _get_mapped(field_name: str) -> str:
                        for idx, field in col_map.items():
                            if field == field_name:
                                return _cell(idx)
                        return ""

                    status_raw = _get_mapped("status")
                    status = status_raw if status_raw else "shortage"

                    records.append({
                        "country_code": self.country_code,
                        "country_name": self.country_name,
                        "source": self.source_name,
                        "medicine_name": medicine,
                        "active_substance": _get_mapped("active_substance"),
                        "strength": _get_mapped("strength"),
                        "package_size": _get_mapped("package_size"),
                        "dosage_form": _get_mapped("dosage_form"),
                        "status": status,
                        "shortage_start": self._parse_date(_get_mapped("shortage_start")),
                        "estimated_end": self._parse_date(_get_mapped("estimated_end")),
                        "shortage_reason": _get_mapped("shortage_reason"),
                        "manufacturer": _get_mapped("manufacturer"),
                        "company": _get_mapped("company"),
                        "therapeutic_alternative": _get_mapped("therapeutic_alternative"),
                        "notes": _get_mapped("notes"),
                        "registration_no": _get_mapped("registration_no"),
                        "scraped_at": datetime.now().isoformat(),
                    })

        except Exception as e:
            print(f"    pdfplumber error: {e}")

        return records

    def _parse_html_table(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Extract shortage records from HTML tables on the page."""
        records = []
        tables = soup.find_all("table")
        if not tables:
            return records

        for table_idx, table in enumerate(tables):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Extract header row
            header_row = rows[0]
            headers = []
            for th in header_row.find_all(["th", "td"]):
                headers.append(th.get_text(strip=True).lower())

            if not headers:
                continue

            # Map column indices to standard fields
            col_map = {}
            for i, h in enumerate(headers):
                h_clean = re.sub(r"\s+", " ", h)
                for hint, standard in self.COLUMN_HINTS.items():
                    if hint in h_clean or hint in h:
                        if standard not in col_map.values():
                            col_map[standard] = i
                        break

            # If no medicine_name column found, try first non-serial text column
            if "medicine_name" not in col_map and len(headers) >= 2:
                for i, h in enumerate(headers):
                    if i not in col_map.values() and not re.match(r"^(#|no|s\.?no|sr)", h):
                        col_map["medicine_name"] = i
                        break

            if "medicine_name" not in col_map:
                continue

            print(f"    HTML Table {table_idx + 1}: {len(rows) - 1} data rows, columns: {list(col_map.keys())}")

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                cell_texts = [c.get_text(strip=True) for c in cells]

                med_idx = col_map.get("medicine_name")
                if med_idx is None or med_idx >= len(cell_texts):
                    continue
                medicine = cell_texts[med_idx]
                if not medicine or medicine.lower() in ("", "-", "nan"):
                    continue

                def _get(field: str) -> str:
                    idx = col_map.get(field)
                    if idx is not None and idx < len(cell_texts):
                        v = cell_texts[idx]
                        return v if v.lower() not in ("", "-", "nan", "n/a") else ""
                    return ""

                status_raw = _get("status")
                status = status_raw if status_raw else "shortage"

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": medicine,
                    "active_substance": _get("active_substance"),
                    "strength": _get("strength"),
                    "package_size": _get("package_size"),
                    "dosage_form": _get("dosage_form"),
                    "manufacturer": _get("manufacturer"),
                    "company": _get("company"),
                    "registration_no": _get("registration_no"),
                    "shortage_reason": _get("shortage_reason"),
                    "status": status,
                    "shortage_start": self._parse_date(_get("shortage_start")),
                    "estimated_end": self._parse_date(_get("estimated_end")),
                    "therapeutic_alternative": _get("therapeutic_alternative"),
                    "notes": _get("notes"),
                    "source_url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        print(f"  Base URL: {self.base_url}")

        empty_df = pd.DataFrame(columns=[
            "country_code", "country_name", "source",
            "medicine_name", "active_substance", "strength",
            "package_size", "dosage_form", "manufacturer", "company",
            "registration_no", "shortage_reason", "status",
            "shortage_start", "estimated_end",
            "therapeutic_alternative", "notes",
            "source_url", "scraped_at",
        ])

        # Step 1: Find bulletin page and PDF links
        result = self._find_bulletin_page()
        if not result:
            print("  WARNING: Could not find EDA shortage bulletin page.")
            print("  The EDA website may have changed structure or shortage data")
            print("  may not be available online. Manual URL verification needed.")
            return empty_df

        page_url, soup, pdf_links = result
        all_records = []

        # Step 2: Try HTML tables first (if the page has them)
        if soup is not None:
            html_records = self._parse_html_table(soup, page_url)
            if html_records:
                print(f"  Found {len(html_records)} records from HTML tables")
                all_records.extend(html_records)

        # Step 3: Download and parse PDF bulletins
        if pdf_links:
            # Limit to most recent bulletins (max 5)
            pdf_links = pdf_links[:5]
            print(f"  Found {len(pdf_links)} PDF bulletin(s) to process")

            for pdf_idx, pdf_url in enumerate(pdf_links):
                print(f"  Downloading PDF {pdf_idx + 1}/{len(pdf_links)}: {pdf_url}")
                try:
                    resp = requests.get(
                        pdf_url,
                        headers=self.HEADERS,
                        timeout=120,
                        verify=True,
                    )
                except requests.exceptions.SSLError:
                    try:
                        resp = requests.get(
                            pdf_url,
                            headers=self.HEADERS,
                            timeout=120,
                            verify=False,
                        )
                    except requests.RequestException as e:
                        print(f"    Failed to download PDF: {e}")
                        continue
                except requests.RequestException as e:
                    print(f"    Failed to download PDF: {e}")
                    continue

                if resp.status_code != 200:
                    print(f"    HTTP {resp.status_code} for PDF download")
                    continue

                # Check if response is actually a PDF
                content_type = resp.headers.get("Content-Type", "")
                if "pdf" not in content_type.lower() and not resp.content[:5].startswith(b"%PDF"):
                    print(f"    Response is not a PDF (Content-Type: {content_type})")
                    continue

                # Save to temp file
                tmp_path = os.path.join(
                    tempfile.gettempdir(),
                    f"eg_eda_bulletin_{pdf_idx}.pdf"
                )
                with open(tmp_path, "wb") as f:
                    f.write(resp.content)
                print(f"    Downloaded {len(resp.content) / 1024:.0f} KB")

                # Parse PDF
                pdf_records = self._parse_pdf_tables(tmp_path)
                if pdf_records:
                    # Add source_url to each record
                    for rec in pdf_records:
                        rec["source_url"] = pdf_url
                    all_records.extend(pdf_records)
                    print(f"    Extracted {len(pdf_records)} records from PDF")
                else:
                    print(f"    No structured records found in PDF")

                # Clean up temp file
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        # Step 4: If still no records and we have a soup, try scanning for
        # structured content in divs/articles
        if not all_records and soup is not None:
            print("  No table/PDF records found, trying structured content scan...")
            all_records = self._parse_structured_content(soup, page_url)

        # Build DataFrame
        df = pd.DataFrame(all_records) if all_records else empty_df

        if df.empty:
            print("  WARNING: No shortage records found.")
            print("  The EDA may not publish structured shortage data online,")
            print("  or the website structure has changed.")
        else:
            # Deduplicate based on medicine_name + strength
            before = len(df)
            dedup_cols = ["medicine_name", "strength"]
            available_dedup = [c for c in dedup_cols if c in df.columns]
            if available_dedup:
                df = df.drop_duplicates(subset=available_dedup, keep="first")
            if len(df) < before:
                print(f"  Removed {before - len(df)} duplicate records")

        print(f"  Total: {len(df)} shortage records scraped")
        return df

    def _parse_structured_content(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Fallback: extract records from non-table structured content."""
        records = []

        # Look for structured content in divs, articles, or list items
        containers = soup.find_all(["article", "div", "li", "section"], class_=re.compile(
            r"(shortage|drug|medicine|item|entry|post|card|bulletin|notice)", re.I
        ))

        for container in containers:
            text = container.get_text(" ", strip=True)
            if len(text) < 10:
                continue

            # Try to extract medicine name from headings or bold text
            medicine = ""
            heading = container.find(["h2", "h3", "h4", "h5", "strong", "b"])
            if heading:
                medicine = heading.get_text(strip=True)

            if not medicine:
                continue

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine,
                "active_substance": "",
                "strength": "",
                "package_size": "",
                "dosage_form": "",
                "manufacturer": "",
                "company": "",
                "registration_no": "",
                "shortage_reason": "",
                "status": "shortage",
                "shortage_start": None,
                "estimated_end": None,
                "therapeutic_alternative": "",
                "notes": text[:500],  # Keep some context
                "source_url": page_url,
                "scraped_at": datetime.now().isoformat(),
            })

        return records
