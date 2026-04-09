"""Scraper for Argentina ANMAT (Administración Nacional de Medicamentos) shortage data."""

import re
import tempfile
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class ArAnmatScraper(BaseScraper):
    """Scraper for ANMAT (Argentina) medicine shortage / faltante data.

    ANMAT publishes shortage information on the Argentina.gob.ar platform.
    The data may appear as HTML tables on the page or as downloadable
    PDF/Excel files. This scraper tries multiple strategies:
      1. Look for structured HTML tables on the shortage page
      2. Look for downloadable Excel/CSV files
      3. Fall back to PDF parsing with tabula
    """

    # Primary page listing shortage / faltante information
    SHORTAGE_PAGE = (
        "https://www.argentina.gob.ar/anmat/regulados/"
        "medicamentos/faltantes-de-medicamentos"
    )

    # Alternative / older ANMAT domain
    ANMAT_BASE = "http://www.anmat.gov.ar"

    # Possible alternative URLs where shortage data may live
    ALT_URLS = [
        "https://www.argentina.gob.ar/anmat/regulados/medicamentos/faltantes-de-medicamentos",
        "https://www.argentina.gob.ar/anmat/faltante-de-medicamentos",
        "https://www.argentina.gob.ar/anmat/regulados/medicamentos",
    ]

    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    def __init__(self):
        super().__init__(
            country_code="AR",
            country_name="Argentina",
            source_name="ANMAT",
            base_url="https://www.argentina.gob.ar/anmat",
        )

    # ------------------------------------------------------------------
    # Date parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date(date_str) -> str | None:
        """Parse common Spanish/Argentine date formats to YYYY-MM-DD."""
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str or date_str.lower() in ("nan", "-", "n/a", "s/d", "sin datos"):
            return None
        for fmt in (
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d-%m-%Y",
            "%d-%m-%y",
            "%d.%m.%Y",
            "%d.%m.%y",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # Fetching helpers
    # ------------------------------------------------------------------

    def _get_page(self, url: str) -> requests.Response:
        """GET a URL with standard headers and timeout."""
        resp = requests.get(url, headers=self.HEADERS, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Strategy 1: HTML tables
    # ------------------------------------------------------------------

    def _try_html_tables(self, soup: BeautifulSoup) -> pd.DataFrame | None:
        """Extract shortage data from HTML tables on the page."""
        tables = soup.find_all("table")
        if not tables:
            return None

        all_records = []
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Extract header row
            header_cells = rows[0].find_all(["th", "td"])
            headers = [cell.get_text(strip=True).upper() for cell in header_cells]

            # Map columns by Spanish keywords
            col_map = self._map_spanish_columns(headers)
            if not col_map.get("medicine_name"):
                # Not a shortage table, skip
                continue

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                values = [cell.get_text(strip=True) for cell in cells]

                record = self._extract_record_from_row(values, col_map)
                if record and record.get("medicine_name"):
                    all_records.append(record)

        if all_records:
            return pd.DataFrame(all_records)
        return None

    def _map_spanish_columns(self, headers: list[str]) -> dict[str, int | None]:
        """Map Spanish column headers to standard field names."""
        col_map: dict[str, int | None] = {
            "medicine_name": None,
            "active_substance": None,
            "strength": None,
            "status": None,
            "shortage_start": None,
            "estimated_end": None,
            "laboratory": None,
            "reason": None,
            "notes": None,
        }
        for i, h in enumerate(headers):
            h_clean = h.upper()
            if any(kw in h_clean for kw in [
                "PRODUCTO", "MEDICAMENTO", "NOMBRE COMERCIAL",
                "ESPECIALIDAD", "DENOMINACI",
            ]):
                col_map["medicine_name"] = i
            elif any(kw in h_clean for kw in [
                "PRINCIPIO ACTIVO", "DROGA", "IFA", "MONODROGA",
                "DCI", "SUSTANCIA",
            ]):
                col_map["active_substance"] = i
            elif any(kw in h_clean for kw in [
                "CONCENTRACI", "DOSIS", "POTENCIA", "PRESENTACI",
            ]):
                col_map["strength"] = i
            elif any(kw in h_clean for kw in [
                "ESTADO", "SITUACI", "STATUS",
            ]):
                col_map["status"] = i
            elif any(kw in h_clean for kw in [
                "FECHA INICIO", "DESDE", "FECHA DE FALTANTE",
                "FECHA NOTIFICACI", "FECHA",
            ]):
                if col_map["shortage_start"] is None:
                    col_map["shortage_start"] = i
            elif any(kw in h_clean for kw in [
                "FECHA FIN", "FECHA ESTIMADA", "FECHA NORMALIZACI",
                "HASTA", "REPOSICI",
            ]):
                col_map["estimated_end"] = i
            elif any(kw in h_clean for kw in [
                "LABORATORIO", "TITULAR", "EMPRESA",
            ]):
                col_map["laboratory"] = i
            elif any(kw in h_clean for kw in [
                "MOTIVO", "CAUSA", "RAZ",
            ]):
                col_map["reason"] = i
            elif any(kw in h_clean for kw in [
                "OBSERVACI", "NOTA", "COMENTARIO",
            ]):
                col_map["notes"] = i

        return col_map

    def _extract_record_from_row(
        self, values: list[str], col_map: dict[str, int | None]
    ) -> dict | None:
        """Build a standard record dict from a row using column mapping."""

        def _get(field: str) -> str:
            idx = col_map.get(field)
            if idx is not None and idx < len(values):
                val = values[idx].strip()
                return val if val.lower() not in ("nan", "-", "") else ""
            return ""

        medicine = _get("medicine_name")
        if not medicine:
            return None

        # Determine status from Spanish text
        raw_status = _get("status").lower()
        if any(kw in raw_status for kw in ["resuelto", "normalizado", "disponible"]):
            status = "resolved"
        elif any(kw in raw_status for kw in ["faltante", "desabastecimiento", "no disponible"]):
            status = "shortage"
        elif raw_status:
            status = raw_status
        else:
            status = "shortage"

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine,
            "active_substance": _get("active_substance"),
            "strength": _get("strength"),
            "package_size": "",
            "laboratory": _get("laboratory"),
            "shortage_start": self._parse_date(_get("shortage_start")),
            "estimated_end": self._parse_date(_get("estimated_end")),
            "status": status,
            "reason": _get("reason"),
            "notes": _get("notes"),
            "scraped_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Strategy 2: Downloadable files (Excel / CSV)
    # ------------------------------------------------------------------

    def _find_download_links(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Find all downloadable file links on the page."""
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            href_lower = href.lower()

            # Check for relevant file types
            is_excel = any(ext in href_lower for ext in [".xls", ".xlsx", ".csv"])
            is_pdf = href_lower.endswith(".pdf")
            is_relevant = any(kw in href_lower + " " + text for kw in [
                "faltante", "desabastecimiento", "shortage", "medicamento",
                "listado", "lista",
            ])

            if (is_excel or is_pdf) and is_relevant:
                full_url = href
                if href.startswith("/"):
                    full_url = base_url.rstrip("/") + href
                elif not href.startswith("http"):
                    full_url = base_url.rstrip("/") + "/" + href
                links.append({
                    "url": full_url,
                    "type": "excel" if is_excel else "pdf",
                    "text": text,
                })

            # Also check for generic download links
            if not (is_excel or is_pdf) and is_relevant:
                if "download" in href_lower or "descarga" in text:
                    full_url = href
                    if href.startswith("/"):
                        full_url = base_url.rstrip("/") + href
                    elif not href.startswith("http"):
                        full_url = base_url.rstrip("/") + "/" + href
                    links.append({
                        "url": full_url,
                        "type": "unknown",
                        "text": text,
                    })

        return links

    def _try_excel_download(self, url: str) -> pd.DataFrame | None:
        """Download and parse an Excel/CSV file."""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Failed to download {url}: {e}")
            return None

        # Determine file type from content
        content = resp.content
        suffix = ".xlsx"
        if content[:4] == b"\xd0\xcf\x11\xe0":
            suffix = ".xls"
        elif content[:4] == b"PK\x03\x04":
            suffix = ".xlsx"
        elif url.lower().endswith(".csv") or b"," in content[:200]:
            suffix = ".csv"

        tmp_path = os.path.join(tempfile.gettempdir(), f"ar_anmat{suffix}")
        with open(tmp_path, "wb") as f:
            f.write(content)

        try:
            if suffix == ".csv":
                df = pd.read_csv(tmp_path, encoding="utf-8", on_bad_lines="skip")
            else:
                # Try to find header row
                raw = pd.read_excel(tmp_path, header=None)
                header_row = self._find_header_row(raw)
                df = pd.read_excel(tmp_path, header=header_row)

            return self._normalize_dataframe(df)

        except Exception as e:
            print(f"    Failed to parse {suffix} file: {e}")
            return None
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    def _find_header_row(self, raw: pd.DataFrame) -> int:
        """Find the header row in a raw DataFrame by looking for Spanish keywords."""
        for i in range(min(20, len(raw))):
            row_text = " ".join(
                str(v).upper() for v in raw.iloc[i].values if pd.notna(v)
            )
            if any(kw in row_text for kw in [
                "PRODUCTO", "MEDICAMENTO", "PRINCIPIO",
                "DROGA", "LABORATORIO",
            ]):
                return i
        return 0  # fallback

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame | None:
        """Normalize a DataFrame with Spanish columns to standard records."""
        if df.empty:
            return None

        # Map column names
        headers = [str(c).strip().upper() for c in df.columns]
        col_map = self._map_spanish_columns(headers)

        if col_map["medicine_name"] is None:
            return None

        # Build reverse map: index -> original column name
        idx_to_col = {i: c for i, c in enumerate(df.columns)}

        records = []
        for _, row in df.iterrows():
            values = [str(row.iloc[i]).strip() if pd.notna(row.iloc[i]) else "" for i in range(len(row))]
            record = self._extract_record_from_row(values, col_map)
            if record and record.get("medicine_name"):
                records.append(record)

        if records:
            return pd.DataFrame(records)
        return None

    # ------------------------------------------------------------------
    # Strategy 3: PDF download and parsing
    # ------------------------------------------------------------------

    def _try_pdf_download(self, url: str) -> pd.DataFrame | None:
        """Download and parse a PDF file using tabula."""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Failed to download PDF {url}: {e}")
            return None

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        print(f"    Downloaded PDF: {len(resp.content) / 1024:.0f} KB")

        try:
            import tabula
            dfs = tabula.read_pdf(tmp_path, pages="all", lattice=True)
            if not dfs:
                # Try stream mode if lattice finds nothing
                dfs = tabula.read_pdf(tmp_path, pages="all", stream=True)
            print(f"    Found {len(dfs)} table(s) in PDF")

            all_rows = []
            header_map = None

            for df in dfs:
                if df.empty:
                    continue

                # Try to identify header from first table
                if header_map is None:
                    headers = [str(c).strip().upper() for c in df.columns]
                    candidate_map = self._map_spanish_columns(headers)
                    if candidate_map.get("medicine_name") is not None:
                        header_map = candidate_map

                # Also check first few rows for header
                if header_map is None:
                    for i in range(min(3, len(df))):
                        row_vals = [
                            str(v).strip().upper()
                            for v in df.iloc[i].values
                            if pd.notna(v)
                        ]
                        candidate_map = self._map_spanish_columns(row_vals)
                        if candidate_map.get("medicine_name") is not None:
                            header_map = candidate_map
                            break

                # Extract data rows
                for _, row in df.iterrows():
                    values = [
                        str(v).strip().replace("\r", " ") if pd.notna(v) else ""
                        for v in row.values
                    ]
                    # Skip header-like rows
                    first_val = values[0].upper() if values else ""
                    if any(kw in first_val for kw in [
                        "PRODUCTO", "MEDICAMENTO", "PRINCIPIO",
                        "NAN", "N°", "NRO",
                    ]):
                        continue
                    if all(v == "" for v in values):
                        continue
                    all_rows.append(values)

            print(f"    Parsed {len(all_rows)} data rows from PDF")

            if not all_rows:
                return None

            # Build records
            records = []
            for values in all_rows:
                if header_map and header_map.get("medicine_name") is not None:
                    record = self._extract_record_from_row(values, header_map)
                else:
                    # Fallback: assume first non-empty column is medicine name
                    record = self._build_fallback_record(values)

                if record and record.get("medicine_name"):
                    records.append(record)

            if records:
                return pd.DataFrame(records)
            return None

        except ImportError:
            print("    tabula-py not installed, skipping PDF parsing")
            return None
        except Exception as e:
            print(f"    Failed to parse PDF: {e}")
            return None
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _build_fallback_record(self, values: list[str]) -> dict | None:
        """Build a record when column mapping is unavailable.

        Uses heuristics to identify fields from positional data.
        """
        # Filter out empty values
        non_empty = [v for v in values if v.strip()]
        if not non_empty:
            return None

        medicine = non_empty[0] if non_empty else ""
        if not medicine or len(medicine) < 2:
            return None

        # Try to find a date-like value for start/end
        start_date = None
        end_date = None
        substance = ""
        strength = ""

        for v in non_empty[1:]:
            parsed = self._parse_date(v)
            if parsed:
                if start_date is None:
                    start_date = parsed
                elif end_date is None:
                    end_date = parsed
            elif re.match(r"^\d+\s*(mg|ml|g|mcg|ui|%)", v, re.IGNORECASE):
                strength = v
            elif not substance and len(v) > 3 and not v.isdigit():
                substance = v

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine,
            "active_substance": substance,
            "strength": strength,
            "package_size": "",
            "shortage_start": start_date,
            "estimated_end": end_date,
            "status": "shortage",
            "scraped_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Strategy 4: Scrape structured text content (non-table)
    # ------------------------------------------------------------------

    def _try_structured_text(self, soup: BeautifulSoup) -> pd.DataFrame | None:
        """Extract shortage data from structured text blocks (lists, divs, etc.)."""
        # Look for article or content sections with medicine listings
        content_sections = soup.find_all(
            ["article", "div"],
            class_=re.compile(r"(content|field|body|text|panel)", re.IGNORECASE),
        )

        records = []
        for section in content_sections:
            # Look for list items that might contain medicine info
            items = section.find_all(["li", "p", "tr"])
            for item in items:
                text = item.get_text(strip=True)
                if len(text) < 10:
                    continue

                # Check if text mentions medicine-related Spanish keywords
                text_lower = text.lower()
                if not any(kw in text_lower for kw in [
                    "faltante", "desabastecimiento", "mg", "ml",
                    "comprimido", "inyectable", "cápsula", "tableta",
                    "solución", "suspensión", "laboratorio",
                ]):
                    continue

                # Try to extract medicine name and details
                record = self._parse_text_entry(text)
                if record:
                    records.append(record)

        if records:
            return pd.DataFrame(records)
        return None

    def _parse_text_entry(self, text: str) -> dict | None:
        """Parse a free-text entry into a structured record."""
        if not text or len(text) < 10:
            return None

        # Try common patterns:
        # "Medicine Name - Active Substance - Strength - Laboratory"
        # "Medicine Name (strength) - Laboratory"
        parts = re.split(r"\s*[-–|]\s*", text)
        if len(parts) < 1:
            return None

        medicine = parts[0].strip()
        if not medicine or len(medicine) < 3:
            return None

        substance = ""
        strength = ""
        laboratory = ""

        for part in parts[1:]:
            part = part.strip()
            if re.search(r"\d+\s*(mg|ml|g|mcg|ui|%)", part, re.IGNORECASE):
                strength = part
            elif any(kw in part.lower() for kw in ["laboratorio", "lab.", "s.a.", "s.r.l."]):
                laboratory = part
            elif not substance and len(part) > 2:
                substance = part

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine,
            "active_substance": substance,
            "strength": strength,
            "package_size": "",
            "laboratory": laboratory,
            "shortage_start": None,
            "estimated_end": None,
            "status": "shortage",
            "scraped_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        """Scrape ANMAT shortage data using multiple strategies.

        Tries in order:
        1. HTML tables on the shortage page
        2. Downloadable Excel/CSV files
        3. PDF download and parsing
        4. Structured text extraction
        """
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Try each URL until we find data
        for url in self.ALT_URLS:
            print(f"  Trying: {url}")
            try:
                resp = self._get_page(url)
            except requests.RequestException as e:
                print(f"    Page not accessible: {e}")
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Strategy 1: HTML tables
            print("    Checking for HTML tables...")
            df = self._try_html_tables(soup)
            if df is not None and not df.empty:
                print(f"  Found {len(df)} records from HTML tables")
                return df

            # Strategy 2: Downloadable files
            print("    Checking for downloadable files...")
            download_links = self._find_download_links(soup, url)
            if download_links:
                print(f"    Found {len(download_links)} download link(s)")

                # Try Excel/CSV files first
                for link in download_links:
                    if link["type"] == "excel":
                        print(f"    Trying Excel: {link['url'][:80]}...")
                        df = self._try_excel_download(link["url"])
                        if df is not None and not df.empty:
                            print(f"  Found {len(df)} records from Excel")
                            return df

                # Then try PDFs
                for link in download_links:
                    if link["type"] == "pdf":
                        print(f"    Trying PDF: {link['url'][:80]}...")
                        df = self._try_pdf_download(link["url"])
                        if df is not None and not df.empty:
                            print(f"  Found {len(df)} records from PDF")
                            return df

                # Try unknown type links
                for link in download_links:
                    if link["type"] == "unknown":
                        print(f"    Trying unknown link: {link['url'][:80]}...")
                        df = self._try_excel_download(link["url"])
                        if df is not None and not df.empty:
                            print(f"  Found {len(df)} records from download")
                            return df

            # Strategy 3: Structured text
            print("    Checking for structured text content...")
            df = self._try_structured_text(soup)
            if df is not None and not df.empty:
                print(f"  Found {len(df)} records from structured text")
                return df

        # If all strategies fail, return empty DataFrame
        print("  WARNING: No shortage data found from any strategy.")
        print("  ANMAT page structure may have changed. Manual review needed.")
        return pd.DataFrame(columns=[
            "country_code", "country_name", "source",
            "medicine_name", "active_substance", "strength",
            "package_size", "shortage_start", "estimated_end",
            "status", "scraped_at",
        ])
