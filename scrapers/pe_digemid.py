"""Scraper for Peru DIGEMID (Direccion General de Medicamentos) medicine shortage data."""

import re
import tempfile
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class PeDigemidScraper(BaseScraper):
    """Scraper for DIGEMID (Peru) medicine shortage alerts.

    DIGEMID publishes alertas de desabastecimiento (shortage alerts) as PDF
    documents and occasionally as HTML listings on their website. This scraper
    tries HTML-based extraction first, then falls back to downloading and
    parsing PDF alert documents.

    Scrapeerbaarheid: 2 stars - content is mainly in PDF alerts.
    """

    BASE = "https://www.digemid.minsa.gob.pe"

    # Known and candidate URLs for shortage / desabastecimiento alerts
    SHORTAGE_URLS = [
        "https://www.digemid.minsa.gob.pe/main.asp?seccion=3&categoria=4",
        "https://www.digemid.minsa.gob.pe/Main.asp?Seccion=3",
        "https://www.digemid.minsa.gob.pe/webDigemid/?q=alertas",
        "https://www.digemid.minsa.gob.pe/webDigemid/?q=desabastecimiento",
        "https://www.digemid.minsa.gob.pe/webDigemid/?q=alertas-de-desabastecimiento",
        "https://www.digemid.minsa.gob.pe/Alertas/Desabastecimiento",
    ]

    # Fallback: search from main pages for shortage-related links
    SEARCH_URLS = [
        "https://www.digemid.minsa.gob.pe",
        "https://www.digemid.minsa.gob.pe/Main.asp",
        "https://www.digemid.minsa.gob.pe/webDigemid/",
    ]

    # Spanish keywords that indicate shortage / supply disruption content
    SHORTAGE_KEYWORDS = [
        "desabastecimiento",
        "desabastecim",
        "falta de stock",
        "escasez",
        "agotamiento",
        "no disponible",
        "alerta",
        "suministro",
        "disponibilidad",
    ]

    # Spanish column header hints -> standard field names
    SPANISH_COLUMN_HINTS = {
        # Medicine / product name
        "nombre del producto": "medicine_name",
        "nombre del medicamento": "medicine_name",
        "producto": "medicine_name",
        "medicamento": "medicine_name",
        "denominacion": "medicine_name",
        "denominaci\u00f3n": "medicine_name",
        "nombre comercial": "medicine_name",
        # Active substance / INN
        "denominacion comun internacional": "active_substance",
        "denominaci\u00f3n com\u00fan internacional": "active_substance",
        "principio activo": "active_substance",
        "sustancia activa": "active_substance",
        "dci": "active_substance",
        "inn": "active_substance",
        # Strength / concentration
        "concentraci\u00f3n": "strength",
        "concentracion": "strength",
        "dosis": "strength",
        "potencia": "strength",
        # Dosage form
        "forma farmac\u00e9utica": "dosage_form",
        "forma farmaceutica": "dosage_form",
        "forma de presentaci\u00f3n": "dosage_form",
        "presentaci\u00f3n": "dosage_form",
        "presentacion": "dosage_form",
        # Status
        "estado": "status",
        "situaci\u00f3n": "status",
        "situacion": "status",
        # Dates
        "fecha de inicio": "shortage_start",
        "fecha inicio": "shortage_start",
        "fecha de notificaci\u00f3n": "shortage_start",
        "fecha notificaci\u00f3n": "shortage_start",
        "fecha de notificacion": "shortage_start",
        "fecha notificacion": "shortage_start",
        "fecha de alerta": "shortage_start",
        "fecha": "shortage_start",
        "fecha estimada": "estimated_end",
        "fecha de resoluci\u00f3n": "estimated_end",
        "fecha de resolucion": "estimated_end",
        "fecha de normalizaci\u00f3n": "estimated_end",
        "fecha de normalizacion": "estimated_end",
        "fecha prevista": "estimated_end",
        # Manufacturer / laboratory
        "laboratorio": "manufacturer",
        "titular": "manufacturer",
        "fabricante": "manufacturer",
        # Registration
        "registro sanitario": "registration_no",
        "n\u00famero de registro": "registration_no",
        "numero de registro": "registration_no",
        # Reason
        "motivo": "reason",
        "causa": "reason",
        "raz\u00f3n": "reason",
        "razon": "reason",
        "observaciones": "reason",
        "observaci\u00f3n": "reason",
        "observacion": "reason",
    }

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.5",
    }

    def __init__(self):
        super().__init__(
            country_code="PE",
            country_name="Peru",
            source_name="DIGEMID",
            base_url="https://www.digemid.minsa.gob.pe",
        )

    # ------------------------------------------------------------------
    # Date parsing
    # ------------------------------------------------------------------

    def _parse_date(self, val) -> str | None:
        """Parse a date string into ISO format, handling Spanish date formats."""
        if pd.isna(val) or not val:
            return None
        val_str = str(val).strip()
        if not val_str or val_str.lower() in (
            "nan", "-", "n/a", "no disponible", "no determinada",
            "por determinar", "nd", "",
        ):
            return None

        # Replace Spanish month names with numbers for text-based dates
        spanish_months = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "setiembre": "09",  # Peruvian variant
            "octubre": "10", "noviembre": "11", "diciembre": "12",
        }

        val_lower = val_str.lower().strip()
        # Handle "15 de enero de 2024" or "15 de enero 2024"
        m = re.match(
            r"(\d{1,2})\s+de\s+(\w+)\s+(?:de\s+)?(\d{4})", val_lower
        )
        if m:
            day, month_name, year = m.groups()
            month_num = spanish_months.get(month_name)
            if month_num:
                try:
                    return datetime(
                        int(year), int(month_num), int(day)
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Try common numeric formats
        for fmt in (
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%d/%m/%y",
            "%Y/%m/%d",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(val_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Pandas fallback with day-first (common in Latin America)
        try:
            parsed = pd.to_datetime(val_str, dayfirst=True)
            if pd.notna(parsed):
                return parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

        return None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _fetch_page(self, url: str, timeout: int = 30) -> requests.Response | None:
        """Fetch a page with error handling. Returns None on failure."""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp
            print(f"    HTTP {resp.status_code} for {url}")
        except requests.RequestException as e:
            print(f"    Request failed for {url}: {e}")
        return None

    def _page_has_shortage_content(self, soup: BeautifulSoup) -> bool:
        """Check whether a parsed page contains shortage-related content."""
        text = soup.get_text(" ", strip=True).lower()
        return any(kw in text for kw in self.SHORTAGE_KEYWORDS)

    # ------------------------------------------------------------------
    # Step 1: Find the shortage page or PDF links
    # ------------------------------------------------------------------

    def _find_shortage_page(self) -> tuple[str, BeautifulSoup] | None:
        """Try known URLs, then crawl the main site for shortage links."""
        # Try direct URLs
        for url in self.SHORTAGE_URLS:
            print(f"    Trying: {url}")
            resp = self._fetch_page(url)
            if resp and self._page_has_shortage_content(
                BeautifulSoup(resp.text, "lxml")
            ):
                print(f"    Found shortage page: {url}")
                return url, BeautifulSoup(resp.text, "lxml")

        # Search main pages for links
        print("    Direct URLs exhausted, searching main site...")
        for search_url in self.SEARCH_URLS:
            resp = self._fetch_page(search_url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href_lower = a["href"].lower()
                link_text = a.get_text(strip=True).lower()
                combined = href_lower + " " + link_text
                if any(kw in combined for kw in self.SHORTAGE_KEYWORDS):
                    full_url = urljoin(search_url, a["href"])
                    print(f"    Found shortage link: {full_url}")
                    resp2 = self._fetch_page(full_url)
                    if resp2:
                        return full_url, BeautifulSoup(resp2.text, "lxml")

        return None

    # ------------------------------------------------------------------
    # Step 2a: HTML table parsing
    # ------------------------------------------------------------------

    def _map_column_index(self, headers: list[str]) -> dict[str, int]:
        """Map Spanish column header text to standard field names and indices."""
        col_map: dict[str, int] = {}
        for i, h in enumerate(headers):
            h_clean = re.sub(r"\s+", " ", h.strip().lower())
            for hint, field in self.SPANISH_COLUMN_HINTS.items():
                if hint in h_clean:
                    # First match wins for each standard field
                    if field not in col_map:
                        col_map[field] = i
                    break
        return col_map

    def _parse_html_tables(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Extract shortage records from HTML tables on the page."""
        records: list[dict] = []
        tables = soup.find_all("table")
        if not tables:
            return records

        for table_idx, table in enumerate(tables):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Extract header row
            header_cells = rows[0].find_all(["th", "td"])
            headers = [c.get_text(strip=True).lower() for c in header_cells]
            if not headers:
                continue

            col_map = self._map_column_index(headers)

            # If we cannot identify a medicine name column, guess the first
            # non-numeric text column
            if "medicine_name" not in col_map and len(headers) >= 2:
                for i, h in enumerate(headers):
                    if i not in col_map.values() and not re.match(
                        r"^(n[°o\.]?|#|no\.?|item)$", h.strip()
                    ):
                        col_map["medicine_name"] = i
                        break

            if "medicine_name" not in col_map:
                continue

            print(
                f"    Table {table_idx + 1}: {len(rows) - 1} data rows, "
                f"mapped columns: {list(col_map.keys())}"
            )

            for row in rows[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if not cells:
                    continue

                med_idx = col_map["medicine_name"]
                if med_idx >= len(cells):
                    continue
                medicine = cells[med_idx]
                if not medicine or medicine.lower() in ("", "-", "nan"):
                    continue

                def _get(field: str) -> str:
                    idx = col_map.get(field)
                    if idx is not None and idx < len(cells):
                        v = cells[idx]
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
                    "package_size": "",
                    "dosage_form": _get("dosage_form"),
                    "manufacturer": _get("manufacturer"),
                    "registration_no": _get("registration_no"),
                    "reason": _get("reason"),
                    "status": status,
                    "shortage_start": self._parse_date(_get("shortage_start")),
                    "estimated_end": self._parse_date(_get("estimated_end")),
                    "source_url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    # ------------------------------------------------------------------
    # Step 2b: PDF link discovery and parsing
    # ------------------------------------------------------------------

    def _find_pdf_links(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        """Find PDF download links that look like shortage alerts."""
        pdf_links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            link_text = a.get_text(strip=True).lower()
            combined = href_lower + " " + link_text

            is_pdf = href_lower.endswith(".pdf") or "pdf" in href_lower
            is_shortage = any(kw in combined for kw in self.SHORTAGE_KEYWORDS)
            is_alert = "alerta" in combined

            if is_pdf and (is_shortage or is_alert):
                full_url = urljoin(page_url, href)
                if full_url not in pdf_links:
                    pdf_links.append(full_url)

        return pdf_links

    def _download_pdf(self, url: str) -> str | None:
        """Download a PDF to a temp file. Returns path or None on failure."""
        resp = self._fetch_page(url, timeout=60)
        if not resp:
            return None

        # Verify it is actually a PDF
        content_type = resp.headers.get("Content-Type", "").lower()
        if resp.content[:5] != b"%PDF-" and "pdf" not in content_type:
            print(f"    Not a PDF: {url}")
            return None

        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(resp.content)
        tmp.close()
        print(f"    Downloaded PDF ({len(resp.content) / 1024:.0f} KB): {url}")
        return tmp.name

    def _parse_pdf_tables(self, pdf_path: str, source_url: str) -> list[dict]:
        """Extract shortage records from a PDF using tabula-py."""
        try:
            import tabula
        except ImportError:
            print("    WARNING: tabula-py not installed, skipping PDF table extraction")
            return []

        try:
            dfs = tabula.read_pdf(pdf_path, pages="all", lattice=True)
        except Exception:
            try:
                dfs = tabula.read_pdf(pdf_path, pages="all", stream=True)
            except Exception as e:
                print(f"    tabula could not parse PDF: {e}")
                return []

        if not dfs:
            print("    No tables found in PDF")
            return []

        print(f"    PDF contains {len(dfs)} table(s)")

        records: list[dict] = []
        for df in dfs:
            if df.empty or len(df.columns) < 2:
                continue

            # Try to detect column mapping from the first row or column names
            col_map = self._map_column_index(
                [str(c).lower() for c in df.columns]
            )

            # If column names are generic (0, 1, ...) try the first data row
            if "medicine_name" not in col_map and len(df) > 0:
                first_row_vals = [
                    str(v).lower() for v in df.iloc[0].values if pd.notna(v)
                ]
                alt_map = self._map_column_index(first_row_vals)
                if "medicine_name" in alt_map:
                    col_map = alt_map
                    df = df.iloc[1:].reset_index(drop=True)

            # Fallback: assign medicine_name to first non-trivial column
            if "medicine_name" not in col_map:
                for i, c in enumerate(df.columns):
                    col_str = str(c).lower()
                    if not re.match(r"^(unnamed|n[°o\.]?|#|no\.?).*$", col_str):
                        col_map["medicine_name"] = i
                        break
                else:
                    if len(df.columns) >= 2:
                        col_map["medicine_name"] = 1  # skip serial-no column
                    else:
                        col_map["medicine_name"] = 0

            # Build reverse map: index -> field
            idx_to_field: dict[int, str] = {}
            for field, idx in col_map.items():
                idx_to_field[idx] = field

            for _, row in df.iterrows():
                vals = row.tolist()
                med_idx = col_map.get("medicine_name", 0)
                if med_idx >= len(vals):
                    continue
                medicine = str(vals[med_idx]).strip() if pd.notna(vals[med_idx]) else ""
                if not medicine or medicine.lower() in ("nan", "", "-"):
                    continue

                # Skip obvious header/title rows repeated in the PDF
                if any(
                    kw in medicine.lower()
                    for kw in ("producto", "medicamento", "denominaci", "nombre")
                ):
                    continue

                def _pdf_get(field: str) -> str:
                    idx = col_map.get(field)
                    if idx is not None and idx < len(vals) and pd.notna(vals[idx]):
                        v = str(vals[idx]).strip()
                        return v if v.lower() not in ("nan", "-", "") else ""
                    return ""

                status_raw = _pdf_get("status")
                status = status_raw if status_raw else "shortage"

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": medicine.replace("\r", " "),
                    "active_substance": _pdf_get("active_substance").replace("\r", " "),
                    "strength": _pdf_get("strength"),
                    "package_size": "",
                    "dosage_form": _pdf_get("dosage_form"),
                    "manufacturer": _pdf_get("manufacturer").replace("\r", " "),
                    "registration_no": _pdf_get("registration_no"),
                    "reason": _pdf_get("reason").replace("\r", " "),
                    "status": status,
                    "shortage_start": self._parse_date(_pdf_get("shortage_start")),
                    "estimated_end": self._parse_date(_pdf_get("estimated_end")),
                    "source_url": source_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    def _parse_pdf_text_fallback(self, pdf_path: str, source_url: str) -> list[dict]:
        """Fallback: extract medicine names from raw PDF text using pdfplumber."""
        try:
            import pdfplumber
        except ImportError:
            print("    WARNING: pdfplumber not installed, skipping text extraction")
            return []

        records: list[dict] = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    # Look for lines that appear to be medicine entries:
                    # typically start with a product name in uppercase or
                    # contain a registro sanitario pattern (e.g. "RSE-xxxx")
                    for line in text.split("\n"):
                        line = line.strip()
                        if not line or len(line) < 5:
                            continue
                        # Skip headers / footers
                        if any(
                            kw in line.lower()
                            for kw in (
                                "digemid", "ministerio", "pagina",
                                "p\u00e1gina", "fecha de emisi",
                                "alerta de desabastecimiento",
                                "direcci\u00f3n general",
                            )
                        ):
                            continue

                        # Heuristic: line with a registro sanitario
                        reg_match = re.search(
                            r"(RS[A-Z]?[-\s]?\d{4,})", line, re.I
                        )
                        if reg_match or (
                            len(line) > 10
                            and line[0].isupper()
                            and not line.startswith("Art")
                        ):
                            medicine = line
                            reg_no = reg_match.group(1) if reg_match else ""

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
                                "registration_no": reg_no,
                                "reason": "",
                                "status": "shortage",
                                "shortage_start": None,
                                "estimated_end": None,
                                "source_url": source_url,
                                "scraped_at": datetime.now().isoformat(),
                            })
        except Exception as e:
            print(f"    pdfplumber extraction failed: {e}")

        return records

    # ------------------------------------------------------------------
    # Step 2c: List / article-based parsing fallback
    # ------------------------------------------------------------------

    def _parse_list_items(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Fallback: extract records from article/div/list-based layouts."""
        records: list[dict] = []

        containers = soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(
                r"(alerta|desabastecimiento|shortage|medicamento|item|entry|post|card)",
                re.I,
            ),
        )

        for container in containers:
            text = container.get_text(" ", strip=True)
            if len(text) < 10:
                continue

            heading = container.find(["h2", "h3", "h4", "h5", "strong", "b"])
            medicine = heading.get_text(strip=True) if heading else ""
            if not medicine:
                continue

            # Try to find a date in the container
            date_match = re.search(r"\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}", text)
            shortage_start = self._parse_date(date_match.group(0)) if date_match else None

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
                "registration_no": "",
                "reason": "",
                "status": "shortage",
                "shortage_start": shortage_start,
                "estimated_end": None,
                "source_url": page_url,
                "scraped_at": datetime.now().isoformat(),
            })

        return records

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        empty_columns = [
            "country_code", "country_name", "source",
            "medicine_name", "active_substance", "strength",
            "package_size", "dosage_form", "manufacturer",
            "registration_no", "reason", "status",
            "shortage_start", "estimated_end",
            "source_url", "scraped_at",
        ]

        result = self._find_shortage_page()
        if not result:
            print("  WARNING: Could not find DIGEMID shortage page.")
            print("  DIGEMID may have changed their URL structure.")
            print("  Returning empty DataFrame. Manual URL verification needed.")
            return pd.DataFrame(columns=empty_columns)

        page_url, soup = result

        # ------ Strategy 1: HTML tables ------
        all_records = self._parse_html_tables(soup, page_url)
        if all_records:
            print(f"  Found {len(all_records)} records from HTML tables")

        # ------ Strategy 2: PDF links on the page ------
        if not all_records:
            print("    No HTML table records, looking for PDF links...")
            pdf_links = self._find_pdf_links(soup, page_url)
            if pdf_links:
                print(f"    Found {len(pdf_links)} PDF link(s)")

            for pdf_url in pdf_links[:5]:  # limit to avoid excessive downloads
                pdf_path = self._download_pdf(pdf_url)
                if not pdf_path:
                    continue
                try:
                    # Try structured table extraction first
                    pdf_records = self._parse_pdf_tables(pdf_path, pdf_url)
                    if not pdf_records:
                        # Fall back to raw text extraction
                        pdf_records = self._parse_pdf_text_fallback(pdf_path, pdf_url)
                    all_records.extend(pdf_records)
                finally:
                    try:
                        os.unlink(pdf_path)
                    except OSError:
                        pass

        # ------ Strategy 3: List / div fallback ------
        if not all_records:
            print("    No table or PDF records, trying list-based parsing...")
            all_records = self._parse_list_items(soup, page_url)

        # Build DataFrame
        df = pd.DataFrame(all_records)

        if df.empty:
            print("  WARNING: No shortage records found on the DIGEMID page.")
            print("  The page structure may have changed or no shortages are currently listed.")
            return pd.DataFrame(columns=empty_columns)

        # Deduplicate on medicine_name + strength
        before = len(df)
        dedup_cols = ["medicine_name", "strength"]
        available_dedup = [c for c in dedup_cols if c in df.columns]
        if available_dedup:
            df = df.drop_duplicates(subset=available_dedup, keep="first")
        if len(df) < before:
            print(f"    Removed {before - len(df)} duplicate records")

        print(f"  Total: {len(df)} shortage records scraped")
        return df
