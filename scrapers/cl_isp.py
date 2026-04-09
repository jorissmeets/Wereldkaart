"""Scraper for Chile ISP (Instituto de Salud Publica) drug shortage data."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class ClIspScraper(BaseScraper):
    """Scraper for ISP (Chile) drug shortage / desabastecimiento notices.

    The ISP publishes drug shortage information (desabastecimiento de
    medicamentos) on their website at https://www.ispch.cl. Information
    may appear as:
    - HTML tables listing affected medicines
    - Individual notice/alert pages (resoluciones, alertas)
    - PDF documents linked from listing pages

    This scraper tries multiple known URL patterns and parses HTML tables
    and structured page content. Language: Spanish.

    Scrapeerbaarheid: 2 stars -- the ISP site changes structure frequently
    and shortage data may be spread across multiple page types.
    """

    BASE = "https://www.ispch.cl"

    # Known and candidate URLs where ISP publishes shortage data
    SHORTAGE_URLS = [
        "https://www.ispch.cl/anamed/desabastecimiento/",
        "https://www.ispch.cl/anamed/desabastecimiento-de-medicamentos/",
        "https://www.ispch.cl/desabastecimiento/",
        "https://www.ispch.cl/desabastecimiento-de-medicamentos/",
        "https://www.ispch.cl/medicamentos/desabastecimiento/",
        "https://www.ispch.cl/anamed/problemas-de-suministro/",
        "https://www.ispch.cl/problemas-de-suministro/",
        "https://www.ispch.cl/anamed/alertas/",
        "https://www.ispch.cl/notificacion-de-desabastecimiento/",
        "https://www.ispch.cl/anamed/notificacion-de-desabastecimiento/",
    ]

    # Broader pages to search for shortage links if direct URLs fail
    SEARCH_URLS = [
        "https://www.ispch.cl/anamed/",
        "https://www.ispch.cl/medicamentos/",
        "https://www.ispch.cl",
    ]

    # Spanish keywords indicating shortage-related content
    SHORTAGE_KEYWORDS_ES = [
        "desabastecimiento",
        "desabastecido",
        "falta de stock",
        "falta de suministro",
        "quiebre de stock",
        "no disponible",
        "indisponibilidad",
        "problema de suministro",
        "escasez",
        "agotado",
        "sin stock",
        "interrupcion de suministro",
        "interrupción de suministro",
        "discontinuado",
    ]

    # Spanish column header keywords for table parsing
    COL_KEYWORDS_ES = {
        "medicine_name": [
            "nombre del producto", "nombre producto", "medicamento",
            "producto", "nombre del medicamento", "nombre comercial",
            "nombre", "especialidad farmacéutica", "especialidad farmaceutica",
        ],
        "active_substance": [
            "principio activo", "sustancia activa", "dci", "inn",
            "ingrediente farmacéutico activo", "ifa", "denominación genérica",
            "denominacion generica",
        ],
        "strength": [
            "concentración", "concentracion", "dosis", "potencia",
            "forma y concentración", "forma y concentracion",
        ],
        "dosage_form": [
            "forma farmacéutica", "forma farmaceutica", "forma",
            "presentación", "presentacion",
        ],
        "status": [
            "estado", "situación", "situacion", "condición", "condicion",
            "disponibilidad",
        ],
        "shortage_start": [
            "fecha inicio", "fecha de inicio", "fecha notificación",
            "fecha notificacion", "fecha de notificación",
            "fecha de notificacion", "fecha reporte", "inicio",
            "fecha", "fecha de publicación", "fecha de publicacion",
        ],
        "estimated_end": [
            "fecha estimada", "fecha fin", "fecha de término",
            "fecha de termino", "fecha restitución", "fecha restitucion",
            "fecha estimada de resolución", "fecha estimada de resolucion",
            "fecha de disponibilidad", "plazo estimado",
        ],
        "manufacturer": [
            "laboratorio", "titular", "fabricante", "empresa",
            "titular de registro", "responsable",
        ],
        "registration_no": [
            "registro sanitario", "registro", "n° registro",
            "número de registro", "numero de registro", "reg. san.",
            "nro. registro",
        ],
        "reason": [
            "motivo", "causa", "razón", "razon", "justificación",
            "justificacion", "observaciones", "observación", "observacion",
        ],
    }

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.5",
    }

    def __init__(self):
        super().__init__(
            country_code="CL",
            country_name="Chile",
            source_name="ISP",
            base_url="https://www.ispch.cl",
        )
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    # ------------------------------------------------------------------
    # Date parsing
    # ------------------------------------------------------------------

    def _parse_date(self, val) -> str | None:
        """Parse a date string into ISO format, handling common Chilean/Spanish formats."""
        if pd.isna(val) or not val:
            return None
        val = str(val).strip()
        if not val or val in ("-", "N/A", "n/a", "S/I", "s/i", "nan", "None", ""):
            return None

        # Normalize common Spanish month abbreviations
        val_norm = val.lower()
        month_map = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
            "ene": "01", "feb": "02", "mar": "03", "abr": "04",
            "may": "05", "jun": "06", "jul": "07", "ago": "08",
            "sep": "09", "oct": "10", "nov": "11", "dic": "12",
        }

        # Try "DD de mes de YYYY" or "DD mes YYYY" pattern
        m = re.search(
            r"(\d{1,2})\s+(?:de\s+)?(\w+)\s+(?:de\s+)?(\d{4})", val_norm
        )
        if m:
            day, month_word, year = m.group(1), m.group(2), m.group(3)
            month_num = month_map.get(month_word)
            if month_num:
                try:
                    return datetime.strptime(
                        f"{day}/{month_num}/{year}", "%d/%m/%Y"
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Try "mes YYYY" pattern (month only)
        m = re.search(r"(\w+)\s+(?:de\s+)?(\d{4})", val_norm)
        if m:
            month_word, year = m.group(1), m.group(2)
            month_num = month_map.get(month_word)
            if month_num:
                try:
                    return datetime.strptime(
                        f"01/{month_num}/{year}", "%d/%m/%Y"
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Standard date formats
        for fmt in (
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%y",
            "%d/%m/%y",
            "%d.%m.%Y",
            "%d %m %Y",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    # ------------------------------------------------------------------
    # Network helpers
    # ------------------------------------------------------------------

    def _fetch_page(self, url: str) -> requests.Response | None:
        """Fetch a page with error handling. Returns None on failure."""
        try:
            resp = self.session.get(url, timeout=30, allow_redirects=True)
            if resp.status_code == 200:
                return resp
            print(f"    HTTP {resp.status_code} for {url}")
        except requests.RequestException as e:
            print(f"    Request failed for {url}: {e}")
        return None

    # ------------------------------------------------------------------
    # Page discovery
    # ------------------------------------------------------------------

    def _is_shortage_content(self, text: str) -> bool:
        """Check whether a text block contains shortage-related Spanish keywords."""
        text_lower = text.lower()
        return any(kw in text_lower for kw in self.SHORTAGE_KEYWORDS_ES)

    def _find_shortage_pages(self) -> list[tuple[str, BeautifulSoup]]:
        """Try known shortage URLs, then search broader ISP pages for links.

        Returns a list of (url, soup) tuples for pages that contain
        shortage-related content.
        """
        found_pages: list[tuple[str, BeautifulSoup]] = []
        visited: set[str] = set()

        # Step 1: Try direct shortage URLs
        for url in self.SHORTAGE_URLS:
            if url in visited:
                continue
            visited.add(url)
            print(f"    Trying: {url}")
            resp = self._fetch_page(url)
            if resp:
                soup = BeautifulSoup(resp.text, "lxml")
                text = soup.get_text(strip=True).lower()
                if self._is_shortage_content(text):
                    print(f"    Found shortage content: {url}")
                    found_pages.append((url, soup))

        # Step 2: Search broader pages for shortage-related links
        if not found_pages:
            print("    Direct URLs did not yield results, searching broader ISP pages...")
            for search_url in self.SEARCH_URLS:
                resp = self._fetch_page(search_url)
                if not resp:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"].lower()
                    link_text = a.get_text(strip=True).lower()
                    if any(kw in href or kw in link_text for kw in self.SHORTAGE_KEYWORDS_ES):
                        full_url = urljoin(search_url, a["href"])
                        if full_url in visited:
                            continue
                        visited.add(full_url)
                        print(f"    Found shortage link: {full_url}")
                        resp2 = self._fetch_page(full_url)
                        if resp2:
                            found_pages.append(
                                (full_url, BeautifulSoup(resp2.text, "lxml"))
                            )
                time.sleep(0.5)

        return found_pages

    # ------------------------------------------------------------------
    # HTML table parsing
    # ------------------------------------------------------------------

    def _map_column(self, header_text: str) -> str | None:
        """Map a Spanish column header to a standard field name."""
        h = header_text.strip().lower()
        h = re.sub(r"\s+", " ", h)
        for field, keywords in self.COL_KEYWORDS_ES.items():
            for kw in keywords:
                if kw in h:
                    return field
        return None

    def _parse_html_tables(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Extract shortage records from HTML tables on a page."""
        records: list[dict] = []
        tables = soup.find_all("table")
        if not tables:
            return records

        for table_idx, table in enumerate(tables):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Build column mapping from header row
            header_row = rows[0]
            headers = [
                cell.get_text(strip=True)
                for cell in header_row.find_all(["th", "td"])
            ]
            col_map: dict[int, str] = {}
            for i, h in enumerate(headers):
                field = self._map_column(h)
                if field:
                    col_map[i] = field

            # If no medicine_name column found, use the first non-mapped text column
            if "medicine_name" not in col_map.values() and len(headers) >= 2:
                for i, h in enumerate(headers):
                    if i not in col_map:
                        col_map[i] = "medicine_name"
                        break

            if "medicine_name" not in col_map.values():
                continue

            mapped_fields = list(col_map.values())
            print(
                f"    Table {table_idx + 1}: {len(rows) - 1} data rows, "
                f"columns mapped: {mapped_fields}"
            )

            # Parse data rows
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                cell_texts = [c.get_text(strip=True) for c in cells]
                if not cell_texts or all(not c for c in cell_texts):
                    continue

                raw: dict[str, str] = {}
                for idx, field in col_map.items():
                    if idx < len(cell_texts):
                        val = cell_texts[idx]
                        if val.lower() not in ("", "-", "nan", "n/a", "s/i"):
                            raw[field] = val

                medicine = raw.get("medicine_name", "")
                if not medicine:
                    continue

                # Determine status from raw value
                status_raw = raw.get("status", "").lower()
                status = self._interpret_status(status_raw)

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": medicine,
                    "active_substance": raw.get("active_substance", ""),
                    "strength": raw.get("strength", ""),
                    "package_size": "",
                    "dosage_form": raw.get("dosage_form", ""),
                    "manufacturer": raw.get("manufacturer", ""),
                    "registration_no": raw.get("registration_no", ""),
                    "reason": raw.get("reason", ""),
                    "status": status,
                    "shortage_start": self._parse_date(raw.get("shortage_start")),
                    "estimated_end": self._parse_date(raw.get("estimated_end")),
                    "source_url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    # ------------------------------------------------------------------
    # List / notice parsing (fallback for non-tabular pages)
    # ------------------------------------------------------------------

    def _parse_notice_listings(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Extract shortage records from notice/alert-style listings.

        ISP may publish individual notices as articles, divs, or list items
        rather than a consolidated table.
        """
        records: list[dict] = []

        # Look for article/post/entry containers
        containers = soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(
                r"(entry|post|notice|alerta|aviso|item|card|noticia)", re.I
            ),
        )

        # If no class-based containers, try semantic elements
        if not containers:
            containers = soup.find_all("article")

        for container in containers:
            text = container.get_text(" ", strip=True)
            if len(text) < 15:
                continue

            # Only process containers with shortage-related content
            if not self._is_shortage_content(text):
                continue

            # Try to extract a medicine name from headings or bold text
            heading = container.find(["h2", "h3", "h4", "h5", "strong", "b"])
            medicine = heading.get_text(strip=True) if heading else ""

            # Try to extract a date
            date_el = container.find("time") or container.find(
                class_=re.compile(r"(date|fecha|time)", re.I)
            )
            notice_date = None
            if date_el:
                notice_date = self._parse_date(
                    date_el.get("datetime", "") or date_el.get_text(strip=True)
                )
            if not notice_date:
                # Try to find a date pattern in the text
                date_match = re.search(
                    r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", text
                )
                if date_match:
                    notice_date = self._parse_date(date_match.group(0))

            # Extract link to detail page if present
            detail_link = ""
            a_tag = container.find("a", href=True)
            if a_tag:
                detail_link = urljoin(page_url, a_tag["href"])

            if medicine:
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
                    "status": "desabastecimiento",
                    "shortage_start": notice_date,
                    "estimated_end": None,
                    "source_url": detail_link or page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    # ------------------------------------------------------------------
    # PDF link collection
    # ------------------------------------------------------------------

    def _collect_pdf_links(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Collect PDF links from a page as fallback records.

        When ISP publishes shortage data as downloadable PDFs rather than
        inline HTML, we record the PDF references so they can be processed
        separately (PDF parsing is not attempted here to avoid heavy
        dependencies).
        """
        records: list[dict] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            link_text = a.get_text(strip=True)
            if not self._is_shortage_content(link_text + " " + href):
                continue
            full_url = urljoin(page_url, href)

            # Try to parse a date from the link text or surrounding context
            notice_date = None
            parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
            date_match = re.search(
                r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", parent_text
            )
            if date_match:
                notice_date = self._parse_date(date_match.group(0))

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": link_text or "PDF notice",
                "active_substance": "",
                "strength": "",
                "package_size": "",
                "dosage_form": "",
                "manufacturer": "",
                "registration_no": "",
                "reason": "",
                "status": "desabastecimiento",
                "shortage_start": notice_date,
                "estimated_end": None,
                "source_url": full_url,
                "scraped_at": datetime.now().isoformat(),
            })

        return records

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def _find_next_pages(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Detect pagination and return additional page URLs."""
        next_pages: list[str] = []

        # Standard pagination containers
        pager = soup.find(
            ["nav", "div", "ul"],
            class_=re.compile(r"(pag|page-nav|wp-pagenavi|nav-links)", re.I),
        )
        if pager:
            for a in pager.find_all("a", href=True):
                href = urljoin(base_url, a["href"])
                if href not in next_pages and href != base_url:
                    next_pages.append(href)

        # "Siguiente" (next) links
        for a in soup.find_all("a", href=True):
            link_text = a.get_text(strip=True).lower()
            classes = " ".join(a.get("class", [])).lower()
            if "siguiente" in link_text or "next" in link_text or "next" in classes:
                href = urljoin(base_url, a["href"])
                if href not in next_pages:
                    next_pages.append(href)

        return next_pages[:10]  # Limit to avoid runaway crawling

    # ------------------------------------------------------------------
    # Status interpretation
    # ------------------------------------------------------------------

    @staticmethod
    def _interpret_status(raw_status: str) -> str:
        """Map Spanish status text to a standardized status value."""
        s = raw_status.strip().lower()
        if not s:
            return "desabastecimiento"
        # Check negative / partial matches BEFORE positive "disponible"
        if any(kw in s for kw in ("desabastecimiento", "desabastecido", "no disponible", "agotado", "quiebre")):
            return "desabastecimiento"
        if any(kw in s for kw in ("parcial", "limitado", "reducido")):
            return "limited_supply"
        if any(kw in s for kw in ("discontinuado", "retirado", "suspendido")):
            return "discontinued"
        if any(kw in s for kw in ("resuelto", "disponible", "normalizado", "restituido", "restablecido")):
            return "resolved"
        return "desabastecimiento"

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Step 1: Find pages with shortage content
        pages = self._find_shortage_pages()
        print(f"  Found {len(pages)} page(s) with shortage content")

        all_records: list[dict] = []

        # Step 2: Parse each page for structured data
        for page_url, soup in pages:
            # Try HTML table parsing first
            table_records = self._parse_html_tables(soup, page_url)
            if table_records:
                print(f"    {page_url}: {len(table_records)} records from tables")
                all_records.extend(table_records)
                continue

            # Try notice/listing parsing
            notice_records = self._parse_notice_listings(soup, page_url)
            if notice_records:
                print(f"    {page_url}: {len(notice_records)} records from notices")
                all_records.extend(notice_records)
                continue

            # Collect PDF references as fallback
            pdf_records = self._collect_pdf_links(soup, page_url)
            if pdf_records:
                print(f"    {page_url}: {len(pdf_records)} PDF references collected")
                all_records.extend(pdf_records)

        # Step 3: Follow pagination on the first found page
        if pages:
            first_url, first_soup = pages[0]
            next_pages = self._find_next_pages(first_soup, first_url)
            if next_pages:
                print(f"  Following {len(next_pages)} additional page(s)...")
            for extra_url in next_pages:
                resp = self._fetch_page(extra_url)
                if not resp:
                    continue
                extra_soup = BeautifulSoup(resp.text, "lxml")
                extra_records = self._parse_html_tables(extra_soup, extra_url)
                if not extra_records:
                    extra_records = self._parse_notice_listings(extra_soup, extra_url)
                if not extra_records:
                    extra_records = self._collect_pdf_links(extra_soup, extra_url)
                all_records.extend(extra_records)
                time.sleep(0.5)

        # Build DataFrame
        if not all_records:
            print("  WARNING: No shortage records found on the ISP website.")
            print("  The ISP site structure may have changed or no shortages are currently listed.")
            print("  Manual verification at https://www.ispch.cl is recommended.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "dosage_form", "manufacturer",
                "registration_no", "reason", "status",
                "shortage_start", "estimated_end",
                "source_url", "scraped_at",
            ])

        df = pd.DataFrame(all_records)

        # Deduplicate based on medicine_name + source_url
        before = len(df)
        dedup_cols = ["medicine_name", "source_url"]
        available_dedup = [c for c in dedup_cols if c in df.columns]
        if available_dedup:
            df = df.drop_duplicates(subset=available_dedup, keep="first")
        if len(df) < before:
            print(f"  Removed {before - len(df)} duplicate records")

        print(f"  Total: {len(df)} shortage records scraped")
        return df
