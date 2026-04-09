"""Scraper for Mexico COFEPRIS (Comision Federal para la Proteccion contra Riesgos Sanitarios) drug shortage and alert data."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class MxCofeprisScraper(BaseScraper):
    """Scraper for COFEPRIS (Mexico) drug shortage and sanitary alert data.

    COFEPRIS publishes drug-related alerts and communications on the gob.mx
    platform. This includes:
    - Alertas sanitarias (sanitary alerts) about drug quality, shortages,
      counterfeits, and recalls
    - Comunicados (communications) about drug availability issues

    The gob.mx platform uses a structured URL pattern for listing alerts
    and provides both HTML listing pages and individual alert detail pages.

    Scrapeerbaarheid: 2 stars — content is spread across multiple alert pages,
    no single structured table of shortages. Requires crawling alert listings
    and parsing individual alert pages.
    """

    BASE = "https://www.gob.mx"

    # Primary URLs for COFEPRIS drug shortage / alert data
    # gob.mx uses a document search API and structured listing pages
    ALERT_URLS = [
        # Sanitary alerts page — main source
        "https://www.gob.mx/cofepris/acciones-y-programas/alertas-sanitarias-702",
        # Alternative alert listings
        "https://www.gob.mx/cofepris/documentos/alertas-sanitarias",
        # Communications that may include shortage notices
        "https://www.gob.mx/cofepris/prensa",
    ]

    # gob.mx document search API endpoint (used for structured queries)
    SEARCH_API = "https://www.gob.mx/busqueda"

    # Keywords for identifying drug shortage / unavailability alerts (Spanish)
    SHORTAGE_KEYWORDS = [
        "desabasto",          # shortage
        "escasez",            # scarcity
        "falta de",           # lack of
        "no disponible",      # not available
        "disponibilidad",     # availability
        "abasto",             # supply
        "suspensión",         # suspension
        "suspension",         # suspension (no accent)
        "retiro",             # withdrawal/recall
        "alerta sanitaria",   # sanitary alert
        "riesgo sanitario",   # sanitary risk
        "medicamento",        # medicine
        "fármaco",            # drug
        "farmaco",            # drug (no accent)
    ]

    # Spanish month names for date parsing
    SPANISH_MONTHS = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
    }

    def __init__(self):
        super().__init__(
            country_code="MX",
            country_name="Mexico",
            source_name="COFEPRIS",
            base_url="https://www.gob.mx/cofepris",
        )
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _parse_spanish_date(self, text: str) -> str | None:
        """Parse a Spanish date string into ISO format.

        Handles formats like:
        - '15 de enero de 2025'
        - '15/01/2025'
        - '2025-01-15'
        - 'enero 2025'
        - '15-ene-2025'
        """
        if not text:
            return None
        text = text.strip().lower()
        if not text or text in ("-", "n/a", "na", "nan", "none", "n/d", "s/f"):
            return None

        # Pattern: '15 de enero de 2025' or '15 de enero, 2025'
        m = re.search(
            r"(\d{1,2})\s+de\s+(\w+)\s+(?:de\s+|,\s*)(\d{4})",
            text,
        )
        if m:
            day, month_name, year = int(m.group(1)), m.group(2), int(m.group(3))
            month = self.SPANISH_MONTHS.get(month_name)
            if month:
                try:
                    return datetime(year, month, day).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Pattern: 'enero de 2025' or 'enero 2025' (month-year only)
        m = re.search(r"(\w+)\s+(?:de\s+)?(\d{4})", text)
        if m:
            month_name, year = m.group(1), int(m.group(2))
            month = self.SPANISH_MONTHS.get(month_name)
            if month:
                try:
                    return datetime(year, month, 1).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Standard numeric formats
        for fmt in (
            "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d",
            "%d/%m/%y", "%d-%m-%y",
            "%d de %B de %Y",
        ):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Short month abbreviations: '15-ene-2025'
        m = re.search(r"(\d{1,2})[/-](\w{3,4})[/-](\d{4})", text)
        if m:
            day_str, month_abbr, year_str = m.group(1), m.group(2), m.group(3)
            abbr_map = {
                "ene": 1, "feb": 2, "mar": 3, "abr": 4,
                "may": 5, "jun": 6, "jul": 7, "ago": 8,
                "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dic": 12,
            }
            month = abbr_map.get(month_abbr)
            if month:
                try:
                    return datetime(int(year_str), month, int(day_str)).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return None

    def _fetch_page(self, url: str) -> requests.Response | None:
        """Fetch a page with error handling. Returns None on failure."""
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp
            print(f"    HTTP {resp.status_code} for {url}")
        except requests.RequestException as e:
            print(f"    Request failed for {url}: {e}")
        return None

    def _is_drug_related(self, text: str) -> bool:
        """Check if text is related to drug shortages or medicine alerts."""
        text_lower = text.lower()
        # Must mention medicines/drugs AND a shortage/alert keyword
        has_medicine = any(kw in text_lower for kw in (
            "medicamento", "fármaco", "farmaco", "medicina",
            "sustancia activa", "principio activo", "tableta",
            "cápsula", "capsula", "inyectable", "solución",
            "solucion", "jarabe", "suspensión",
        ))
        has_shortage = any(kw in text_lower for kw in (
            "desabasto", "escasez", "falta", "no disponible",
            "disponibilidad", "retiro", "suspensión", "suspension",
            "alerta", "riesgo",
        ))
        return has_medicine or has_shortage

    def _extract_medicine_info(self, text: str) -> dict:
        """Try to extract medicine name, active substance, and strength from alert text.

        COFEPRIS alerts often mention medicines in formats like:
        - 'PARACETAMOL tabletas 500 mg'
        - 'Metformina 850mg tabletas'
        - 'Principio activo: omeprazol'
        """
        info = {
            "medicine_name": "",
            "active_substance": "",
            "strength": "",
        }

        # Try to find 'principio activo' or 'sustancia activa' pattern
        m = re.search(
            r"(?:principio\s+activo|sustancia\s+activa)[:\s]+([^\n,;]+)",
            text, re.IGNORECASE,
        )
        if m:
            info["active_substance"] = m.group(1).strip()

        # Try to find 'denominación distintiva' (brand name)
        m = re.search(
            r"(?:denominaci[oó]n\s+distintiva|nombre\s+comercial|marca)[:\s]+([^\n,;]+)",
            text, re.IGNORECASE,
        )
        if m:
            info["medicine_name"] = m.group(1).strip()

        # Extract strength patterns: '500 mg', '850mg/5ml', '10 mg/ml'
        m = re.search(
            r"(\d+(?:\.\d+)?\s*(?:mg|mcg|g|ml|ui|%|µg)(?:\s*/\s*\d*\s*(?:mg|mcg|g|ml|ui|%|µg))?)",
            text, re.IGNORECASE,
        )
        if m:
            info["strength"] = m.group(1).strip()

        return info

    def _scrape_alert_listing(self, url: str) -> list[dict]:
        """Scrape an alert listing page for individual alert entries.

        gob.mx listing pages typically have article cards with title, date,
        and link to the full alert.
        """
        records = []
        print(f"    Fetching listing: {url}")
        resp = self._fetch_page(url)
        if not resp:
            return records

        soup = BeautifulSoup(resp.text, "lxml")

        # gob.mx uses article/div containers for listing items
        # Look for common gob.mx listing patterns
        articles = soup.find_all(["article", "div"], class_=re.compile(
            r"(article|post|prensa|documento|list-item|media|card)", re.I
        ))

        if not articles:
            # Fallback: look for link lists within main content area
            main_content = soup.find(["main", "div"], class_=re.compile(
                r"(content|main|body|articles|documentos)", re.I
            ))
            if main_content:
                articles = main_content.find_all("a", href=True)

        # Also try finding tables on the page
        table_records = self._parse_html_tables(soup, url)
        if table_records:
            records.extend(table_records)

        seen_urls = set()
        alert_links = []

        for article in articles:
            # Find the link and title
            if article.name == "a":
                link = article
            else:
                link = article.find("a", href=True)

            if not link:
                continue

            href = link.get("href", "")
            if not href:
                continue

            # Build full URL
            if href.startswith("/"):
                full_url = self.BASE + href
            elif href.startswith("http"):
                full_url = href
            else:
                full_url = urljoin(url, href)

            # Skip non-COFEPRIS links and already-seen URLs
            if "cofepris" not in full_url.lower() and "gob.mx" not in full_url.lower():
                continue
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get title text
            title = link.get_text(strip=True)
            if not title:
                heading = article.find(["h2", "h3", "h4", "h5"])
                if heading:
                    title = heading.get_text(strip=True)

            if not title:
                continue

            # Filter for drug-related alerts
            article_text = article.get_text(" ", strip=True) if article.name != "a" else title
            if not self._is_drug_related(article_text):
                continue

            # Extract date from the listing entry
            date_text = ""
            date_el = article.find(["time", "span", "p"], class_=re.compile(
                r"(date|fecha|time|published)", re.I
            ))
            if date_el:
                date_text = date_el.get_text(strip=True)
            else:
                # Try to find date in text
                m = re.search(
                    r"(\d{1,2}\s+de\s+\w+\s+(?:de\s+)?\d{4}|\d{1,2}/\d{1,2}/\d{4})",
                    article_text,
                )
                if m:
                    date_text = m.group(1)

            alert_links.append({
                "url": full_url,
                "title": title,
                "date_text": date_text,
                "article_text": article_text,
            })

        print(f"    Found {len(alert_links)} drug-related alert links")

        # Process individual alert detail pages (limit to avoid excessive requests)
        max_detail_pages = 30
        for i, alert in enumerate(alert_links[:max_detail_pages]):
            detail_record = self._scrape_alert_detail(alert)
            if detail_record:
                records.append(detail_record)

        return records

    def _scrape_alert_detail(self, alert: dict) -> dict | None:
        """Scrape an individual alert detail page for medicine information."""
        url = alert["url"]

        # First, create a record from the listing-level data
        title = alert["title"]
        date_text = alert.get("date_text", "")
        article_text = alert.get("article_text", "")

        # Try to fetch the detail page for richer data
        detail_text = ""
        resp = self._fetch_page(url)
        if resp:
            soup = BeautifulSoup(resp.text, "lxml")

            # Extract main article content
            content_div = soup.find(["article", "div"], class_=re.compile(
                r"(article-body|content-body|article-content|entry-content"
                r"|field-item|post-content|main-content)", re.I
            ))
            if content_div:
                detail_text = content_div.get_text(" ", strip=True)
            else:
                # Fallback to main tag
                main = soup.find("main")
                if main:
                    detail_text = main.get_text(" ", strip=True)

            # Try to find a more precise date on the detail page
            if not date_text:
                date_el = soup.find(["time", "span", "p"], class_=re.compile(
                    r"(date|fecha|time|published)", re.I
                ))
                if date_el:
                    date_text = date_el.get_text(strip=True)

                # Check <time> element datetime attribute
                time_el = soup.find("time", attrs={"datetime": True})
                if time_el:
                    date_text = time_el.get("datetime", "")

        # Combine text for extraction
        full_text = f"{title} {detail_text}" if detail_text else f"{title} {article_text}"

        # Extract medicine information
        med_info = self._extract_medicine_info(full_text)

        # Use title as medicine_name if no specific name found
        medicine_name = med_info["medicine_name"] if med_info["medicine_name"] else title

        # Determine status from the text
        status = self._classify_status(full_text)

        # Parse date
        parsed_date = self._parse_spanish_date(date_text)

        record = {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine_name,
            "active_substance": med_info["active_substance"],
            "strength": med_info["strength"],
            "package_size": "",
            "status": status,
            "shortage_start": parsed_date,
            "estimated_end": None,
            "alert_title": title,
            "source_url": url,
            "scraped_at": datetime.now().isoformat(),
        }

        return record

    def _classify_status(self, text: str) -> str:
        """Classify the alert status based on Spanish keywords in the text."""
        text_lower = text.lower()

        if any(kw in text_lower for kw in ("desabasto", "escasez", "falta de")):
            return "shortage"
        if any(kw in text_lower for kw in ("no disponible", "indisponible")):
            return "unavailable"
        if "retiro" in text_lower or "recall" in text_lower:
            return "recall"
        if any(kw in text_lower for kw in ("suspensión", "suspension", "suspendido")):
            return "suspended"
        if any(kw in text_lower for kw in ("falsificad", "ilegal", "irregular")):
            return "falsified"
        if any(kw in text_lower for kw in ("resuelto", "restablecido", "normalizado")):
            return "resolved"
        if "alerta" in text_lower:
            return "alert"

        return "alert"

    def _parse_html_tables(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Extract shortage records from HTML tables found on a page.

        Some COFEPRIS pages contain tables listing affected medicines.
        """
        records = []
        tables = soup.find_all("table")
        if not tables:
            return records

        for table_idx, table in enumerate(tables):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Extract headers
            header_row = rows[0]
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

            if not headers:
                continue

            # Map columns to standard fields (Spanish headers)
            col_map = {}
            for i, h in enumerate(headers):
                h_clean = re.sub(r"\s+", " ", h).strip()
                if any(kw in h_clean for kw in (
                    "nombre", "medicamento", "producto", "denominación",
                    "denominacion", "marca",
                )):
                    if "activ" in h_clean or "principio" in h_clean:
                        col_map["active_substance"] = i
                    else:
                        col_map["medicine_name"] = i
                elif any(kw in h_clean for kw in (
                    "principio activo", "sustancia activa", "sustancia", "activo",
                )):
                    col_map["active_substance"] = i
                elif any(kw in h_clean for kw in (
                    "concentración", "concentracion", "dosis", "potencia",
                    "strength", "mg",
                )):
                    col_map["strength"] = i
                elif any(kw in h_clean for kw in (
                    "estado", "estatus", "status", "situación", "situacion",
                )):
                    col_map["status"] = i
                elif any(kw in h_clean for kw in (
                    "fecha", "date", "inicio", "notificación", "notificacion",
                )):
                    if "fin" in h_clean or "estimada" in h_clean or "resolución" in h_clean:
                        col_map["estimated_end"] = i
                    else:
                        col_map["shortage_start"] = i
                elif any(kw in h_clean for kw in (
                    "laboratorio", "fabricante", "titular", "manufacturer",
                )):
                    col_map["manufacturer"] = i
                elif any(kw in h_clean for kw in (
                    "lote", "batch", "número de lote",
                )):
                    col_map["batch_no"] = i
                elif any(kw in h_clean for kw in (
                    "registro", "reg. san", "registro sanitario",
                )):
                    col_map["registration_no"] = i
                elif any(kw in h_clean for kw in (
                    "forma", "presentación", "presentacion",
                )):
                    col_map["dosage_form"] = i
                elif any(kw in h_clean for kw in (
                    "motivo", "razón", "razon", "causa",
                )):
                    col_map["reason"] = i

            # Need at least a medicine name column to be useful
            if "medicine_name" not in col_map and len(headers) >= 2:
                # Use the first non-number text column as medicine name
                for i, h in enumerate(headers):
                    if i not in col_map.values() and "no" not in h and "#" not in h and "num" not in h:
                        col_map["medicine_name"] = i
                        break

            if "medicine_name" not in col_map:
                continue

            print(f"    Table {table_idx + 1}: {len(rows) - 1} data rows, columns: {list(col_map.keys())}")

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue

                cell_texts = [c.get_text(strip=True) for c in cells]

                if len(cell_texts) <= col_map.get("medicine_name", 0):
                    continue

                def _get(field: str) -> str:
                    idx = col_map.get(field)
                    if idx is not None and idx < len(cell_texts):
                        v = cell_texts[idx]
                        return v if v.lower() not in ("", "-", "nan", "n/a", "n/d") else ""
                    return ""

                medicine = _get("medicine_name")
                if not medicine or medicine.lower() in ("", "nan", "-", "total"):
                    continue

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
                    "batch_no": _get("batch_no"),
                    "registration_no": _get("registration_no"),
                    "reason": _get("reason"),
                    "status": status,
                    "shortage_start": self._parse_spanish_date(_get("shortage_start")),
                    "estimated_end": self._parse_spanish_date(_get("estimated_end")),
                    "source_url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    def _scrape_gob_mx_search(self) -> list[dict]:
        """Use gob.mx search to find COFEPRIS shortage-related documents.

        The gob.mx platform provides a search interface that can be queried
        for COFEPRIS documents mentioning drug shortages.
        """
        records = []
        search_terms = [
            "cofepris desabasto medicamentos",
            "cofepris alerta sanitaria medicamentos",
            "cofepris escasez medicamentos",
        ]

        for term in search_terms:
            print(f"    Searching gob.mx for: {term}")
            try:
                resp = self.session.get(
                    self.SEARCH_API,
                    params={
                        "utf8": "✓",
                        "site": "cofepris",
                        "q": term,
                    },
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "lxml")

                # Parse search results
                results = soup.find_all(["article", "div", "li"], class_=re.compile(
                    r"(result|search-item|item|entry)", re.I
                ))

                for result in results:
                    link = result.find("a", href=True)
                    if not link:
                        continue

                    title = link.get_text(strip=True)
                    href = link["href"]
                    if href.startswith("/"):
                        href = self.BASE + href

                    if not self._is_drug_related(title):
                        continue

                    # Extract date
                    date_el = result.find(["time", "span"], class_=re.compile(
                        r"(date|fecha|time)", re.I
                    ))
                    date_text = date_el.get_text(strip=True) if date_el else ""

                    med_info = self._extract_medicine_info(title)
                    medicine_name = med_info["medicine_name"] if med_info["medicine_name"] else title

                    records.append({
                        "country_code": self.country_code,
                        "country_name": self.country_name,
                        "source": self.source_name,
                        "medicine_name": medicine_name,
                        "active_substance": med_info["active_substance"],
                        "strength": med_info["strength"],
                        "package_size": "",
                        "status": self._classify_status(title),
                        "shortage_start": self._parse_spanish_date(date_text),
                        "estimated_end": None,
                        "alert_title": title,
                        "source_url": href,
                        "scraped_at": datetime.now().isoformat(),
                    })

            except requests.RequestException as e:
                print(f"    Search request failed: {e}")

        return records

    def _check_pagination(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Find additional listing pages from pagination links."""
        next_pages = []

        # gob.mx pagination patterns
        pager = soup.find(["nav", "div", "ul"], class_=re.compile(
            r"(pager|pagination|page-nav|paginas)", re.I
        ))
        if pager:
            for a in pager.find_all("a", href=True):
                href = urljoin(base_url, a["href"])
                if href not in next_pages and href != base_url:
                    next_pages.append(href)

        # Check for 'Siguiente' (next) links
        for a in soup.find_all("a", href=True):
            link_text = a.get_text(strip=True).lower()
            classes = " ".join(a.get("class", []))
            if any(kw in link_text for kw in ("siguiente", "next", ">>", "›")):
                href = urljoin(base_url, a["href"])
                if href not in next_pages:
                    next_pages.append(href)
            elif "next" in classes:
                href = urljoin(base_url, a["href"])
                if href not in next_pages:
                    next_pages.append(href)

        return next_pages[:5]  # Limit to avoid excessive crawling

    def scrape(self) -> pd.DataFrame:
        """Scrape COFEPRIS for drug shortage and sanitary alert data.

        Strategy:
        1. Scrape the main alert listing pages for drug-related alerts
        2. Follow pagination to get additional alerts
        3. Fall back to gob.mx search if listing pages yield no results
        """
        print(f"Scraping {self.country_name} ({self.source_name})...")
        all_records = []

        # Step 1: Scrape primary alert listing pages
        for listing_url in self.ALERT_URLS:
            try:
                listing_records = self._scrape_alert_listing(listing_url)
                if listing_records:
                    all_records.extend(listing_records)
                    print(f"    {len(listing_records)} records from {listing_url[:60]}...")

                # Check for pagination
                resp = self._fetch_page(listing_url)
                if resp:
                    soup = BeautifulSoup(resp.text, "lxml")
                    next_pages = self._check_pagination(soup, listing_url)
                    for page_url in next_pages:
                        try:
                            page_records = self._scrape_alert_listing(page_url)
                            if page_records:
                                all_records.extend(page_records)
                                print(f"    {len(page_records)} records from paginated page")
                        except Exception as e:
                            print(f"    Error scraping page {page_url}: {e}")

            except Exception as e:
                print(f"    Error scraping {listing_url}: {e}")

        print(f"  Step 1: {len(all_records)} records from alert listings")

        # Step 2: If no records from listings, try gob.mx search
        if not all_records:
            print("  No records from listings, trying gob.mx search...")
            try:
                search_records = self._scrape_gob_mx_search()
                if search_records:
                    all_records.extend(search_records)
                    print(f"  Step 2: {len(search_records)} records from gob.mx search")
            except Exception as e:
                print(f"  Step 2: Search failed: {e}")

        if not all_records:
            print("  WARNING: No drug shortage or alert records found from COFEPRIS.")
            print("  The gob.mx site structure may have changed.")
            print("  Returning empty DataFrame. Manual URL verification needed.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "status", "shortage_start",
                "estimated_end", "alert_title", "source_url",
                "scraped_at",
            ])

        df = pd.DataFrame(all_records)

        # Deduplicate based on source_url (same alert page)
        if "source_url" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["source_url"], keep="first")
            if len(df) < before:
                print(f"  Removed {before - len(df)} duplicate records")

        # Ensure all standard columns exist
        for col in ("medicine_name", "active_substance", "strength",
                     "package_size", "shortage_start", "estimated_end", "status"):
            if col not in df.columns:
                df[col] = ""

        print(f"  Total: {len(df)} COFEPRIS alert/shortage records scraped")
        return df
