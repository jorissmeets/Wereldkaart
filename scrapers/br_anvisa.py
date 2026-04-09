"""Scraper for Brazil ANVISA medicine shortage (desabastecimento) data."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class BrAnvisaScraper(BaseScraper):
    """Scraper for ANVISA (Brazil) medicine shortage and discontinuation data.

    ANVISA publishes medicine shortage information through multiple channels:

    1. The consultas.anvisa.gov.br portal — an Angular SPA backed by a REST API
       that lists medicines with discontinuation (descontinuação) notifications.
       The API requires browser-like headers and a valid Referer.

    2. The gov.br informational page about drug discontinuation, which links to
       a MicroStrategy dashboard at sad.anvisa.gov.br.

    3. The gov.br ANVISA news/resolutions section, which publishes resolutions
       and notices about specific medicine shortage situations.

    This scraper attempts the API first, then falls back to scraping the gov.br
    informational pages and news for shortage-related content.

    Scrapeerbaarheid: 2 stars — the portal is JS-heavy and rate-limits aggressively.
    """

    # ANVISA consultas portal API (Angular SPA backend)
    CONSULTAS_BASE = "https://consultas.anvisa.gov.br"
    API_BASE = "https://consultas.anvisa.gov.br/api/consulta"

    # Known API endpoints for medicine data
    DESCONTINUACAO_API = f"{API_BASE}/descontinuacao"
    MEDICAMENTO_API = f"{API_BASE}/medicamento"

    # Gov.br pages with shortage information
    GOVBR_DESCONTINUACAO = (
        "https://www.gov.br/anvisa/pt-br/assuntos/"
        "fiscalizacao-e-monitoramento/mercado/"
        "descontinuacao-de-medicamentos"
    )
    GOVBR_NOTICIAS = (
        "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa"
    )

    # Portuguese keywords for shortage detection in news/announcements
    SHORTAGE_KEYWORDS_PT = [
        "desabastecimento",
        "descontinuação",
        "descontinuacao",
        "falta de medicamento",
        "indisponibilidade",
        "interrupção de produção",
        "interrupção temporária",
        "ruptura de estoque",
        "escassez",
        "suspensão de fabricação",
        "cessação de fabricação",
    ]

    # Status mapping from Portuguese to standardized values
    STATUS_MAP_PT = {
        "descontinuado": "discontinued",
        "descontinuação temporária": "temporary_discontinuation",
        "descontinuação definitiva": "discontinued",
        "interrupção temporária": "temporary_interruption",
        "em desabastecimento": "shortage",
        "normalizado": "resolved",
        "resolvido": "resolved",
        "ativo": "active",
    }

    REQUEST_DELAY = 1.0  # seconds between requests — ANVISA rate-limits

    def __init__(self):
        super().__init__(
            country_code="BR",
            country_name="Brazil",
            source_name="ANVISA",
            base_url="https://consultas.anvisa.gov.br",
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
        })

    def _parse_date(self, val) -> str | None:
        """Parse dates from various Brazilian Portuguese formats."""
        if not val or (isinstance(val, float) and pd.isna(val)):
            return None
        val = str(val).strip()
        if not val or val in ("-", "N/A", "NA", "nan", "None", "—", ""):
            return None

        # Brazilian formats: dd/mm/yyyy is most common
        for fmt in (
            "%d/%m/%Y",       # 15/01/2024
            "%d-%m-%Y",       # 15-01-2024
            "%d.%m.%Y",       # 15.01.2024
            "%Y-%m-%d",       # 2024-01-15
            "%d/%m/%y",       # 15/01/24
            "%d de %B de %Y", # 15 de janeiro de 2024 (handled separately)
            "%d %b %Y",       # 15 jan 2024
            "%B %Y",          # janeiro 2024
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try Portuguese month names manually
        pt_months = {
            "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
            "abril": 4, "maio": 5, "junho": 6,
            "julho": 7, "agosto": 8, "setembro": 9,
            "outubro": 10, "novembro": 11, "dezembro": 12,
        }

        # Pattern: "15 de janeiro de 2024" or "janeiro de 2024"
        m = re.search(
            r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", val, re.IGNORECASE
        )
        if m:
            day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
            month_num = pt_months.get(month_name)
            if month_num:
                try:
                    return datetime(int(year), month_num, int(day)).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Pattern: "janeiro de 2024" (no day)
        m = re.search(r"(\w+)\s+de\s+(\d{4})", val, re.IGNORECASE)
        if m:
            month_name, year = m.group(1).lower(), m.group(2)
            month_num = pt_months.get(month_name)
            if month_num:
                try:
                    return datetime(int(year), month_num, 1).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # ISO timestamp: 2024-01-15T10:30:00.000Z
        m = re.match(r"(\d{4}-\d{2}-\d{2})T", val)
        if m:
            return m.group(1)

        return None

    def _normalize_status(self, raw_status: str) -> str:
        """Map Portuguese status text to standardized status values."""
        if not raw_status:
            return "shortage"
        status_lower = raw_status.strip().lower()
        for pt_status, en_status in self.STATUS_MAP_PT.items():
            if pt_status in status_lower:
                return en_status
        if any(kw in status_lower for kw in ("descontinua", "cessação")):
            return "discontinued"
        if any(kw in status_lower for kw in ("normaliz", "resolv", "disponível")):
            return "resolved"
        if any(kw in status_lower for kw in ("interrupção", "temporári")):
            return "temporary_interruption"
        return "shortage"

    def _is_shortage_related(self, text: str) -> bool:
        """Check if text contains Portuguese shortage-related keywords."""
        text_lower = text.lower()
        return any(kw in text_lower for kw in self.SHORTAGE_KEYWORDS_PT)

    # ------------------------------------------------------------------
    # Strategy 1: ANVISA consultas API
    # ------------------------------------------------------------------

    def _scrape_api(self) -> list[dict]:
        """Try to fetch data from the ANVISA consultas REST API.

        The Angular SPA at consultas.anvisa.gov.br makes XHR requests to
        /api/consulta/descontinuacao (and similar endpoints). These require
        browser-like headers and a valid Origin/Referer to avoid 403.
        """
        records = []

        api_headers = {
            "Origin": self.CONSULTAS_BASE,
            "Referer": f"{self.CONSULTAS_BASE}/",
            "Authorization": "Guest",
            "Content-Type": "application/json",
        }

        # Try multiple known API endpoint patterns
        api_endpoints = [
            (f"{self.API_BASE}/descontinuacao", {"count": 100, "page": 1}),
            (f"{self.API_BASE}/medicamentos/desabastecimento", {"count": 100, "page": 1}),
            (f"{self.API_BASE}/desabastecimento", {"count": 100, "page": 1}),
            (f"{self.API_BASE}/consultas/descontinuacao/medicamentos", {"count": 100, "page": 1}),
        ]

        for endpoint, params in api_endpoints:
            try:
                resp = self.session.get(
                    endpoint,
                    params=params,
                    headers=api_headers,
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue

                data = resp.json()
                if not data:
                    continue

                # Handle paginated response (common ANVISA API pattern)
                items = []
                if isinstance(data, dict):
                    items = data.get("content", data.get("data", data.get("results", [])))
                    total_pages = data.get("totalPages", 1)
                elif isinstance(data, list):
                    items = data
                    total_pages = 1
                else:
                    continue

                if not items:
                    continue

                print(f"  API endpoint responded: {endpoint}")
                print(f"  First page: {len(items)} items, total pages: {total_pages}")

                # Parse first page
                for item in items:
                    record = self._parse_api_item(item)
                    if record:
                        records.append(record)

                # Fetch remaining pages
                for page in range(2, min(total_pages + 1, 50)):
                    time.sleep(self.REQUEST_DELAY)
                    try:
                        params["page"] = page
                        resp = self.session.get(
                            endpoint,
                            params=params,
                            headers=api_headers,
                            timeout=30,
                        )
                        if resp.status_code != 200:
                            break

                        page_data = resp.json()
                        page_items = []
                        if isinstance(page_data, dict):
                            page_items = page_data.get(
                                "content",
                                page_data.get("data", page_data.get("results", [])),
                            )
                        elif isinstance(page_data, list):
                            page_items = page_data

                        if not page_items:
                            break

                        for item in page_items:
                            record = self._parse_api_item(item)
                            if record:
                                records.append(record)

                        if page % 5 == 0:
                            print(f"    Page {page}/{total_pages}, {len(records)} records so far...")

                    except Exception:
                        break

                # If we got data from one endpoint, no need to try others
                if records:
                    break

            except requests.RequestException:
                continue
            except (ValueError, KeyError):
                continue

        return records

    def _parse_api_item(self, item: dict) -> dict | None:
        """Parse a single item from the ANVISA API response into a standard record.

        The API response fields vary by endpoint but commonly include:
        - nomeProduto / nome / medicamento: product name
        - principioAtivo / substanciaAtiva: active substance
        - concentracao: strength/concentration
        - apresentacao: presentation/package
        - motivo / motivoDescontinuacao: reason for discontinuation
        - dataInicio / dataDescontinuacao: start date
        - dataPrevisao / dataPrevisaoRetorno: expected return date
        - situacao / status: current status
        - empresa / detentor: marketing authorization holder
        - registro / numeroRegistro: registration number
        """
        if not isinstance(item, dict):
            return None

        # Extract medicine name — try multiple possible field names
        medicine_name = ""
        for key in ("nomeProduto", "nome", "medicamento", "nomeMedicamento",
                     "produto", "nomeComercial", "descricao"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                medicine_name = str(val).strip()
                break

        if not medicine_name:
            return None

        # Extract active substance
        active_substance = ""
        for key in ("principioAtivo", "substanciaAtiva", "ativo",
                     "principioAtivoDescricao", "dci"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                active_substance = str(val).strip()
                break

        # Extract strength
        strength = ""
        for key in ("concentracao", "dosagem", "posologia", "apresentacao"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                strength = str(val).strip()
                break

        # Extract package size / presentation
        package_size = ""
        for key in ("apresentacao", "formaFarmaceutica", "embalagem"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                # Avoid duplicating if we already used it for strength
                if str(val).strip() != strength:
                    package_size = str(val).strip()
                    break

        # Extract dates
        shortage_start = None
        for key in ("dataInicio", "dataDescontinuacao", "dataNotificacao",
                     "dataInicioDesabastecimento", "dataComunicacao"):
            val = item.get(key)
            if val:
                shortage_start = self._parse_date(val)
                if shortage_start:
                    break

        estimated_end = None
        for key in ("dataPrevisao", "dataPrevisaoRetorno", "dataFim",
                     "dataPrevisaoNormalizacao", "previsaoRetorno"):
            val = item.get(key)
            if val:
                estimated_end = self._parse_date(val)
                if estimated_end:
                    break

        # Extract status
        raw_status = ""
        for key in ("situacao", "status", "tipoDescontinuacao", "tipo"):
            val = item.get(key, "")
            if val and str(val).strip():
                raw_status = str(val).strip()
                break

        # Extract reason
        reason = ""
        for key in ("motivo", "motivoDescontinuacao", "justificativa",
                     "motivoDesabastecimento"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                reason = str(val).strip()
                break

        # Extract holder / company
        holder = ""
        for key in ("empresa", "detentor", "titular", "fabricante",
                     "razaoSocial", "laboratorio"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                holder = str(val).strip()
                break

        # Extract registration number
        reg_number = ""
        for key in ("registro", "numeroRegistro", "registroAnvisa",
                     "numeroProcesso", "processo"):
            val = item.get(key, "")
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                reg_number = str(val).strip()
                break

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine_name,
            "active_substance": active_substance,
            "strength": strength,
            "package_size": package_size,
            "registration_number": reg_number,
            "marketing_auth_holder": holder,
            "shortage_start": shortage_start,
            "estimated_end": estimated_end,
            "status": self._normalize_status(raw_status),
            "reason": reason,
            "scraped_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Strategy 2: Gov.br descontinuação page scraping
    # ------------------------------------------------------------------

    def _scrape_govbr_descontinuacao(self) -> list[dict]:
        """Scrape the gov.br discontinuation page for links to data or dashboards.

        The page at gov.br/anvisa/.../descontinuacao-de-medicamentos contains
        information about the notification process and links to the MicroStrategy
        dashboard (sad.anvisa.gov.br) with the actual data.
        """
        records = []

        try:
            resp = self.session.get(
                self.GOVBR_DESCONTINUACAO,
                headers={"Accept": "text/html,application/xhtml+xml"},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  Gov.br page returned status {resp.status_code}")
                return records
        except requests.RequestException as e:
            print(f"  Could not access gov.br page: {e}")
            return records

        soup = BeautifulSoup(resp.text, "lxml")

        # Look for embedded tables with medicine data
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            headers = [
                th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])
            ]

            # Map column indices to fields
            col_map = {}
            for i, h in enumerate(headers):
                if any(kw in h for kw in ("medicamento", "produto", "nome")):
                    col_map["medicine_name"] = i
                elif any(kw in h for kw in ("princípio", "principio", "ativo", "substância")):
                    col_map["active_substance"] = i
                elif any(kw in h for kw in ("concentra", "dosagem")):
                    col_map["strength"] = i
                elif any(kw in h for kw in ("situação", "situacao", "status")):
                    col_map["status"] = i
                elif any(kw in h for kw in ("data", "início", "inicio")):
                    col_map["date"] = i
                elif any(kw in h for kw in ("empresa", "titular", "laborat")):
                    col_map["holder"] = i

            if "medicine_name" not in col_map:
                continue

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells or all(not c for c in cells):
                    continue

                name_idx = col_map["medicine_name"]
                if name_idx >= len(cells) or not cells[name_idx]:
                    continue

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": cells[name_idx],
                    "active_substance": (
                        cells[col_map["active_substance"]]
                        if "active_substance" in col_map and col_map["active_substance"] < len(cells)
                        else ""
                    ),
                    "strength": (
                        cells[col_map["strength"]]
                        if "strength" in col_map and col_map["strength"] < len(cells)
                        else ""
                    ),
                    "package_size": "",
                    "shortage_start": (
                        self._parse_date(cells[col_map["date"]])
                        if "date" in col_map and col_map["date"] < len(cells)
                        else None
                    ),
                    "estimated_end": None,
                    "status": (
                        self._normalize_status(cells[col_map["status"]])
                        if "status" in col_map and col_map["status"] < len(cells)
                        else "discontinued"
                    ),
                    "marketing_auth_holder": (
                        cells[col_map["holder"]]
                        if "holder" in col_map and col_map["holder"] < len(cells)
                        else ""
                    ),
                    "scraped_at": datetime.now().isoformat(),
                })

        # Also look for download links (Excel, CSV, PDF)
        download_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if any(ext in href.lower() for ext in (".xls", ".xlsx", ".csv")):
                download_links.append(urljoin(self.GOVBR_DESCONTINUACAO, href))
            elif any(kw in text for kw in ("download", "baixar", "planilha", "lista")):
                download_links.append(urljoin(self.GOVBR_DESCONTINUACAO, href))

        # Try downloading any Excel/CSV files found
        for link in download_links[:3]:  # Limit to 3 attempts
            try:
                dl_records = self._try_download_file(link)
                records.extend(dl_records)
            except Exception as e:
                print(f"    Could not download {link}: {e}")
            time.sleep(self.REQUEST_DELAY)

        # Extract links to MicroStrategy or other data portals
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "sad.anvisa.gov.br" in href or "microstrategy" in href.lower():
                print(f"  Found MicroStrategy dashboard link: {href}")
                print("  Note: MicroStrategy dashboards require JavaScript — "
                      "data may need manual extraction or Selenium.")

        return records

    def _try_download_file(self, url: str) -> list[dict]:
        """Try to download and parse an Excel or CSV file from a URL."""
        import tempfile
        import os

        records = []

        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()

        # Determine file type
        if any(sig in resp.content[:4] for sig in (b"\xd0\xcf\x11\xe0", b"PK\x03\x04")):
            ext = ".xlsx" if resp.content[:4] == b"PK\x03\x04" else ".xls"
        elif "csv" in content_type or "text/csv" in content_type:
            ext = ".csv"
        elif "excel" in content_type or "spreadsheet" in content_type:
            ext = ".xlsx"
        else:
            return records

        tmp_path = os.path.join(tempfile.gettempdir(), f"br_anvisa{ext}")
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        try:
            if ext == ".csv":
                df = pd.read_csv(tmp_path, encoding="utf-8-sig")
            else:
                df = pd.read_excel(tmp_path)

            print(f"    Downloaded file: {len(df)} rows")

            # Auto-map columns by scanning headers for Portuguese keywords
            col_map = self._auto_map_columns(df.columns)
            if "medicine_name" not in col_map:
                # Try reading with header on different rows
                for header_row in range(1, min(10, len(df))):
                    try:
                        if ext == ".csv":
                            df2 = pd.read_csv(tmp_path, header=header_row, encoding="utf-8-sig")
                        else:
                            df2 = pd.read_excel(tmp_path, header=header_row)
                        col_map = self._auto_map_columns(df2.columns)
                        if "medicine_name" in col_map:
                            df = df2
                            break
                    except Exception:
                        continue

            if "medicine_name" not in col_map:
                return records

            for _, row in df.iterrows():
                name_col = col_map["medicine_name"]
                medicine_name = str(row.get(name_col, "")).strip()
                if not medicine_name or medicine_name.lower() in ("nan", ""):
                    continue

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": medicine_name,
                    "active_substance": (
                        str(row.get(col_map.get("active_substance", ""), "")).strip()
                        if col_map.get("active_substance") and pd.notna(row.get(col_map.get("active_substance", "")))
                        else ""
                    ),
                    "strength": (
                        str(row.get(col_map.get("strength", ""), "")).strip()
                        if col_map.get("strength") and pd.notna(row.get(col_map.get("strength", "")))
                        else ""
                    ),
                    "package_size": "",
                    "shortage_start": (
                        self._parse_date(row.get(col_map.get("date", "")))
                        if col_map.get("date")
                        else None
                    ),
                    "estimated_end": None,
                    "status": (
                        self._normalize_status(str(row.get(col_map.get("status", ""), "")))
                        if col_map.get("status")
                        else "discontinued"
                    ),
                    "scraped_at": datetime.now().isoformat(),
                })

        except Exception as e:
            print(f"    Error parsing downloaded file: {e}")
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

        return records

    def _auto_map_columns(self, columns) -> dict:
        """Map DataFrame column names to standard field names using Portuguese keywords."""
        col_map = {}
        for col in columns:
            col_lower = str(col).strip().lower()
            if any(kw in col_lower for kw in ("medicamento", "produto", "nome comercial", "nome do produto")):
                col_map["medicine_name"] = col
            elif any(kw in col_lower for kw in ("princípio ativo", "principio ativo", "substância ativa", "substancia ativa", "dci")):
                col_map["active_substance"] = col
            elif any(kw in col_lower for kw in ("concentração", "concentracao", "dosagem", "apresentação", "apresentacao")):
                col_map["strength"] = col
            elif any(kw in col_lower for kw in ("situação", "situacao", "status")):
                col_map["status"] = col
            elif any(kw in col_lower for kw in ("data", "início", "inicio", "notificação", "notificacao")):
                col_map["date"] = col
            elif any(kw in col_lower for kw in ("empresa", "titular", "laboratório", "laboratorio", "detentor")):
                col_map["holder"] = col
            elif any(kw in col_lower for kw in ("registro", "processo")):
                col_map["registration"] = col
        return col_map

    # ------------------------------------------------------------------
    # Strategy 3: Gov.br ANVISA news scraping for shortage-related items
    # ------------------------------------------------------------------

    def _scrape_govbr_news(self) -> list[dict]:
        """Scrape ANVISA news pages for shortage-related announcements.

        ANVISA publishes resolutions (RDC) and technical notes (Nota Técnica)
        about specific drug shortage situations through its gov.br news section.
        """
        records = []
        seen_urls = set()

        # Search terms for shortage-related news
        search_terms = [
            "desabastecimento medicamento",
            "descontinuação medicamento",
            "ruptura estoque medicamento",
        ]

        for term in search_terms:
            search_url = (
                f"https://www.gov.br/anvisa/pt-br/search?"
                f"SearchableText={term.replace(' ', '+')}"
                f"&portal_type=News+Item"
            )

            try:
                resp = self.session.get(
                    search_url,
                    headers={"Accept": "text/html,application/xhtml+xml"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue
            except requests.RequestException:
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Find search result items
            result_items = soup.find_all(
                "article", class_=re.compile(r"tileItem|searchResult|entry")
            )
            if not result_items:
                # Try alternative selectors
                result_items = soup.find_all(
                    "div", class_=re.compile(r"tileItem|resultado|item")
                )
            if not result_items:
                # Broad fallback: look for links in the main content
                main = soup.find("main") or soup.find("div", id="content") or soup
                for a in main.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if not text or len(text) < 15:
                        continue
                    if self._is_shortage_related(text) and href not in seen_urls:
                        full_url = urljoin("https://www.gov.br", href)
                        seen_urls.add(full_url)
                        result_items.append(a)

            for item in result_items:
                if isinstance(item, BeautifulSoup) or hasattr(item, "find"):
                    a_tag = item.find("a", href=True) if item.name != "a" else item
                else:
                    continue

                if not a_tag or not a_tag.get("href"):
                    continue

                href = a_tag["href"]
                title = a_tag.get_text(strip=True)

                if not title or not self._is_shortage_related(title):
                    continue

                full_url = urljoin("https://www.gov.br", href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Extract date from result item
                date_str = ""
                date_el = None
                if item.name != "a":
                    date_el = item.find(
                        class_=re.compile(r"date|time|data|publicado")
                    ) or item.find("time") or item.find("span", class_="summary-view-icon")
                if date_el:
                    date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": title,
                    "active_substance": "",
                    "strength": "",
                    "package_size": "",
                    "shortage_start": self._parse_date(date_str),
                    "estimated_end": None,
                    "status": "shortage",
                    "announcement_title": title,
                    "announcement_url": full_url,
                    "scraped_at": datetime.now().isoformat(),
                })

            time.sleep(self.REQUEST_DELAY)

        # For the most relevant news items, try scraping detail pages
        detail_records = []
        for i, rec in enumerate(records[:10]):  # Limit detail page scraping
            ann_url = rec.get("announcement_url", "")
            if not ann_url:
                continue

            try:
                detail_recs = self._scrape_news_detail(ann_url)
                if detail_recs:
                    detail_records.extend(detail_recs)
            except Exception as e:
                print(f"    Could not scrape detail page: {e}")

            time.sleep(self.REQUEST_DELAY)

        # If we got detail-level records, prefer those over title-level records
        if detail_records:
            return detail_records

        return records

    def _scrape_news_detail(self, url: str) -> list[dict]:
        """Scrape a single news detail page for structured medicine shortage data."""
        records = []

        try:
            resp = self.session.get(
                url,
                headers={"Accept": "text/html,application/xhtml+xml"},
                timeout=30,
            )
            if resp.status_code != 200:
                return records
        except requests.RequestException:
            return records

        soup = BeautifulSoup(resp.text, "lxml")

        # Look for the main content area
        content = (
            soup.find("div", id="content-core")
            or soup.find("div", class_=re.compile(r"content|article|post"))
            or soup.find("article")
            or soup.find("main")
        )
        if not content:
            content = soup

        text = content.get_text("\n", strip=True)

        # Extract publication date from the page
        pub_date = None
        date_el = soup.find("span", class_=re.compile(r"documentPublished|date|data"))
        if date_el:
            pub_date = self._parse_date(date_el.get_text(strip=True))

        # Look for tables with medicine data
        tables = content.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            headers = [
                th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])
            ]

            col_map = {}
            for i, h in enumerate(headers):
                if any(kw in h for kw in ("medicamento", "produto", "nome")):
                    col_map["medicine_name"] = i
                elif any(kw in h for kw in ("princípio", "principio", "ativo", "substância")):
                    col_map["active_substance"] = i
                elif any(kw in h for kw in ("concentra", "dosagem", "apresentação")):
                    col_map["strength"] = i
                elif any(kw in h for kw in ("situação", "situacao", "status")):
                    col_map["status"] = i

            if "medicine_name" not in col_map and "active_substance" not in col_map:
                continue

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells or all(not c for c in cells):
                    continue

                name = ""
                if "medicine_name" in col_map and col_map["medicine_name"] < len(cells):
                    name = cells[col_map["medicine_name"]]
                substance = ""
                if "active_substance" in col_map and col_map["active_substance"] < len(cells):
                    substance = cells[col_map["active_substance"]]

                if not name and not substance:
                    continue

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": name,
                    "active_substance": substance,
                    "strength": (
                        cells[col_map["strength"]]
                        if "strength" in col_map and col_map["strength"] < len(cells)
                        else ""
                    ),
                    "package_size": "",
                    "shortage_start": pub_date,
                    "estimated_end": None,
                    "status": (
                        self._normalize_status(cells[col_map["status"]])
                        if "status" in col_map and col_map["status"] < len(cells)
                        else "shortage"
                    ),
                    "announcement_url": url,
                    "scraped_at": datetime.now().isoformat(),
                })

        # If no tables found, extract medicine names from text using patterns
        if not records:
            # Common patterns in ANVISA news about shortages
            medicine_patterns = [
                re.compile(
                    r"(?:medicamento|produto)\s*[:\-–]\s*(.+?)(?:\n|$)",
                    re.IGNORECASE,
                ),
                re.compile(
                    r"(?:princípio ativo|substância ativa)\s*[:\-–]\s*(.+?)(?:\n|$)",
                    re.IGNORECASE,
                ),
            ]
            for pattern in medicine_patterns:
                m = pattern.search(text)
                if m:
                    name = m.group(1).strip().rstrip(",;.")
                    if name and len(name) >= 3:
                        records.append({
                            "country_code": self.country_code,
                            "country_name": self.country_name,
                            "source": self.source_name,
                            "medicine_name": name,
                            "active_substance": "",
                            "strength": "",
                            "package_size": "",
                            "shortage_start": pub_date,
                            "estimated_end": None,
                            "status": "shortage",
                            "announcement_url": url,
                            "scraped_at": datetime.now().isoformat(),
                        })
                        break  # One record per page in text mode

        return records

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        """Scrape ANVISA medicine shortage data.

        Uses a multi-strategy approach:
        1. Try the ANVISA consultas REST API (most structured data)
        2. Scrape the gov.br discontinuation page for tables and downloads
        3. Scrape gov.br news for shortage-related announcements

        Returns combined and deduplicated results.
        """
        print(f"Scraping {self.country_name} ({self.source_name})...")
        all_records = []

        # Strategy 1: ANVISA consultas API
        print("  Strategy 1: Trying ANVISA consultas API...")
        try:
            api_records = self._scrape_api()
            if api_records:
                all_records.extend(api_records)
                print(f"  Strategy 1: {len(api_records)} records from API")
            else:
                print("  Strategy 1: No data from API (likely 403 — portal requires JS session)")
        except Exception as e:
            print(f"  Strategy 1: API scraping failed: {e}")

        # Strategy 2: Gov.br discontinuation page
        print("  Strategy 2: Scraping gov.br discontinuation page...")
        try:
            govbr_records = self._scrape_govbr_descontinuacao()
            if govbr_records:
                all_records.extend(govbr_records)
                print(f"  Strategy 2: {len(govbr_records)} records from gov.br page")
            else:
                print("  Strategy 2: No tabular data on gov.br page "
                      "(data is behind MicroStrategy dashboard)")
        except Exception as e:
            print(f"  Strategy 2: Gov.br scraping failed: {e}")

        # Strategy 3: Gov.br news (always run as supplementary data)
        print("  Strategy 3: Scraping gov.br ANVISA news...")
        try:
            news_records = self._scrape_govbr_news()
            if news_records:
                all_records.extend(news_records)
                print(f"  Strategy 3: {len(news_records)} records from news")
            else:
                print("  Strategy 3: No shortage-related news found")
        except Exception as e:
            print(f"  Strategy 3: News scraping failed: {e}")

        if not all_records:
            print("  Warning: No shortage data could be extracted from any source.")
            print("  ANVISA's main data is behind a MicroStrategy dashboard at "
                  "sad.anvisa.gov.br which requires JavaScript execution.")
            print("  Consider using Selenium or Playwright for full data extraction.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "status", "shortage_start",
                "estimated_end", "scraped_at",
            ])

        df = pd.DataFrame(all_records)

        # Ensure all standard columns exist
        for col in ("medicine_name", "active_substance", "strength",
                     "package_size", "shortage_start", "estimated_end", "status"):
            if col not in df.columns:
                df[col] = ""

        # Deduplicate by medicine name (case-insensitive)
        if not df.empty and "medicine_name" in df.columns:
            before = len(df)
            df["_dedup_key"] = df["medicine_name"].str.strip().str.lower()
            df = df.drop_duplicates(subset=["_dedup_key"], keep="first")
            df = df.drop(columns=["_dedup_key"])
            if len(df) < before:
                print(f"  Deduplicated: {before} -> {len(df)} records")

        print(f"  Total: {len(df)} shortage/discontinuation records scraped")
        return df
