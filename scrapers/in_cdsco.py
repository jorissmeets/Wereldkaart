"""Scraper for India CDSCO (Central Drugs Standard Control Organisation) NSQ drug data."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class InCdscoScraper(BaseScraper):
    """Scraper for CDSCO (India) Not of Standard Quality (NSQ) drug alerts.

    CDSCO publishes NSQ data in two locations:
    1. A dedicated NSQ drugs page with DataTables (JS-rendered):
       /opencms/opencms/en/Notifications/nsq-drugs/
    2. Monthly NSQ alert PDFs listed on the Alerts page:
       /opencms/opencms/en/Notifications/Alerts/

    This scraper targets the NSQ drugs page first (which contains structured
    HTML tables with drug details including name, batch number, manufacturer,
    and test results). If that page yields no data (due to JS rendering), it
    falls back to scraping the Alerts page for NSQ alert metadata.
    """

    BASE = "https://cdsco.gov.in"
    NSQ_DRUGS_URL = "https://cdsco.gov.in/opencms/opencms/en/Notifications/nsq-drugs/"
    ALERTS_URL = "https://cdsco.gov.in/opencms/opencms/en/Notifications/Alerts/"

    # Known DataTable IDs on the NSQ drugs page
    TABLE_IDS = [
        "example", "example1", "example2", "example3",
        "example4", "example5", "example6", "example7",
        "example_upload", "example_upload_indus", "example_upload_pen",
    ]

    # Common headers found in CDSCO NSQ tables (various spellings observed)
    HEADER_PATTERNS = {
        "s_no": re.compile(r"s\.?\s*no|sr\.?\s*no|sl\.?\s*no", re.IGNORECASE),
        "drug_name": re.compile(r"name\s*of\s*(the\s*)?drug|drug\s*name|product\s*name|medicine\s*name", re.IGNORECASE),
        "batch_no": re.compile(r"batch|b\.?\s*no|lot", re.IGNORECASE),
        "manufacturer": re.compile(r"manuf|mfr|m/s|firm|company|licence\s*holder", re.IGNORECASE),
        "reason": re.compile(r"reason|ground|nsq\s*reason|not\s*of\s*standard", re.IGNORECASE),
        "date_drawn": re.compile(r"date\s*(of\s*)?(drawn|sample|collection|sampling)", re.IGNORECASE),
        "date_tested": re.compile(r"date\s*(of\s*)?(test|analy)", re.IGNORECASE),
        "lab": re.compile(r"lab|laboratory|testing\s*lab", re.IGNORECASE),
        "state": re.compile(r"state|zone|division", re.IGNORECASE),
        "category": re.compile(r"categor|type|class", re.IGNORECASE),
    }

    def __init__(self):
        super().__init__(
            country_code="IN",
            country_name="India",
            source_name="CDSCO",
            base_url="https://cdsco.gov.in",
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _parse_date(self, val) -> str | None:
        """Parse various date formats found in CDSCO data."""
        if pd.isna(val) or not val:
            return None
        val = str(val).strip()
        if not val or val in ("-", "N/A", "NA", "nan", "None"):
            return None
        # Try common Indian/CDSCO date formats
        for fmt in (
            "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
            "%d/%m/%y", "%d-%m-%y",
            "%Y-%m-%d",
            "%d %b %Y", "%d %B %Y",
            "%b %Y", "%B %Y",
            "%d-%b-%Y", "%d-%B-%Y",
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_month_year(self, title: str) -> str | None:
        """Extract month/year from alert titles like 'NSQ ALERT FOR THE MONTH OF MAY-2025'."""
        m = re.search(
            r"(?:MONTH\s+OF\s+)?(\w+)[\s\-]+(\d{4})",
            title, re.IGNORECASE,
        )
        if m:
            month_str, year_str = m.group(1), m.group(2)
            for fmt in ("%B", "%b"):
                try:
                    dt = datetime.strptime(f"1 {month_str} {year_str}", f"%d {fmt} %Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        return None

    def _map_columns(self, headers: list[str]) -> dict[int, str]:
        """Map column indices to standardized field names based on header text."""
        mapping = {}
        for idx, header in enumerate(headers):
            header_clean = header.strip()
            if not header_clean:
                continue
            for field_name, pattern in self.HEADER_PATTERNS.items():
                if pattern.search(header_clean):
                    mapping[idx] = field_name
                    break
        return mapping

    def _extract_strength(self, drug_name: str) -> tuple[str, str]:
        """Try to separate strength from drug name if embedded.

        Returns (cleaned_name, strength).
        """
        # Common patterns: "Paracetamol 500mg Tablets", "Amoxicillin 250mg/5ml"
        m = re.search(
            r"(\d+\s*(?:mg|mcg|g|ml|iu|%|µg)(?:/\d*\s*(?:mg|mcg|g|ml|iu|%|µg))?)",
            drug_name, re.IGNORECASE,
        )
        if m:
            strength = m.group(1).strip()
            name = drug_name[:m.start()].strip().rstrip("-,/ ")
            if name:
                return name, strength
        return drug_name, ""

    def _scrape_nsq_tables(self) -> list[dict]:
        """Attempt to scrape structured NSQ data from the dedicated NSQ page.

        The CDSCO NSQ page uses DataTables with HTML tables that may contain
        data in the raw HTML (before JS enhancement).
        """
        print("  Trying NSQ drugs page...")
        records = []

        try:
            resp = self.session.get(self.NSQ_DRUGS_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Warning: Could not access NSQ drugs page: {e}")
            return records

        soup = BeautifulSoup(resp.text, "lxml")

        # Try each known table ID
        tables_found = 0
        for table_id in self.TABLE_IDS:
            table = soup.find("table", {"id": table_id})
            if not table:
                continue

            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            tables_found += 1

            # Extract headers from first row with th or td
            header_row = rows[0]
            headers = [
                cell.get_text(strip=True)
                for cell in header_row.find_all(["th", "td"])
            ]
            col_map = self._map_columns(headers)

            if not col_map:
                # Try second row as header
                if len(rows) > 1:
                    headers = [
                        cell.get_text(strip=True)
                        for cell in rows[1].find_all(["th", "td"])
                    ]
                    col_map = self._map_columns(headers)
                    rows = rows[2:]  # Skip both header rows
                else:
                    continue
            else:
                rows = rows[1:]  # Skip header row

            for row in rows:
                cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                if not cells or all(not c for c in cells):
                    continue

                raw = {}
                for idx, field_name in col_map.items():
                    if idx < len(cells):
                        raw[field_name] = cells[idx]

                drug_name = raw.get("drug_name", "")
                if not drug_name:
                    continue

                name_clean, strength = self._extract_strength(drug_name)

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": name_clean,
                    "active_substance": "",
                    "strength": strength,
                    "package_size": "",
                    "batch_no": raw.get("batch_no", ""),
                    "manufacturer": raw.get("manufacturer", ""),
                    "status": "NSQ",
                    "shortage_start": self._parse_date(raw.get("date_drawn")) or self._parse_date(raw.get("date_tested")),
                    "estimated_end": None,
                    "reason": raw.get("reason", ""),
                    "testing_lab": raw.get("lab", ""),
                    "state": raw.get("state", ""),
                    "scraped_at": datetime.now().isoformat(),
                })

        if tables_found > 0:
            print(f"  Found {tables_found} table(s) on NSQ page, {len(records)} records")
        else:
            print("  No populated tables found on NSQ page (JS-rendered content)")

        # Also try parsing any generic tables not matched by ID
        if not records:
            for table in soup.find_all("table"):
                if table.get("id") in self.TABLE_IDS:
                    continue  # Already processed
                rows = table.find_all("tr")
                if len(rows) < 2:
                    continue

                headers = [
                    cell.get_text(strip=True)
                    for cell in rows[0].find_all(["th", "td"])
                ]
                col_map = self._map_columns(headers)
                if not col_map:
                    continue

                tables_found += 1
                for row in rows[1:]:
                    cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                    if not cells or all(not c for c in cells):
                        continue

                    raw = {}
                    for idx, field_name in col_map.items():
                        if idx < len(cells):
                            raw[field_name] = cells[idx]

                    drug_name = raw.get("drug_name", "")
                    if not drug_name:
                        continue

                    name_clean, strength = self._extract_strength(drug_name)

                    records.append({
                        "country_code": self.country_code,
                        "country_name": self.country_name,
                        "source": self.source_name,
                        "medicine_name": name_clean,
                        "active_substance": "",
                        "strength": strength,
                        "package_size": "",
                        "batch_no": raw.get("batch_no", ""),
                        "manufacturer": raw.get("manufacturer", ""),
                        "status": "NSQ",
                        "shortage_start": self._parse_date(raw.get("date_drawn")) or self._parse_date(raw.get("date_tested")),
                        "estimated_end": None,
                        "reason": raw.get("reason", ""),
                        "testing_lab": raw.get("lab", ""),
                        "state": raw.get("state", ""),
                        "scraped_at": datetime.now().isoformat(),
                    })

        return records

    def _scrape_alerts_page(self) -> list[dict]:
        """Scrape the Alerts page for NSQ alert entries.

        This provides a fallback when the NSQ drugs page is JS-rendered.
        Extracts alert metadata (title, date, PDF link) for all NSQ-related alerts.
        """
        print("  Scraping Alerts page for NSQ entries...")
        records = []

        try:
            resp = self.session.get(self.ALERTS_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Warning: Could not access Alerts page: {e}")
            return records

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table")
        if not table:
            print("  Warning: No table found on Alerts page")
            return records

        rows = table.find_all("tr")
        nsq_count = 0

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            # Extract title text
            title = cells[1].get_text(strip=True) if len(cells) > 1 else ""

            # Filter for NSQ-related alerts
            if not re.search(r"NSQ|not\s+of\s+standard\s+quality", title, re.IGNORECASE):
                continue

            nsq_count += 1

            # Extract release date
            release_date_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            # Extract PDF download link
            pdf_link = ""
            link_tag = row.find("a", href=True)
            if link_tag:
                pdf_link = urljoin(self.BASE, link_tag["href"])

            # Determine if this is a state-level or central alert
            is_state = bool(re.search(r"\bstate\b", title, re.IGNORECASE))
            alert_type = "State NSQ Alert" if is_state else "Central NSQ Alert"

            # Parse the alert month/year as approximate shortage date
            alert_date = self._extract_month_year(title)

            # Parse the release/publication date
            release_date = self._parse_date(release_date_text)

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": title,
                "active_substance": "",
                "strength": "",
                "package_size": "",
                "status": "NSQ",
                "alert_type": alert_type,
                "shortage_start": alert_date or release_date,
                "estimated_end": None,
                "release_date": release_date,
                "pdf_url": pdf_link,
                "scraped_at": datetime.now().isoformat(),
            })

        print(f"  Found {nsq_count} NSQ alert entries on Alerts page")
        return records

    def _scrape_alert_pdf_links(self, max_pdfs: int = 3) -> list[dict]:
        """Download and parse the most recent NSQ alert PDFs for detailed drug data.

        Each monthly PDF contains a table of NSQ drugs with columns such as:
        S.No, Name of Drug, Batch No, Manufacturer, Reason for NSQ, etc.

        Args:
            max_pdfs: Maximum number of recent PDFs to process.
        """
        print(f"  Attempting to parse up to {max_pdfs} recent NSQ alert PDFs...")
        records = []

        try:
            resp = self.session.get(self.ALERTS_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Warning: Could not access Alerts page: {e}")
            return records

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table")
        if not table:
            return records

        pdf_urls = []
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            title = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            # Only central NSQ alerts (not state), as they tend to be more structured
            if re.search(r"NSQ\s+ALERT\s+FOR\s+THE\s+MONTH", title, re.IGNORECASE) and \
               not re.search(r"\bstate\b", title, re.IGNORECASE):
                link_tag = row.find("a", href=True)
                if link_tag:
                    url = urljoin(self.BASE, link_tag["href"])
                    alert_date = self._extract_month_year(title)
                    pdf_urls.append((url, title, alert_date))
                    if len(pdf_urls) >= max_pdfs:
                        break

        for pdf_url, title, alert_date in pdf_urls:
            try:
                pdf_records = self._parse_nsq_pdf(pdf_url, alert_date)
                records.extend(pdf_records)
                print(f"    {title}: {len(pdf_records)} drugs extracted")
                time.sleep(1)  # Be respectful to the server
            except Exception as e:
                print(f"    Warning: Could not parse PDF for '{title}': {e}")

        return records

    def _parse_nsq_pdf(self, pdf_url: str, alert_date: str | None) -> list[dict]:
        """Download and parse a single NSQ alert PDF.

        Uses tabula-py if available, otherwise returns empty list.
        """
        import tempfile
        import os

        records = []

        resp = self.session.get(pdf_url, timeout=60)
        resp.raise_for_status()

        # Check if we got a PDF (the download JSP may redirect)
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not resp.content[:5] == b"%PDF-":
            return records

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        try:
            import tabula
            dfs = tabula.read_pdf(tmp_path, pages="all", lattice=True, multiple_tables=True)

            for df in dfs:
                if df.empty or len(df.columns) < 3:
                    continue

                # Try to identify columns
                df.columns = [str(c).strip() for c in df.columns]
                col_map = self._map_columns(list(df.columns))

                # If headers aren't in column names, check first row
                if not col_map and len(df) > 0:
                    first_row = [str(v).strip() for v in df.iloc[0]]
                    col_map = self._map_columns(first_row)
                    if col_map:
                        df = df.iloc[1:]

                if not col_map:
                    continue

                # Create reverse map: field_name -> column_index
                idx_to_col = {idx: df.columns[idx] for idx in col_map if idx < len(df.columns)}

                for _, row in df.iterrows():
                    raw = {}
                    for idx, field_name in col_map.items():
                        if idx < len(df.columns):
                            val = row.iloc[idx]
                            raw[field_name] = str(val).strip() if pd.notna(val) else ""

                    drug_name = raw.get("drug_name", "")
                    if not drug_name or drug_name.upper() in ("NAN", ""):
                        continue

                    name_clean, strength = self._extract_strength(drug_name)

                    records.append({
                        "country_code": self.country_code,
                        "country_name": self.country_name,
                        "source": self.source_name,
                        "medicine_name": name_clean,
                        "active_substance": "",
                        "strength": strength,
                        "package_size": "",
                        "batch_no": raw.get("batch_no", ""),
                        "manufacturer": raw.get("manufacturer", ""),
                        "status": "NSQ",
                        "shortage_start": self._parse_date(raw.get("date_drawn")) or alert_date,
                        "estimated_end": None,
                        "reason": raw.get("reason", ""),
                        "testing_lab": raw.get("lab", ""),
                        "state": raw.get("state", ""),
                        "scraped_at": datetime.now().isoformat(),
                    })
        except ImportError:
            print("    tabula-py not installed, skipping PDF parsing")
        except Exception as e:
            print(f"    Error parsing PDF: {e}")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        return records

    def scrape(self) -> pd.DataFrame:
        """Scrape CDSCO NSQ drug data.

        Strategy:
        1. Try the structured NSQ drugs page (HTML tables)
        2. If no data from tables, try parsing recent NSQ alert PDFs (requires tabula-py)
        3. Always collect alert metadata from the Alerts page as baseline data
        """
        print(f"Scraping {self.country_name} ({self.source_name})...")
        all_records = []

        # Step 1: Try structured NSQ tables page
        table_records = self._scrape_nsq_tables()
        if table_records:
            all_records.extend(table_records)
            print(f"  Step 1: {len(table_records)} records from NSQ tables page")

        # Step 2: Try parsing recent NSQ alert PDFs for detailed drug data
        if not all_records:
            try:
                pdf_records = self._scrape_alert_pdf_links(max_pdfs=3)
                if pdf_records:
                    all_records.extend(pdf_records)
                    print(f"  Step 2: {len(pdf_records)} records from NSQ alert PDFs")
            except Exception as e:
                print(f"  Step 2: PDF parsing failed: {e}")

        # Step 3: Always collect alert-level metadata as baseline
        alert_records = self._scrape_alerts_page()
        if alert_records and not all_records:
            # Only use alert metadata if we have no detailed drug-level data
            all_records.extend(alert_records)
            print(f"  Step 3: {len(alert_records)} alert-level records (fallback)")
        elif alert_records:
            print(f"  Step 3: {len(alert_records)} alert entries found (not added, detailed data available)")

        if not all_records:
            print("  Warning: No NSQ data could be extracted from any source")
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

        print(f"  Total: {len(df)} NSQ records scraped")
        return df
