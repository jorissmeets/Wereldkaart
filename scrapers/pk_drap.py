"""Scraper for Pakistan DRAP (Drug Regulatory Authority of Pakistan) drug shortage data."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class PkDrapScraper(BaseScraper):
    """Scraper for DRAP (Pakistan) drug shortage / availability data.

    DRAP publishes drug shortage and supply disruption information on their
    website. This scraper checks multiple known URL patterns for shortage
    listings and parses HTML tables or structured page content.
    """

    # Primary and fallback URLs for shortage data
    SHORTAGE_URLS = [
        "https://www.drap.gov.pk/drug-shortage",
        "https://www.drap.gov.pk/drug-shortages",
        "https://www.drap.gov.pk/shortage",
        "https://www.drap.gov.pk/medicine-shortage",
        "https://www.drap.gov.pk/publications/drug-shortage",
        "https://www.drap.gov.pk/notifications/drug-shortage",
    ]

    # Fallback: search the main site for shortage-related links
    SEARCH_URLS = [
        "https://www.drap.gov.pk",
        "https://www.drap.gov.pk/publications",
        "https://www.drap.gov.pk/notifications",
    ]

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self):
        super().__init__(
            country_code="PK",
            country_name="Pakistan",
            source_name="DRAP",
            base_url="https://www.drap.gov.pk",
        )

    def _parse_date(self, val) -> str | None:
        """Parse a date string into ISO format, handling multiple formats."""
        if pd.isna(val) or not val:
            return None
        val = str(val).strip()
        if not val or val == "-" or val.lower() in ("n/a", "na", "unknown", "tbd"):
            return None
        for fmt in (
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d %B %Y",
            "%d %b %Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d-%b-%Y",
            "%d-%B-%Y",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _fetch_page(self, url: str) -> requests.Response | None:
        """Fetch a page with error handling. Returns None on failure."""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp
        except requests.RequestException as e:
            print(f"    Request failed for {url}: {e}")
        return None

    def _find_shortage_page(self) -> tuple[str, BeautifulSoup] | None:
        """Try known shortage URLs, then search the main site for links."""
        # Try direct shortage URLs
        for url in self.SHORTAGE_URLS:
            print(f"    Trying: {url}")
            resp = self._fetch_page(url)
            if resp:
                soup = BeautifulSoup(resp.text, "lxml")
                # Check if the page has meaningful content (not just a 404 page)
                text = soup.get_text(strip=True).lower()
                if "shortage" in text or "medicine" in text or "drug" in text:
                    print(f"    Found shortage page: {url}")
                    return url, soup

        # Search the main pages for links containing shortage-related keywords
        print("    Direct URLs failed, searching main site for shortage links...")
        for search_url in self.SEARCH_URLS:
            resp = self._fetch_page(search_url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                link_text = a.get_text(strip=True).lower()
                if any(kw in href or kw in link_text for kw in
                       ("shortage", "unavailab", "supply disruption", "out of stock")):
                    full_url = urljoin(search_url, a["href"])
                    print(f"    Found shortage link: {full_url}")
                    resp2 = self._fetch_page(full_url)
                    if resp2:
                        return full_url, BeautifulSoup(resp2.text, "lxml")

        return None

    def _parse_html_table(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Extract shortage records from HTML tables on the page."""
        records = []
        tables = soup.find_all("table")
        if not tables:
            print("    No HTML tables found on page")
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
                if any(kw in h_clean for kw in ("medicine name", "product name", "drug name", "brand name", "name of medicine", "name of drug")):
                    col_map["medicine_name"] = i
                elif any(kw in h_clean for kw in ("active substance", "active ingredient", "generic name", "inn", "salt", "molecule")):
                    col_map["active_substance"] = i
                elif any(kw in h_clean for kw in ("strength", "dose", "dosage", "concentration")):
                    col_map["strength"] = i
                elif any(kw in h_clean for kw in ("status", "availability", "current status")):
                    col_map["status"] = i
                elif any(kw in h_clean for kw in ("start date", "shortage start", "date of shortage", "shortage date", "reported date", "date reported")):
                    col_map["shortage_start"] = i
                elif any(kw in h_clean for kw in ("end date", "estimated end", "expected date", "resolution date", "estimated resolution", "expected availability")):
                    col_map["estimated_end"] = i
                elif any(kw in h_clean for kw in ("manufacturer", "company", "mah", "marketing auth")):
                    col_map["manufacturer"] = i
                elif any(kw in h_clean for kw in ("reason", "cause")):
                    col_map["reason"] = i
                elif any(kw in h_clean for kw in ("registration", "reg. no", "reg no")):
                    col_map["registration_no"] = i
                elif any(kw in h_clean for kw in ("dosage form", "form", "formulation")):
                    col_map["dosage_form"] = i
                elif any(kw in h_clean for kw in ("pack", "package")):
                    col_map["package_size"] = i
                elif any(kw in h_clean for kw in ("s.no", "s. no", "sr", "sr.", "#", "no.")):
                    col_map["serial_no"] = i

            # If no medicine_name column found, try to use the first
            # non-serial text column
            if "medicine_name" not in col_map and len(headers) >= 2:
                for i, h in enumerate(headers):
                    if i not in col_map.values() and "no" not in h and "#" not in h:
                        col_map["medicine_name"] = i
                        break

            if "medicine_name" not in col_map:
                continue

            print(f"    Table {table_idx + 1}: {len(rows) - 1} data rows, columns mapped: {list(col_map.keys())}")

            # Parse data rows
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue

                cell_texts = [c.get_text(strip=True) for c in cells]

                # Skip rows that are too short or appear to be sub-headers
                if len(cell_texts) <= col_map.get("medicine_name", 0):
                    continue

                medicine = cell_texts[col_map["medicine_name"]] if col_map.get("medicine_name") is not None and col_map["medicine_name"] < len(cell_texts) else ""
                if not medicine or medicine.lower() in ("", "nan", "-"):
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
                    "registration_no": _get("registration_no"),
                    "reason": _get("reason"),
                    "status": status,
                    "shortage_start": self._parse_date(_get("shortage_start")),
                    "estimated_end": self._parse_date(_get("estimated_end")),
                    "source_url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        return records

    def _parse_list_items(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        """Fallback: extract shortage records from list/div-based layouts."""
        records = []

        # Look for structured content in divs, articles, or list items
        containers = soup.find_all(["article", "div", "li"], class_=re.compile(
            r"(shortage|drug|medicine|item|entry|post|card)", re.I
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
                "registration_no": "",
                "reason": "",
                "status": "shortage",
                "shortage_start": None,
                "estimated_end": None,
                "source_url": page_url,
                "scraped_at": datetime.now().isoformat(),
            })

        return records

    def _check_pagination(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Find additional pages if the listing is paginated."""
        next_pages = []
        # Common pagination patterns
        pager = soup.find(["nav", "div", "ul"], class_=re.compile(
            r"(pager|pagination|page-nav)", re.I
        ))
        if pager:
            for a in pager.find_all("a", href=True):
                href = urljoin(base_url, a["href"])
                if href not in next_pages and href != base_url:
                    next_pages.append(href)

        # Also check for "next" links
        for a in soup.find_all("a", href=True):
            link_text = a.get_text(strip=True).lower()
            classes = " ".join(a.get("class", []))
            if "next" in link_text or "next" in classes:
                href = urljoin(base_url, a["href"])
                if href not in next_pages:
                    next_pages.append(href)

        return next_pages[:10]  # Limit to avoid infinite loops

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        result = self._find_shortage_page()
        if not result:
            print("  WARNING: Could not find DRAP shortage page.")
            print("  DRAP may have changed their URL structure.")
            print("  Returning empty DataFrame. Manual URL verification needed.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "dosage_form", "manufacturer",
                "registration_no", "reason", "status",
                "shortage_start", "estimated_end",
                "source_url", "scraped_at",
            ])

        page_url, soup = result

        # Try table-based parsing first
        all_records = self._parse_html_table(soup, page_url)

        # If no tables found, try list/div parsing
        if not all_records:
            print("    No table records found, trying list-based parsing...")
            all_records = self._parse_list_items(soup, page_url)

        # Check for pagination and scrape additional pages
        next_pages = self._check_pagination(soup, page_url)
        if next_pages:
            print(f"    Found {len(next_pages)} additional pages")
        for extra_url in next_pages:
            resp = self._fetch_page(extra_url)
            if not resp:
                continue
            extra_soup = BeautifulSoup(resp.text, "lxml")
            extra_records = self._parse_html_table(extra_soup, extra_url)
            if not extra_records:
                extra_records = self._parse_list_items(extra_soup, extra_url)
            all_records.extend(extra_records)

        df = pd.DataFrame(all_records)

        if df.empty:
            print("  WARNING: No shortage records found on the DRAP page.")
            print("  The page structure may have changed or no shortages are listed.")
        else:
            # Deduplicate based on medicine_name + strength
            before = len(df)
            dedup_cols = ["medicine_name", "strength"]
            available_dedup = [c for c in dedup_cols if c in df.columns]
            if available_dedup:
                df = df.drop_duplicates(subset=available_dedup, keep="first")
            if len(df) < before:
                print(f"    Removed {before - len(df)} duplicate records")

        print(f"  Total: {len(df)} shortage records scraped")
        return df
