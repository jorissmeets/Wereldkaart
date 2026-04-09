"""Scraper for Philippines FDA drug shortage advisories and circulars."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class PhFdaScraper(BaseScraper):
    """Scraper for FDA Philippines drug shortage advisories.

    The Philippine FDA publishes drug-related circulars and advisories
    at https://www.fda.gov.ph/. This scraper collects advisories related
    to drug shortages, supply disruptions, and product unavailability
    from the FDA advisories listing pages.
    """

    ADVISORIES_URL = "https://www.fda.gov.ph/advisories/"
    # WordPress REST API endpoint for posts (advisories are posts)
    WP_API_URL = "https://www.fda.gov.ph/wp-json/wp/v2/posts"

    # Keywords that indicate a shortage-related advisory
    SHORTAGE_KEYWORDS = [
        "shortage",
        "short supply",
        "supply disruption",
        "unavailable",
        "unavailability",
        "out of stock",
        "limited supply",
        "supply issue",
        "supply concern",
        "supply problem",
        "stockout",
        "stock-out",
        "drug supply",
        "medicine supply",
        "product discontinuation",
        "discontinue",
        "recall",  # recalls can indicate supply impact
        "insufficient supply",
        "supply interruption",
        "kakulangan",  # Filipino for shortage
        "walang supply",  # Filipino for no supply
    ]

    # Patterns to extract medicine details from advisory text
    MEDICINE_PATTERNS = [
        # "Product Name (Active Substance) Strength"
        re.compile(
            r"(?:product|medicine|drug)\s*(?:name)?\s*[:\-–]\s*"
            r"(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        # Tabular data: look for rows with medicine info
        re.compile(
            r"(?:brand\s*name|product\s*name|generic\s*name)\s*[:\-–]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
    ]

    ACTIVE_SUBSTANCE_PATTERNS = [
        re.compile(
            r"(?:active\s*(?:substance|ingredient)|generic\s*name|INN)\s*[:\-–]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
    ]

    STRENGTH_PATTERNS = [
        re.compile(
            r"(?:strength|dosage|dose|concentration)\s*[:\-–]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        # Common strength patterns like "500mg", "10mg/ml"
        re.compile(
            r"(\d+\s*(?:mg|mcg|g|ml|iu|unit)(?:\s*/\s*\d*\s*(?:mg|mcg|g|ml|tab|cap))?)",
            re.IGNORECASE,
        ),
    ]

    def __init__(self):
        super().__init__(
            country_code="PH",
            country_name="Philippines",
            source_name="FDA",
            base_url="https://www.fda.gov.ph",
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
        """Parse dates from various formats used in Philippine FDA advisories."""
        if not val or (isinstance(val, float) and pd.isna(val)):
            return None
        val = str(val).strip()
        if not val or val == "-" or val.lower() == "nan":
            return None

        # Try various date formats
        for fmt in (
            "%B %d, %Y",       # January 15, 2024
            "%b %d, %Y",       # Jan 15, 2024
            "%d %B %Y",        # 15 January 2024
            "%d %b %Y",        # 15 Jan 2024
            "%m/%d/%Y",        # 01/15/2024
            "%d/%m/%Y",        # 15/01/2024
            "%Y-%m-%d",        # 2024-01-15
            "%Y-%m-%dT%H:%M:%S",  # ISO format from WP API
            "%B %Y",           # January 2024
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _is_shortage_related(self, title: str, content: str) -> bool:
        """Determine if an advisory is related to drug shortages."""
        combined = f"{title} {content}".lower()
        return any(kw in combined for kw in self.SHORTAGE_KEYWORDS)

    def _extract_medicine_name(self, text: str) -> str:
        """Extract medicine/product name from advisory text."""
        for pattern in self.MEDICINE_PATTERNS:
            m = pattern.search(text)
            if m:
                name = m.group(1).strip().rstrip(",;.")
                if name and len(name) >= 2:
                    return name
        return ""

    def _extract_active_substance(self, text: str) -> str:
        """Extract active substance/generic name from advisory text."""
        for pattern in self.ACTIVE_SUBSTANCE_PATTERNS:
            m = pattern.search(text)
            if m:
                substance = m.group(1).strip().rstrip(",;.")
                if substance and len(substance) >= 2:
                    return substance
        return ""

    def _extract_strength(self, text: str) -> str:
        """Extract dosage strength from advisory text."""
        for pattern in self.STRENGTH_PATTERNS:
            m = pattern.search(text)
            if m:
                strength = m.group(1).strip().rstrip(",;.")
                if strength:
                    return strength
        return ""

    def _extract_table_rows(self, soup: BeautifulSoup) -> list[dict]:
        """Extract medicine data from HTML tables within an advisory page.

        Many FDA PH advisories contain tables listing affected products.
        """
        records = []
        tables = soup.find_all("table")

        for table in tables:
            headers = []
            header_row = table.find("tr")
            if header_row:
                headers = [
                    th.get_text(strip=True).lower()
                    for th in header_row.find_all(["th", "td"])
                ]

            # Identify column indices
            name_idx = None
            substance_idx = None
            strength_idx = None
            status_idx = None

            for i, h in enumerate(headers):
                if any(kw in h for kw in ("product", "brand", "medicine", "drug")):
                    name_idx = i
                elif any(kw in h for kw in ("generic", "active", "ingredient", "inn", "substance")):
                    substance_idx = i
                elif any(kw in h for kw in ("strength", "dosage", "dose", "concentration")):
                    strength_idx = i
                elif any(kw in h for kw in ("status", "remark", "action", "availability")):
                    status_idx = i

            # Parse data rows
            data_rows = table.find_all("tr")[1:]  # skip header
            for tr in data_rows:
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if not cells or all(not c for c in cells):
                    continue

                record = {
                    "medicine_name": cells[name_idx] if name_idx is not None and name_idx < len(cells) else "",
                    "active_substance": cells[substance_idx] if substance_idx is not None and substance_idx < len(cells) else "",
                    "strength": cells[strength_idx] if strength_idx is not None and strength_idx < len(cells) else "",
                    "status": cells[status_idx] if status_idx is not None and status_idx < len(cells) else "",
                }

                # Only include rows that have at least a medicine name or substance
                if record["medicine_name"] or record["active_substance"]:
                    records.append(record)

        return records

    def _scrape_advisory_page(self, url: str) -> list[dict]:
        """Scrape a single advisory page for medicine shortage details."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Warning: Could not fetch {url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        # Try to find the main content area
        content_div = (
            soup.find("div", class_="entry-content")
            or soup.find("article")
            or soup.find("div", class_="post-content")
            or soup.find("main")
        )
        if not content_div:
            content_div = soup

        text = content_div.get_text("\n", strip=True)

        # First, try to extract structured table data
        table_records = self._extract_table_rows(content_div)
        if table_records:
            return table_records

        # Fall back to text extraction for unstructured advisories
        medicine_name = self._extract_medicine_name(text)
        active_substance = self._extract_active_substance(text)
        strength = self._extract_strength(text)

        # If no structured data found, create a single record from the advisory
        # using the title as medicine name if no explicit name found
        if medicine_name or active_substance:
            return [{
                "medicine_name": medicine_name,
                "active_substance": active_substance,
                "strength": strength,
                "status": "shortage",
            }]

        return []

    def _scrape_via_wp_api(self, max_pages: int = 20) -> list[dict]:
        """Scrape advisories using the WordPress REST API.

        The FDA PH site runs on WordPress, so the WP REST API is the
        most reliable way to fetch and paginate through advisories.
        """
        all_advisories = []
        page = 1

        while page <= max_pages:
            try:
                resp = self.session.get(
                    self.WP_API_URL,
                    params={
                        "per_page": 100,
                        "page": page,
                        "search": "shortage OR supply OR unavailable OR discontinu",
                        "orderby": "date",
                        "order": "desc",
                    },
                    timeout=30,
                )
                if resp.status_code == 400:
                    # No more pages
                    break
                resp.raise_for_status()
                posts = resp.json()
            except (requests.RequestException, ValueError) as e:
                print(f"    Warning: WP API page {page} failed: {e}")
                break

            if not posts:
                break

            for post in posts:
                title = post.get("title", {}).get("rendered", "")
                content_html = post.get("content", {}).get("rendered", "")
                link = post.get("link", "")
                date_str = post.get("date", "")

                # Strip HTML for keyword matching
                content_text = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True)

                if self._is_shortage_related(title, content_text):
                    all_advisories.append({
                        "title": BeautifulSoup(title, "lxml").get_text(strip=True),
                        "content_html": content_html,
                        "content_text": content_text,
                        "url": link,
                        "date": date_str,
                    })

            # Check if there are more pages
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break

            page += 1
            time.sleep(0.5)

        return all_advisories

    def _scrape_via_html(self, max_pages: int = 10) -> list[dict]:
        """Fallback: scrape advisories by crawling the HTML listing pages."""
        all_advisories = []

        for page_num in range(1, max_pages + 1):
            url = self.ADVISORIES_URL if page_num == 1 else f"{self.ADVISORIES_URL}page/{page_num}/"

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 404:
                    break  # no more pages
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"    Warning: Could not fetch listing page {page_num}: {e}")
                break

            soup = BeautifulSoup(resp.text, "lxml")

            # Find advisory links - typically in article or post listing elements
            articles = soup.find_all("article") or soup.find_all("div", class_=re.compile(r"post|entry"))
            if not articles:
                # Try finding links in the main content area
                main = soup.find("main") or soup.find("div", id="content") or soup
                articles = main.find_all("a", href=re.compile(r"fda\.gov\.ph/"))

            found_any = False
            for article in articles:
                # Get the link and title
                if article.name == "a":
                    link = article.get("href", "")
                    title = article.get_text(strip=True)
                else:
                    a_tag = article.find("a", href=True)
                    if not a_tag:
                        continue
                    link = a_tag.get("href", "")
                    title = a_tag.get_text(strip=True)

                if not link or not title:
                    continue

                link = urljoin(self.base_url, link)

                # Quick keyword check on title
                if self._is_shortage_related(title, ""):
                    # Extract date from article metadata if available
                    date_el = article.find("time") or article.find(class_=re.compile(r"date|time"))
                    date_str = ""
                    if date_el:
                        date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)

                    all_advisories.append({
                        "title": title,
                        "content_html": "",
                        "content_text": "",
                        "url": link,
                        "date": date_str,
                    })
                    found_any = True

            if not found_any and page_num > 1:
                break

            time.sleep(0.5)

        return all_advisories

    def _determine_status(self, title: str, text: str) -> str:
        """Determine the shortage status from advisory content."""
        combined = f"{title} {text}".lower()
        if any(kw in combined for kw in ("resolved", "resumed", "restored", "available again", "back in stock")):
            return "resolved"
        if any(kw in combined for kw in ("discontinu", "permanently")):
            return "discontinued"
        if any(kw in combined for kw in ("recall", "withdraw")):
            return "recalled"
        if any(kw in combined for kw in ("limited", "reduced")):
            return "limited_supply"
        return "shortage"

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Try WordPress REST API first (more structured, better pagination)
        print("  Attempting WordPress REST API...")
        advisories = self._scrape_via_wp_api(max_pages=20)

        if not advisories:
            # Fall back to HTML scraping
            print("  WP API returned no results, falling back to HTML scraping...")
            advisories = self._scrape_via_html(max_pages=10)

        print(f"  Found {len(advisories)} shortage-related advisories")

        records = []
        for i, adv in enumerate(advisories):
            title = adv["title"]
            content_html = adv.get("content_html", "")
            content_text = adv.get("content_text", "")
            advisory_url = adv.get("url", "")
            advisory_date = self._parse_date(adv.get("date", ""))

            # If we have the content already (from WP API), parse it directly
            if content_html:
                soup = BeautifulSoup(content_html, "lxml")
                table_records = self._extract_table_rows(soup)

                if table_records:
                    for rec in table_records:
                        status = rec.get("status", "") or self._determine_status(title, content_text)
                        records.append({
                            "country_code": self.country_code,
                            "country_name": self.country_name,
                            "source": self.source_name,
                            "medicine_name": rec.get("medicine_name", ""),
                            "active_substance": rec.get("active_substance", ""),
                            "strength": rec.get("strength", ""),
                            "package_size": "",
                            "status": status,
                            "shortage_start": advisory_date,
                            "estimated_end": None,
                            "advisory_title": title,
                            "advisory_url": advisory_url,
                            "advisory_date": advisory_date,
                            "scraped_at": datetime.now().isoformat(),
                        })
                    continue

                # Try text-based extraction
                medicine_name = self._extract_medicine_name(content_text)
                active_substance = self._extract_active_substance(content_text)
                strength = self._extract_strength(content_text)

                if medicine_name or active_substance:
                    records.append({
                        "country_code": self.country_code,
                        "country_name": self.country_name,
                        "source": self.source_name,
                        "medicine_name": medicine_name,
                        "active_substance": active_substance,
                        "strength": strength,
                        "package_size": "",
                        "status": self._determine_status(title, content_text),
                        "shortage_start": advisory_date,
                        "estimated_end": None,
                        "advisory_title": title,
                        "advisory_url": advisory_url,
                        "advisory_date": advisory_date,
                        "scraped_at": datetime.now().isoformat(),
                    })
                    continue

            # If no content yet (HTML scraping path) or no structured data found,
            # fetch the individual advisory page
            if advisory_url and not content_html:
                print(f"    Fetching advisory {i + 1}/{len(advisories)}: {title[:60]}...")
                page_records = self._scrape_advisory_page(advisory_url)

                if page_records:
                    for rec in page_records:
                        status = rec.get("status", "") or self._determine_status(title, "")
                        records.append({
                            "country_code": self.country_code,
                            "country_name": self.country_name,
                            "source": self.source_name,
                            "medicine_name": rec.get("medicine_name", ""),
                            "active_substance": rec.get("active_substance", ""),
                            "strength": rec.get("strength", ""),
                            "package_size": "",
                            "status": status,
                            "shortage_start": advisory_date,
                            "estimated_end": None,
                            "advisory_title": title,
                            "advisory_url": advisory_url,
                            "advisory_date": advisory_date,
                            "scraped_at": datetime.now().isoformat(),
                        })
                    time.sleep(0.5)
                    continue

                time.sleep(0.5)

            # Last resort: record the advisory itself with title as medicine name
            # This ensures we do not lose track of shortage advisories even when
            # structured extraction fails
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": title,
                "active_substance": "",
                "strength": "",
                "package_size": "",
                "status": self._determine_status(title, content_text),
                "shortage_start": advisory_date,
                "estimated_end": None,
                "advisory_title": title,
                "advisory_url": advisory_url,
                "advisory_date": advisory_date,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)

        if df.empty:
            print("  No shortage records found. The FDA PH website may have changed structure.")
            # Return empty DataFrame with expected columns
            df = pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "status", "shortage_start", "estimated_end",
                "advisory_title", "advisory_url", "advisory_date",
                "scraped_at",
            ])
        else:
            # Deduplicate by medicine name + advisory URL
            before = len(df)
            df = df.drop_duplicates(
                subset=["medicine_name", "advisory_url"],
                keep="first",
            )
            if len(df) < before:
                print(f"  Deduplicated: {before} -> {len(df)} records")

        print(f"  Total: {len(df)} shortage records scraped")
        return df
