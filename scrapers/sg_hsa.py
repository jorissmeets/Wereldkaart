"""Scraper for Singapore HSA (Health Sciences Authority) drug shortage data."""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class SgHsaScraper(BaseScraper):
    """Scraper for HSA (Singapore) drug shortage and supply disruption notices.

    The Singapore Health Sciences Authority publishes drug shortage and
    supply disruption information through announcements on their website
    at https://www.hsa.gov.sg/announcements. The site uses Telerik
    Sitefinity CMS which renders content dynamically.

    This scraper:
    1. Fetches the HSA announcements listing pages (DHCPL and regulatory updates)
    2. Filters for shortage/supply-related announcements by keyword
    3. Scrapes individual announcement detail pages for medicine data
    4. Extracts medicine names, active substances, strength, and dates
    """

    BASE_URL = "https://www.hsa.gov.sg"

    # Announcement listing pages to check
    LISTING_URLS = [
        "https://www.hsa.gov.sg/announcements/dear-healthcare-professional-letter",
        "https://www.hsa.gov.sg/announcements/regulatory-update",
        "https://www.hsa.gov.sg/announcements/safety-alert",
    ]

    # Keywords that indicate a shortage or supply disruption announcement
    SHORTAGE_KEYWORDS = [
        "shortage",
        "short supply",
        "supply disruption",
        "supply issue",
        "supply interruption",
        "unavailable",
        "unavailability",
        "out of stock",
        "limited supply",
        "insufficient supply",
        "stockout",
        "stock-out",
        "discontinu",
        "supply affected",
        "supply impact",
        "temporary unavailability",
        "product recall",
    ]

    # Regex patterns to extract medicine details from announcement text
    MEDICINE_PATTERNS = [
        re.compile(
            r"(?:product|medicine|drug|brand)\s*(?:name)?\s*[:\-–]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:the\s+following\s+(?:product|medicine|drug)s?\s*[:\-–]\s*)(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
    ]

    ACTIVE_SUBSTANCE_PATTERNS = [
        re.compile(
            r"(?:active\s*(?:substance|ingredient)|generic\s*name|INN)\s*[:\-–]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:containing|contains)\s+(.+?)(?:\s+(?:is|are|has|have|will)\b)",
            re.IGNORECASE,
        ),
    ]

    STRENGTH_PATTERNS = [
        re.compile(
            r"(?:strength|dosage|dose|concentration)\s*[:\-–]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(\d+\s*(?:mg|mcg|g|ml|iu|units?)(?:\s*/\s*\d*\s*(?:mg|mcg|g|ml|tab|cap|vial))?)",
            re.IGNORECASE,
        ),
    ]

    # Date patterns common in HSA announcements
    DATE_PATTERNS = [
        re.compile(
            r"(?:from|since|starting|effective|start\s*date)\s*[:\-–]?\s*"
            r"(\d{1,2}\s+\w+\s+\d{4})",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:until|end\s*date|expected\s*(?:to\s*)?resol(?:ve|ution)|resume)\s*[:\-–]?\s*"
            r"(\d{1,2}\s+\w+\s+\d{4})",
            re.IGNORECASE,
        ),
    ]

    MAX_LISTING_PAGES = 10
    REQUEST_DELAY = 0.5  # seconds between requests for rate limiting

    def __init__(self):
        super().__init__(
            country_code="SG",
            country_name="Singapore",
            source_name="HSA",
            base_url="https://www.hsa.gov.sg",
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
        """Parse dates from various formats used in HSA announcements."""
        if not val or (isinstance(val, float) and pd.isna(val)):
            return None
        val = str(val).strip()
        if not val or val == "-" or val.lower() == "nan":
            return None

        for fmt in (
            "%d %B %Y",       # 15 January 2024
            "%d %b %Y",       # 15 Jan 2024
            "%B %d, %Y",      # January 15, 2024
            "%b %d, %Y",      # Jan 15, 2024
            "%d/%m/%Y",       # 15/01/2024
            "%m/%d/%Y",       # 01/15/2024
            "%Y-%m-%d",       # 2024-01-15
            "%d-%m-%Y",       # 15-01-2024
            "%d %B, %Y",      # 15 January, 2024
            "%B %Y",          # January 2024
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _is_shortage_related(self, title: str, content: str = "") -> bool:
        """Determine if an announcement is related to drug shortages."""
        combined = f"{title} {content}".lower()
        return any(kw in combined for kw in self.SHORTAGE_KEYWORDS)

    def _extract_medicine_name(self, text: str) -> str:
        """Extract medicine/product name from announcement text."""
        for pattern in self.MEDICINE_PATTERNS:
            m = pattern.search(text)
            if m:
                name = m.group(1).strip().rstrip(",;.")
                if name and len(name) >= 2:
                    return name
        return ""

    def _extract_active_substance(self, text: str) -> str:
        """Extract active substance from announcement text."""
        for pattern in self.ACTIVE_SUBSTANCE_PATTERNS:
            m = pattern.search(text)
            if m:
                substance = m.group(1).strip().rstrip(",;.")
                if substance and len(substance) >= 2:
                    return substance
        return ""

    def _extract_strength(self, text: str) -> str:
        """Extract dosage strength from announcement text."""
        for pattern in self.STRENGTH_PATTERNS:
            m = pattern.search(text)
            if m:
                strength = m.group(1).strip().rstrip(",;.")
                if strength:
                    return strength
        return ""

    def _extract_shortage_dates(self, text: str) -> tuple[str | None, str | None]:
        """Extract shortage start and estimated end dates from text."""
        shortage_start = None
        estimated_end = None

        # Look for start date
        start_pattern = re.compile(
            r"(?:from|since|starting|effective|start\s*(?:date)?)\s*[:\-–]?\s*"
            r"(\d{1,2}\s+\w+\s+\d{4})",
            re.IGNORECASE,
        )
        m = start_pattern.search(text)
        if m:
            shortage_start = self._parse_date(m.group(1))

        # Look for end date
        end_pattern = re.compile(
            r"(?:until|end\s*(?:date)?|expected\s*(?:to\s*)?(?:resol(?:ve|ution)|resume|restor)|"
            r"anticipated\s*(?:to\s*)?(?:resol|resume|restor)|available\s*(?:again\s*)?(?:by|from))\s*"
            r"[:\-–]?\s*(\d{1,2}\s+\w+\s+\d{4})",
            re.IGNORECASE,
        )
        m = end_pattern.search(text)
        if m:
            estimated_end = self._parse_date(m.group(1))

        return shortage_start, estimated_end

    def _extract_table_rows(self, soup: BeautifulSoup) -> list[dict]:
        """Extract medicine data from HTML tables within an announcement page.

        HSA announcements sometimes contain tables listing affected products.
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
                if any(kw in h for kw in ("product", "brand", "medicine", "drug", "trade")):
                    name_idx = i
                elif any(kw in h for kw in ("generic", "active", "ingredient", "inn", "substance")):
                    substance_idx = i
                elif any(kw in h for kw in ("strength", "dosage", "dose", "concentration")):
                    strength_idx = i
                elif any(kw in h for kw in ("status", "remark", "action", "availability")):
                    status_idx = i

            # Parse data rows (skip header)
            data_rows = table.find_all("tr")[1:]
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

                if record["medicine_name"] or record["active_substance"]:
                    records.append(record)

        return records

    def _scrape_announcement_page(self, url: str) -> tuple[list[dict], str]:
        """Scrape a single announcement detail page for medicine shortage details.

        Returns a tuple of (records, page_text).
        """
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Warning: Could not fetch {url}: {e}")
            return [], ""

        soup = BeautifulSoup(resp.text, "lxml")

        # Try to find the main content area
        content_div = (
            soup.find("div", class_=re.compile(r"sf-content|sfContentBlock|content-area|main-content"))
            or soup.find("div", class_="content")
            or soup.find("article")
            or soup.find("main")
            or soup.find("div", id="content")
        )
        if not content_div:
            content_div = soup

        text = content_div.get_text("\n", strip=True)

        # First, try to extract structured table data
        table_records = self._extract_table_rows(content_div)
        if table_records:
            return table_records, text

        # Fall back to text extraction for unstructured announcements
        medicine_name = self._extract_medicine_name(text)
        active_substance = self._extract_active_substance(text)
        strength = self._extract_strength(text)

        if medicine_name or active_substance:
            return [{
                "medicine_name": medicine_name,
                "active_substance": active_substance,
                "strength": strength,
                "status": "shortage",
            }], text

        return [], text

    def _determine_status(self, title: str, text: str) -> str:
        """Determine the shortage status from announcement content."""
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

    def _extract_title_medicine(self, title: str) -> tuple[str, str]:
        """Try to extract medicine name and substance from the announcement title.

        HSA titles often follow patterns like:
        - "Supply Disruption of Product Name (Substance) Strength"
        - "Shortage of Product Name Tablets/Capsules"
        - "Discontinuation of Product Name"
        """
        # Pattern: "Supply Disruption / Shortage / Discontinuation of <product>"
        m = re.search(
            r"(?:supply\s*disruption|shortage|discontinuation|unavailability|recall)\s+(?:of\s+)?(.+)",
            title,
            re.IGNORECASE,
        )
        if m:
            product_part = m.group(1).strip()
            # Try to split "Product Name (Substance) Strength"
            paren_match = re.match(r"(.+?)\s*\(([^)]+)\)\s*(.*)", product_part)
            if paren_match:
                name = paren_match.group(1).strip()
                substance = paren_match.group(2).strip()
                return name, substance
            return product_part, ""

        return "", ""

    def _scrape_listing_page(self, url: str) -> list[dict]:
        """Scrape a single listing page for announcement entries.

        Parses the HTML to find announcement links and metadata.
        Returns a list of dicts with keys: title, url, date.
        """
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Warning: Could not fetch listing page {url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        announcements = []

        # Strategy 1: Look for announcement items in common list/card patterns
        # Sitefinity often renders lists in divs with specific classes
        items = soup.find_all("div", class_=re.compile(
            r"sf-list|sflistItem|news-item|announcement-item|list-item|card|result-item"
        ))

        # Strategy 2: Look for <li> elements with links
        if not items:
            items = soup.find_all("li", class_=re.compile(
                r"sf-list|item|announcement|news|result"
            ))

        # Strategy 3: Look for all links in the main content area that go to HSA pages
        if not items:
            main_content = (
                soup.find("div", class_=re.compile(r"sf-content|main-content|content-area"))
                or soup.find("main")
                or soup.find("div", id="content")
                or soup
            )
            # Find links that look like announcement detail pages
            for a in main_content.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if not text or len(text) < 10:
                    continue
                # Filter for links that look like announcements
                if href.startswith("/") or "hsa.gov.sg" in href:
                    full_url = urljoin(self.BASE_URL, href)
                    if "/announcements/" in full_url or "/news/" in full_url:
                        # Look for a date near the link
                        parent = a.parent
                        date_str = ""
                        if parent:
                            date_el = parent.find(
                                class_=re.compile(r"date|time|published")
                            ) or parent.find("time")
                            if date_el:
                                date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)

                        announcements.append({
                            "title": text,
                            "url": full_url,
                            "date": date_str,
                        })
            return announcements

        # Process items found by Strategy 1 or 2
        for item in items:
            a_tag = item.find("a", href=True)
            if not a_tag:
                continue

            href = a_tag["href"]
            title = a_tag.get_text(strip=True)
            if not title:
                continue

            full_url = urljoin(self.BASE_URL, href)

            # Look for date
            date_str = ""
            date_el = item.find(class_=re.compile(r"date|time|published")) or item.find("time")
            if date_el:
                date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)

            announcements.append({
                "title": title,
                "url": full_url,
                "date": date_str,
            })

        return announcements

    def _scrape_all_listings(self) -> list[dict]:
        """Scrape all announcement listing pages and collect shortage-related entries."""
        all_announcements = []
        seen_urls = set()

        for listing_url in self.LISTING_URLS:
            print(f"  Checking listing: {listing_url}")

            for page_num in range(1, self.MAX_LISTING_PAGES + 1):
                # Sitefinity pagination typically uses ?page=N
                if page_num == 1:
                    url = listing_url
                else:
                    sep = "&" if "?" in listing_url else "?"
                    url = f"{listing_url}{sep}page={page_num}"

                entries = self._scrape_listing_page(url)

                if not entries:
                    if page_num > 1:
                        break
                    # Even page 1 may have no entries if JS-rendered
                    continue

                found_shortage = False
                for entry in entries:
                    entry_url = entry["url"]
                    if entry_url in seen_urls:
                        continue
                    seen_urls.add(entry_url)

                    title = entry["title"]
                    if self._is_shortage_related(title):
                        all_announcements.append(entry)
                        found_shortage = True

                # If no shortage-related entries on this page and we are past
                # the first page, stop paginating this listing
                if not found_shortage and page_num > 2:
                    break

                time.sleep(self.REQUEST_DELAY)

        return all_announcements

    def _scrape_search_results(self) -> list[dict]:
        """Alternative approach: use site search for shortage-related announcements.

        The HSA site may provide search functionality that returns results
        as server-rendered HTML.
        """
        search_terms = [
            "drug shortage",
            "supply disruption",
            "medicine shortage",
            "product discontinuation",
        ]

        all_results = []
        seen_urls = set()

        for term in search_terms:
            search_url = f"{self.BASE_URL}/search-results?searchText={term.replace(' ', '+')}"
            try:
                resp = self.session.get(search_url, timeout=30)
                if resp.status_code != 200:
                    continue
            except requests.RequestException:
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Look for search result items
            result_items = soup.find_all("div", class_=re.compile(
                r"search-result|result-item|sf-search"
            ))

            if not result_items:
                # Try finding links in the main content
                main = soup.find("main") or soup.find("div", id="content") or soup
                for a in main.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if not text or len(text) < 10:
                        continue
                    full_url = urljoin(self.BASE_URL, href)
                    if full_url in seen_urls:
                        continue
                    if self._is_shortage_related(text):
                        seen_urls.add(full_url)
                        all_results.append({
                            "title": text,
                            "url": full_url,
                            "date": "",
                        })
            else:
                for item in result_items:
                    a_tag = item.find("a", href=True)
                    if not a_tag:
                        continue
                    href = a_tag["href"]
                    title = a_tag.get_text(strip=True)
                    full_url = urljoin(self.BASE_URL, href)
                    if full_url in seen_urls:
                        continue
                    if self._is_shortage_related(title):
                        seen_urls.add(full_url)
                        date_el = item.find(class_=re.compile(r"date|time"))
                        date_str = ""
                        if date_el:
                            date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)
                        all_results.append({
                            "title": title,
                            "url": full_url,
                            "date": date_str,
                        })

            time.sleep(self.REQUEST_DELAY)

        return all_results

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Approach 1: Scrape announcement listing pages
        print("  Scraping announcement listing pages...")
        announcements = self._scrape_all_listings()
        print(f"  Found {len(announcements)} shortage-related announcements from listings")

        # Approach 2: If listings yielded few results, also try search
        if len(announcements) < 5:
            print("  Supplementing with search results...")
            search_results = self._scrape_search_results()
            # Merge, avoiding duplicates
            seen_urls = {a["url"] for a in announcements}
            for result in search_results:
                if result["url"] not in seen_urls:
                    announcements.append(result)
                    seen_urls.add(result["url"])
            print(f"  Total announcements after search: {len(announcements)}")

        if not announcements:
            print("  Warning: No shortage announcements found. The HSA website may "
                  "use dynamic rendering that requires JavaScript execution.")
            print("  Returning empty DataFrame with expected columns.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "status", "shortage_start", "estimated_end",
                "announcement_title", "announcement_url", "announcement_date",
                "scraped_at",
            ])

        # Scrape individual announcement pages for medicine details
        print(f"  Scraping {len(announcements)} announcement detail pages...")
        records = []

        for i, ann in enumerate(announcements):
            title = ann["title"]
            ann_url = ann["url"]
            ann_date = self._parse_date(ann.get("date", ""))

            if (i + 1) % 10 == 0:
                print(f"    ... {i + 1}/{len(announcements)} pages processed")

            # Try to extract from the detail page
            page_records, page_text = self._scrape_announcement_page(ann_url)

            # Extract dates from the page text
            shortage_start, estimated_end = None, None
            if page_text:
                shortage_start, estimated_end = self._extract_shortage_dates(page_text)

            # Use announcement date as fallback for shortage_start
            if not shortage_start and ann_date:
                shortage_start = ann_date

            if page_records:
                for rec in page_records:
                    status = rec.get("status", "") or self._determine_status(title, page_text)
                    records.append({
                        "country_code": self.country_code,
                        "country_name": self.country_name,
                        "source": self.source_name,
                        "medicine_name": rec.get("medicine_name", ""),
                        "active_substance": rec.get("active_substance", ""),
                        "strength": rec.get("strength", ""),
                        "package_size": "",
                        "status": status,
                        "shortage_start": shortage_start,
                        "estimated_end": estimated_end,
                        "announcement_title": title,
                        "announcement_url": ann_url,
                        "announcement_date": ann_date,
                        "scraped_at": datetime.now().isoformat(),
                    })
            else:
                # Try to extract medicine info from the title itself
                title_medicine, title_substance = self._extract_title_medicine(title)
                status = self._determine_status(title, page_text)

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": title_medicine or title,
                    "active_substance": title_substance,
                    "strength": "",
                    "package_size": "",
                    "status": status,
                    "shortage_start": shortage_start,
                    "estimated_end": estimated_end,
                    "announcement_title": title,
                    "announcement_url": ann_url,
                    "announcement_date": ann_date,
                    "scraped_at": datetime.now().isoformat(),
                })

            time.sleep(self.REQUEST_DELAY)

        df = pd.DataFrame(records)

        if not df.empty:
            # Deduplicate by medicine name + announcement URL
            before = len(df)
            df = df.drop_duplicates(
                subset=["medicine_name", "announcement_url"],
                keep="first",
            )
            if len(df) < before:
                print(f"  Deduplicated: {before} -> {len(df)} records")

        print(f"  Total: {len(df)} shortage records scraped")
        return df
