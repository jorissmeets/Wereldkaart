"""Scraper for South Africa SAHPRA (South African Health Products Regulatory Authority) medicine shortage data."""

import io
import re
import time
import tempfile
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class ZaSahpraScraper(BaseScraper):
    """Scraper for SAHPRA (South Africa) medicine shortage communications.

    SAHPRA does not maintain a centralised, structured shortage database.
    Instead, medicine shortage and supply-disruption information is published
    as news articles, press releases, and PDF notices on their WordPress-based
    website.  This scraper:

    1. Queries the WordPress REST API for shortage-related posts.
    2. Scrapes the product-recalls page (recalls can affect supply).
    3. Parses HTML content of individual posts for medicine details.
    4. Follows links to PDF notices and extracts tabular data with pdfplumber.

    Scrapeerbaarheid: 2 stars -- data is unstructured and scattered across
    multiple page types and PDF documents.
    """

    BASE_URL = "https://www.sahpra.org.za"
    WP_API_POSTS = "https://www.sahpra.org.za/wp-json/wp/v2/posts"
    WP_API_PAGES = "https://www.sahpra.org.za/wp-json/wp/v2/pages"
    PRODUCT_RECALLS_URL = "https://www.sahpra.org.za/product-recalls/"

    # WordPress category IDs observed on the live site
    # (News & Updates, Press Releases, Safety Alerts, Product Recalls, Communications)
    CATEGORY_SLUGS = [
        "news-and-updates",
        "press-releases",
        "safety-alerts",
        "safety-alerts-1",
        "product-recalls",
        "communications",
    ]

    # Keywords that signal a shortage / supply-disruption communication
    SHORTAGE_KEYWORDS = [
        "shortage",
        "short supply",
        "supply disruption",
        "supply challenge",
        "supply issue",
        "supply problem",
        "stock-out",
        "stockout",
        "out of stock",
        "unavailable",
        "unavailability",
        "limited supply",
        "limited availability",
        "discontinu",
        "supply interrupt",
        "medicine access",
        "medicine availability",
        "recall",
    ]

    # Search terms sent to WP REST API -- keep short to avoid query limits
    WP_SEARCH_TERMS = [
        "shortage",
        "supply",
        "recall",
        "unavailable",
        "discontinu",
        "stock out",
    ]

    # Regex patterns to extract structured medicine info from free text
    MEDICINE_PATTERNS = [
        re.compile(
            r"(?:product|medicine|drug|trade)\s*(?:name)?\s*[:\-\u2013]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:brand\s*name)\s*[:\-\u2013]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
    ]

    ACTIVE_SUBSTANCE_PATTERNS = [
        re.compile(
            r"(?:active\s*(?:substance|ingredient)|generic\s*name|INN)\s*[:\-\u2013]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
    ]

    STRENGTH_PATTERNS = [
        re.compile(
            r"(?:strength|dosage|dose|concentration)\s*[:\-\u2013]\s*(.+?)(?:\n|$)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(\d+\s*(?:mg|mcg|g|ml|iu|units?)(?:\s*/\s*\d*\s*(?:mg|mcg|g|ml|tab|cap))?)",
            re.IGNORECASE,
        ),
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

    # Rate limiting: seconds between requests
    REQUEST_DELAY = 1.0

    def __init__(self):
        super().__init__(
            country_code="ZA",
            country_name="South Africa",
            source_name="SAHPRA",
            base_url=self.BASE_URL,
        )
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._last_request_time = 0.0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _rate_limit(self):
        """Enforce minimum delay between HTTP requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()

    def _fetch(self, url: str, **kwargs) -> requests.Response | None:
        """GET request with rate limiting and error handling."""
        self._rate_limit()
        try:
            resp = self.session.get(url, timeout=30, **kwargs)
            if resp.status_code == 200:
                return resp
            print(f"    HTTP {resp.status_code} for {url}")
        except requests.RequestException as e:
            print(f"    Request failed for {url}: {e}")
        return None

    def _parse_date(self, val) -> str | None:
        """Parse a date string into ISO format."""
        if not val or (isinstance(val, float) and pd.isna(val)):
            return None
        val = str(val).strip()
        if not val or val.lower() in ("", "-", "n/a", "nan", "unknown", "tbd", "tba"):
            return None
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",   # ISO from WP API
            "%Y-%m-%d",
            "%d %B %Y",            # 15 January 2024
            "%d %b %Y",            # 15 Jan 2024
            "%B %d, %Y",           # January 15, 2024
            "%b %d, %Y",           # Jan 15, 2024
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Fallback: pandas parser
        try:
            parsed = pd.to_datetime(val, dayfirst=True)
            if pd.notna(parsed):
                return parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        return None

    def _is_shortage_related(self, title: str, content: str = "") -> bool:
        """Return True if text matches shortage/supply keywords."""
        combined = f"{title} {content}".lower()
        return any(kw in combined for kw in self.SHORTAGE_KEYWORDS)

    def _determine_status(self, title: str, text: str) -> str:
        """Infer a normalised status string from free text."""
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

    def _extract_with_patterns(self, text: str, patterns: list[re.Pattern]) -> str:
        """Return first match from a list of compiled patterns."""
        for pat in patterns:
            m = pat.search(text)
            if m:
                val = m.group(1).strip().rstrip(",;.")
                if val and len(val) >= 2:
                    return val
        return ""

    # ------------------------------------------------------------------
    # WordPress REST API layer
    # ------------------------------------------------------------------

    def _resolve_category_ids(self) -> list[int]:
        """Look up WP category IDs for the slugs we care about."""
        ids = []
        resp = self._fetch(
            f"{self.BASE_URL}/wp-json/wp/v2/categories",
            params={"per_page": 100},
        )
        if not resp:
            return ids
        try:
            categories = resp.json()
            for cat in categories:
                if cat.get("slug") in self.CATEGORY_SLUGS:
                    ids.append(cat["id"])
        except (ValueError, KeyError):
            pass
        return ids

    def _search_wp_posts(self, max_pages: int = 10) -> list[dict]:
        """Query the WP REST API for shortage-related posts.

        Runs a separate search for each keyword to work around WP search
        limitations, then deduplicates by post ID.
        """
        seen_ids: set[int] = set()
        results: list[dict] = []

        for term in self.WP_SEARCH_TERMS:
            page = 1
            while page <= max_pages:
                resp = self._fetch(
                    self.WP_API_POSTS,
                    params={
                        "search": term,
                        "per_page": 50,
                        "page": page,
                        "orderby": "date",
                        "order": "desc",
                    },
                )
                if not resp:
                    break
                try:
                    posts = resp.json()
                except ValueError:
                    break

                if not posts:
                    break

                for post in posts:
                    pid = post.get("id")
                    if pid in seen_ids:
                        continue
                    seen_ids.add(pid)

                    title = BeautifulSoup(
                        post.get("title", {}).get("rendered", ""), "lxml"
                    ).get_text(strip=True)
                    content_html = post.get("content", {}).get("rendered", "")
                    content_text = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True)
                    link = post.get("link", "")
                    date_str = post.get("date", "")

                    if self._is_shortage_related(title, content_text):
                        results.append({
                            "title": title,
                            "content_html": content_html,
                            "content_text": content_text,
                            "url": link,
                            "date": date_str,
                        })

                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1

        return results

    # ------------------------------------------------------------------
    # HTML parsing helpers
    # ------------------------------------------------------------------

    def _extract_table_rows(self, soup: BeautifulSoup) -> list[dict]:
        """Extract structured medicine data from HTML tables."""
        records: list[dict] = []
        tables = soup.find_all("table")

        for table in tables:
            header_row = table.find("tr")
            if not header_row:
                continue
            headers = [
                th.get_text(strip=True).lower()
                for th in header_row.find_all(["th", "td"])
            ]

            name_idx = substance_idx = strength_idx = status_idx = None
            for i, h in enumerate(headers):
                if any(kw in h for kw in ("product", "brand", "medicine", "drug", "name")):
                    name_idx = i
                elif any(kw in h for kw in ("generic", "active", "ingredient", "inn", "substance")):
                    substance_idx = i
                elif any(kw in h for kw in ("strength", "dosage", "dose", "concentration")):
                    strength_idx = i
                elif any(kw in h for kw in ("status", "remark", "action", "availability")):
                    status_idx = i

            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if not cells or all(not c for c in cells):
                    continue

                med = cells[name_idx] if name_idx is not None and name_idx < len(cells) else ""
                sub = cells[substance_idx] if substance_idx is not None and substance_idx < len(cells) else ""
                stre = cells[strength_idx] if strength_idx is not None and strength_idx < len(cells) else ""
                stat = cells[status_idx] if status_idx is not None and status_idx < len(cells) else ""

                if med or sub:
                    records.append({
                        "medicine_name": med,
                        "active_substance": sub,
                        "strength": stre,
                        "status": stat,
                    })

        return records

    def _parse_recall_entry(self, section_soup: BeautifulSoup) -> dict | None:
        """Parse a single recall entry from the product-recalls page."""
        text = section_soup.get_text("\n", strip=True)
        if len(text) < 20:
            return None

        # Try to find product/medicine name
        heading = section_soup.find(["h2", "h3", "h4", "h5", "strong", "b"])
        medicine_name = heading.get_text(strip=True) if heading else ""

        # Look for labelled fields in the text
        fields: dict[str, str] = {}
        for line in text.split("\n"):
            if ":" in line:
                key, _, value = line.partition(":")
                fields[key.strip().lower()] = value.strip()

        # Map known fields
        if not medicine_name:
            for k in ("product", "medicine", "drug", "brand", "trade name"):
                if k in fields:
                    medicine_name = fields[k]
                    break

        if not medicine_name:
            return None

        registration = ""
        for k in ("registration", "reg no", "reg. no"):
            if k in fields:
                registration = fields[k]
                break

        batch = ""
        for k in ("batch", "lot"):
            if k in fields:
                batch = fields[k]
                break

        recall_date = ""
        for k in ("recall date", "date"):
            if k in fields:
                recall_date = fields[k]
                break

        # Find PDF download link
        pdf_link = ""
        a_tag = section_soup.find("a", href=re.compile(r"\.pdf", re.I))
        if a_tag:
            pdf_link = urljoin(self.BASE_URL, a_tag["href"])

        return {
            "medicine_name": medicine_name,
            "active_substance": "",
            "strength": "",
            "registration_no": registration,
            "batch_number": batch,
            "recall_date": recall_date,
            "pdf_url": pdf_link,
        }

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_from_pdf(self, pdf_url: str) -> list[dict]:
        """Download a PDF and attempt to extract medicine shortage info.

        Uses pdfplumber to read tables; falls back to text extraction.
        Returns a list of partial record dicts.
        """
        try:
            import pdfplumber
        except ImportError:
            print("    pdfplumber not installed -- skipping PDF extraction")
            return []

        resp = self._fetch(pdf_url)
        if not resp:
            return []

        records: list[dict] = []
        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                # First pass: try to extract tables
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        headers = [str(c).strip().lower() if c else "" for c in table[0]]

                        name_idx = sub_idx = str_idx = stat_idx = None
                        for i, h in enumerate(headers):
                            if any(kw in h for kw in ("product", "brand", "medicine", "drug", "name")):
                                name_idx = i
                            elif any(kw in h for kw in ("generic", "active", "ingredient", "substance")):
                                sub_idx = i
                            elif any(kw in h for kw in ("strength", "dosage", "dose")):
                                str_idx = i
                            elif any(kw in h for kw in ("status", "availability")):
                                stat_idx = i

                        for row in table[1:]:
                            if not row:
                                continue
                            med = str(row[name_idx]).strip() if name_idx is not None and name_idx < len(row) and row[name_idx] else ""
                            sub = str(row[sub_idx]).strip() if sub_idx is not None and sub_idx < len(row) and row[sub_idx] else ""
                            stre = str(row[str_idx]).strip() if str_idx is not None and str_idx < len(row) and row[str_idx] else ""
                            stat = str(row[stat_idx]).strip() if stat_idx is not None and stat_idx < len(row) and row[stat_idx] else ""
                            if med or sub:
                                records.append({
                                    "medicine_name": med,
                                    "active_substance": sub,
                                    "strength": stre,
                                    "status": stat,
                                })

                # Second pass: text extraction if no tables found
                if not records:
                    full_text = "\n".join(
                        page.extract_text() or "" for page in pdf.pages
                    )
                    med = self._extract_with_patterns(full_text, self.MEDICINE_PATTERNS)
                    sub = self._extract_with_patterns(full_text, self.ACTIVE_SUBSTANCE_PATTERNS)
                    stre = self._extract_with_patterns(full_text, self.STRENGTH_PATTERNS)
                    if med or sub:
                        records.append({
                            "medicine_name": med,
                            "active_substance": sub,
                            "strength": stre,
                            "status": "",
                        })

        except Exception as e:
            print(f"    PDF parsing error for {pdf_url}: {e}")

        return records

    # ------------------------------------------------------------------
    # Product recalls page
    # ------------------------------------------------------------------

    def _scrape_product_recalls(self) -> list[dict]:
        """Scrape the SAHPRA product-recalls page."""
        print("  Scraping product recalls page...")
        resp = self._fetch(self.PRODUCT_RECALLS_URL)
        if not resp:
            print("    Could not fetch product recalls page")
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        # The page lists recalls in sections; each recall may be in an article,
        # div, or separated by headings.
        content = (
            soup.find("div", class_="entry-content")
            or soup.find("article")
            or soup.find("main")
            or soup
        )

        records: list[dict] = []

        # Strategy 1: look for HTML tables
        table_records = self._extract_table_rows(content)
        if table_records:
            for rec in table_records:
                rec["source_section"] = "product_recalls"
            records.extend(table_records)
            print(f"    Found {len(table_records)} records from recalls tables")
            return records

        # Strategy 2: parse individual recall sections (div/article blocks)
        sections = content.find_all(
            ["article", "div", "section"],
            class_=re.compile(r"(recall|entry|post|card|product|item)", re.I),
        )
        if not sections:
            # Try splitting by <hr> or heading tags
            sections = content.find_all(["h2", "h3", "h4"])

        for sec in sections:
            # Grab the sibling content until the next heading
            if sec.name in ("h2", "h3", "h4"):
                wrapper = BeautifulSoup("<div></div>", "lxml").find("div")
                wrapper.append(sec.__copy__())
                for sib in sec.next_siblings:
                    if hasattr(sib, "name") and sib.name in ("h2", "h3", "h4"):
                        break
                    wrapper.append(sib.__copy__() if hasattr(sib, "__copy__") else sib)
                entry = self._parse_recall_entry(wrapper)
            else:
                entry = self._parse_recall_entry(sec)

            if entry:
                entry["source_section"] = "product_recalls"
                records.append(entry)

        print(f"    Found {len(records)} recall entries")
        return records

    # ------------------------------------------------------------------
    # Post-level parsing
    # ------------------------------------------------------------------

    def _parse_post(self, post: dict) -> list[dict]:
        """Parse a single WP post/advisory and return record dicts."""
        title = post["title"]
        content_html = post.get("content_html", "")
        content_text = post.get("content_text", "")
        post_url = post.get("url", "")
        post_date = self._parse_date(post.get("date", ""))
        status = self._determine_status(title, content_text)

        records: list[dict] = []

        # Try HTML table extraction
        if content_html:
            soup = BeautifulSoup(content_html, "lxml")
            table_records = self._extract_table_rows(soup)
            if table_records:
                for rec in table_records:
                    records.append(self._build_record(
                        medicine_name=rec.get("medicine_name", ""),
                        active_substance=rec.get("active_substance", ""),
                        strength=rec.get("strength", ""),
                        status=rec.get("status") or status,
                        shortage_start=post_date,
                        notice_title=title,
                        notice_url=post_url,
                        notice_date=post_date,
                    ))
                return records

            # Follow PDF links inside the post
            pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
            for a_tag in pdf_links[:5]:  # limit to avoid runaway fetches
                pdf_url = urljoin(self.BASE_URL, a_tag["href"])
                pdf_records = self._extract_from_pdf(pdf_url)
                for rec in pdf_records:
                    records.append(self._build_record(
                        medicine_name=rec.get("medicine_name", ""),
                        active_substance=rec.get("active_substance", ""),
                        strength=rec.get("strength", ""),
                        status=rec.get("status") or status,
                        shortage_start=post_date,
                        notice_title=title,
                        notice_url=post_url,
                        notice_date=post_date,
                        pdf_url=pdf_url,
                    ))
                if records:
                    return records

        # Free-text extraction from the post body
        if content_text:
            med = self._extract_with_patterns(content_text, self.MEDICINE_PATTERNS)
            sub = self._extract_with_patterns(content_text, self.ACTIVE_SUBSTANCE_PATTERNS)
            stre = self._extract_with_patterns(content_text, self.STRENGTH_PATTERNS)
            if med or sub:
                records.append(self._build_record(
                    medicine_name=med,
                    active_substance=sub,
                    strength=stre,
                    status=status,
                    shortage_start=post_date,
                    notice_title=title,
                    notice_url=post_url,
                    notice_date=post_date,
                ))
                return records

        # Last resort: keep the notice itself so it is not lost
        records.append(self._build_record(
            medicine_name=title,
            active_substance="",
            strength="",
            status=status,
            shortage_start=post_date,
            notice_title=title,
            notice_url=post_url,
            notice_date=post_date,
        ))
        return records

    def _build_record(
        self,
        medicine_name: str,
        active_substance: str,
        strength: str,
        status: str,
        shortage_start: str | None = None,
        estimated_end: str | None = None,
        notice_title: str = "",
        notice_url: str = "",
        notice_date: str | None = None,
        pdf_url: str = "",
    ) -> dict:
        """Construct a standardised output record."""
        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine_name,
            "active_substance": active_substance,
            "strength": strength,
            "package_size": "",
            "status": status,
            "shortage_start": shortage_start,
            "estimated_end": estimated_end,
            "notice_title": notice_title,
            "notice_url": notice_url,
            "notice_date": notice_date,
            "pdf_url": pdf_url,
            "scraped_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Fallback: HTML scraping of news listing pages
    # ------------------------------------------------------------------

    def _scrape_news_html(self, max_pages: int = 5) -> list[dict]:
        """Crawl the news/press-release listing pages as a WP-API fallback."""
        listing_urls = [
            f"{self.BASE_URL}/press-releases/",
            f"{self.BASE_URL}/news-and-updates/",
        ]

        advisories: list[dict] = []
        seen_urls: set[str] = set()

        for listing_url in listing_urls:
            for page_num in range(1, max_pages + 1):
                url = listing_url if page_num == 1 else f"{listing_url}page/{page_num}/"
                resp = self._fetch(url)
                if not resp:
                    break

                soup = BeautifulSoup(resp.text, "lxml")
                articles = (
                    soup.find_all("article")
                    or soup.find_all("div", class_=re.compile(r"post|entry"))
                )
                if not articles:
                    main = soup.find("main") or soup.find("div", id="content") or soup
                    articles = main.find_all("a", href=re.compile(r"sahpra\.org\.za/"))

                found = False
                for article in articles:
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
                    link = urljoin(self.BASE_URL, link)
                    if link in seen_urls:
                        continue

                    if self._is_shortage_related(title, ""):
                        seen_urls.add(link)
                        date_el = article.find("time") if hasattr(article, "find") else None
                        date_str = ""
                        if date_el:
                            date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)

                        advisories.append({
                            "title": title,
                            "content_html": "",
                            "content_text": "",
                            "url": link,
                            "date": date_str,
                        })
                        found = True

                if not found and page_num > 1:
                    break

        return advisories

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_records: list[dict] = []

        # ----- 1. WordPress REST API search for shortage posts -----
        print("  Searching WordPress REST API for shortage-related posts...")
        wp_posts = self._search_wp_posts(max_pages=5)
        print(f"  Found {len(wp_posts)} shortage-related posts via WP API")

        # If the API returned nothing, fall back to HTML crawl
        if not wp_posts:
            print("  WP API returned no results, falling back to HTML listing pages...")
            wp_posts = self._scrape_news_html(max_pages=5)
            print(f"  Found {len(wp_posts)} posts via HTML scraping")

            # For HTML-scraped posts we may need to fetch full content
            for i, post in enumerate(wp_posts):
                if not post.get("content_html") and post.get("url"):
                    print(f"    Fetching post {i + 1}/{len(wp_posts)}: {post['title'][:60]}...")
                    resp = self._fetch(post["url"])
                    if resp:
                        page_soup = BeautifulSoup(resp.text, "lxml")
                        content_div = (
                            page_soup.find("div", class_="entry-content")
                            or page_soup.find("article")
                            or page_soup.find("main")
                            or page_soup
                        )
                        post["content_html"] = str(content_div)
                        post["content_text"] = content_div.get_text(" ", strip=True)

        # Parse each post
        for post in wp_posts:
            post_records = self._parse_post(post)
            all_records.extend(post_records)

        # ----- 2. Product recalls page -----
        recall_entries = self._scrape_product_recalls()
        for entry in recall_entries:
            status = "recalled"
            all_records.append(self._build_record(
                medicine_name=entry.get("medicine_name", ""),
                active_substance=entry.get("active_substance", ""),
                strength=entry.get("strength", ""),
                status=status,
                shortage_start=self._parse_date(entry.get("recall_date")),
                notice_title=entry.get("medicine_name", ""),
                notice_url=self.PRODUCT_RECALLS_URL,
                notice_date=self._parse_date(entry.get("recall_date")),
                pdf_url=entry.get("pdf_url", ""),
            ))

        # ----- 3. Build DataFrame -----
        df = pd.DataFrame(all_records)

        if df.empty:
            print("  WARNING: No shortage records found on the SAHPRA website.")
            print("  The website structure may have changed or no shortages are currently published.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source",
                "medicine_name", "active_substance", "strength",
                "package_size", "status", "shortage_start", "estimated_end",
                "notice_title", "notice_url", "notice_date", "pdf_url",
                "scraped_at",
            ])

        # Deduplicate by medicine_name + notice_url
        before = len(df)
        dedup_cols = ["medicine_name", "notice_url"]
        available_dedup = [c for c in dedup_cols if c in df.columns]
        if available_dedup:
            df = df.drop_duplicates(subset=available_dedup, keep="first")
        if len(df) < before:
            print(f"  Removed {before - len(df)} duplicate records")

        print(f"  Total: {len(df)} shortage / supply-disruption records scraped")
        return df
