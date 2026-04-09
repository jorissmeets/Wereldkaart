"""Scraper for Taiwan TFDA drug shortage data.

Primary source: TFDA Open Data API (data.fda.gov.tw)
Fallback source: TFDA DSMS (Drug Shortage Management System) at dsms.fda.gov.tw
Second fallback: TFDA website drug shortage announcements via news API

The TFDA open data platform was rebuilt and API endpoints may change.
This scraper tries multiple known URL patterns and falls back to
web scraping if the API is unavailable.
"""

from __future__ import annotations

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class TwTfdaScraper(BaseScraper):
    """Scraper for TFDA (Taiwan) drug shortage data."""

    # TFDA Open Data API — platform was rebuilt; try multiple URL patterns
    OPEN_DATA_URLS = [
        # New platform (Swagger UI at root suggests REST API)
        "https://data.fda.gov.tw/api/v1/drugShortage",
        "https://data.fda.gov.tw/api/v2/drugShortage",
        "https://data.fda.gov.tw/api/drugShortage",
        # Old platform URL patterns (pre-rebuild)
        "https://data.fda.gov.tw/opendata/exportDataList.do?method=ExportData&InfoId=79&pageNo=1&typeSort=JSON",
        "https://data.fda.gov.tw/opendata/exportDataList.do?method=ExportData&InfoId=79&pageNo=1&typeSort=CSV",
    ]

    # DSMS (Drug Shortage Management System) — primary shortage database
    DSMS_BASE_URL = "https://dsms.fda.gov.tw"
    DSMS_LIST_URL = "https://dsms.fda.gov.tw/Home/DrugShortageList"
    DSMS_API_URL = "https://dsms.fda.gov.tw/api/DrugShortage/GetList"

    # TFDA news API — fallback for shortage announcements
    NEWS_API_URL = "https://www.fda.gov.tw/DataAction"

    # Column mapping: Chinese field names -> English
    COLUMN_MAP = {
        "藥品名稱": "medicine_name",
        "藥品品名": "medicine_name",
        "品名": "medicine_name",
        "成分": "active_substance",
        "有效成分": "active_substance",
        "主成分": "active_substance",
        "含量": "strength",
        "劑量": "strength",
        "規格量": "strength",
        "劑型": "dosage_form",
        "許可證號": "license_no",
        "許可證字號": "license_no",
        "藥品許可證字號": "license_no",
        "廠商名稱": "manufacturer",
        "製造廠": "manufacturer",
        "申請商": "applicant",
        "短缺原因": "shortage_reason",
        "缺藥原因": "shortage_reason",
        "短缺狀態": "status",
        "狀態": "status",
        "處理情形": "status",
        "通報日期": "report_date",
        "短缺開始日期": "shortage_start",
        "缺藥日期": "shortage_start",
        "預計恢復日期": "estimated_end",
        "預計供應日期": "estimated_end",
        "恢復供應日期": "estimated_end",
        "替代藥品": "alternative_drug",
        "替代品項": "alternative_drug",
        "備註": "remarks",
        "ATC碼": "atc_code",
        "ATC代碼": "atc_code",
    }

    # Status mapping: Chinese -> normalized English
    STATUS_MAP = {
        "短缺中": "shortage",
        "缺藥中": "shortage",
        "持續短缺": "shortage",
        "尚未恢復": "shortage",
        "處理中": "processing",
        "調查中": "investigating",
        "已恢復": "resolved",
        "恢復供應": "resolved",
        "已解決": "resolved",
        "供應正常": "resolved",
        "無短缺": "no_shortage",
        "建議使用替代品項": "alternative_available",
        "替代": "alternative_available",
        "專案進口": "special_import",
        "專案製造": "special_manufacture",
    }

    def __init__(self):
        super().__init__(
            country_code="TW",
            country_name="Taiwan",
            source_name="TFDA",
            base_url="https://data.fda.gov.tw",
        )

    def _parse_date(self, val) -> str | None:
        """Parse various date formats used by TFDA."""
        if pd.isna(val) or not val:
            return None
        val = str(val).strip()
        if not val or val in ("-", "N/A", "無", "未定", "不明"):
            return None

        # Handle ROC calendar (民國) dates: 112/03/25 or 112-03-25 or 1120325
        roc_match = re.match(r"(\d{2,3})[/\-.](\d{1,2})[/\-.](\d{1,2})", val)
        if roc_match:
            year = int(roc_match.group(1)) + 1911
            month = int(roc_match.group(2))
            day = int(roc_match.group(3))
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Handle compact ROC date: 1120325
        roc_compact = re.match(r"(\d{3})(\d{2})(\d{2})$", val)
        if roc_compact:
            year = int(roc_compact.group(1)) + 1911
            month = int(roc_compact.group(2))
            day = int(roc_compact.group(3))
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Standard date formats
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d", "%Y%m%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def _normalize_status(self, status_raw) -> str:
        """Normalize Chinese status text to English."""
        if pd.isna(status_raw) or not status_raw:
            return "unknown"
        status_str = str(status_raw).strip()
        for chinese, english in self.STATUS_MAP.items():
            if chinese in status_str:
                return english
        return status_str

    def _try_open_data_api(self) -> pd.DataFrame | None:
        """Try the TFDA Open Data API with multiple URL patterns."""
        for url in self.OPEN_DATA_URLS:
            try:
                resp = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue

                content_type = resp.headers.get("Content-Type", "")

                # Skip HTML error pages
                if "text/html" in content_type and "<title>" in resp.text[:500]:
                    continue

                # Try JSON
                if "json" in content_type or resp.text.strip().startswith(("[", "{")):
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        print(f"  Open Data API responded with {len(data)} records from {url}")
                        return self._parse_open_data_json(data)
                    elif isinstance(data, dict):
                        records = data.get("data", data.get("result", data.get("records", [])))
                        if isinstance(records, list) and len(records) > 0:
                            print(f"  Open Data API responded with {len(records)} records from {url}")
                            return self._parse_open_data_json(records)

                # Try CSV
                if "csv" in content_type or "text/plain" in content_type:
                    import io
                    df = pd.read_csv(io.StringIO(resp.text), encoding="utf-8")
                    if len(df) > 0:
                        print(f"  Open Data API responded with {len(df)} CSV records from {url}")
                        return self._parse_open_data_csv(df)

            except (requests.RequestException, ValueError):
                continue

        return None

    def _parse_open_data_json(self, records: list) -> pd.DataFrame:
        """Parse JSON records from the TFDA Open Data API."""
        rows = []
        for rec in records:
            row = {}
            for key, value in rec.items():
                mapped = self.COLUMN_MAP.get(key, key)
                row[mapped] = value
            rows.append(row)

        df = pd.DataFrame(rows)
        return self._standardize_dataframe(df)

    def _parse_open_data_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse CSV data from the TFDA Open Data API."""
        df = df.rename(columns=self.COLUMN_MAP)
        return self._standardize_dataframe(df)

    def _try_dsms(self) -> pd.DataFrame | None:
        """Try scraping the DSMS (Drug Shortage Management System)."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        })

        # Try the DSMS API endpoint first
        try:
            resp = session.get(self.DSMS_API_URL, timeout=30, verify=True)
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                if "json" in content_type:
                    data = resp.json()
                    records = data if isinstance(data, list) else data.get("data", data.get("result", []))
                    if records:
                        print(f"  DSMS API responded with {len(records)} records")
                        return self._parse_open_data_json(records)
        except requests.RequestException:
            pass

        # Try scraping the DSMS web page
        try:
            resp = session.get(self.DSMS_LIST_URL, timeout=30, verify=True)
            if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                return self._parse_dsms_html(resp.text)
        except requests.RequestException:
            pass

        # Try with relaxed SSL (DSMS sometimes has certificate issues)
        try:
            resp = session.get(self.DSMS_LIST_URL, timeout=30, verify=False)
            if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                return self._parse_dsms_html(resp.text)
        except requests.RequestException:
            pass

        return None

    def _parse_dsms_html(self, html: str) -> pd.DataFrame | None:
        """Parse drug shortage list from DSMS HTML page."""
        soup = BeautifulSoup(html, "html.parser")

        # Find the main data table
        table = soup.find("table", class_=re.compile(r"table|data|list", re.I))
        if not table:
            table = soup.find("table")
        if not table:
            return None

        # Extract headers
        headers = []
        header_row = table.find("tr")
        if header_row:
            for th in header_row.find_all(["th", "td"]):
                headers.append(th.get_text(strip=True))

        # Extract rows
        rows = []
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells and any(c.strip() for c in cells):
                if len(cells) == len(headers):
                    rows.append(dict(zip(headers, cells)))
                else:
                    rows.append({f"col_{i}": c for i, c in enumerate(cells)})

        if not rows:
            return None

        df = pd.DataFrame(rows)
        df = df.rename(columns=self.COLUMN_MAP)
        print(f"  DSMS HTML table: {len(df)} records")
        return self._standardize_dataframe(df)

    def _try_news_api(self) -> pd.DataFrame | None:
        """Fallback: query TFDA news API for drug shortage announcements."""
        keywords = ["缺藥", "藥品短缺", "藥品供應"]
        all_news = []

        for keyword in keywords:
            try:
                resp = requests.get(
                    self.NEWS_API_URL,
                    params={"keyword": keyword},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue

                content_type = resp.headers.get("Content-Type", "")
                if "json" not in content_type and "text/plain" not in content_type:
                    continue

                news_items = resp.json()
                if isinstance(news_items, list):
                    all_news.extend(news_items)
                    print(f"  News API: {len(news_items)} articles for '{keyword}'")
            except (requests.RequestException, ValueError):
                continue

        if not all_news:
            return None

        # Deduplicate by title
        seen_titles = set()
        unique_news = []
        for item in all_news:
            title = item.get("標題", "")
            if title and title not in seen_titles:
                seen_titles.add(title)
                unique_news.append(item)

        return self._parse_news_for_shortages(unique_news)

    def _parse_news_for_shortages(self, news_items: list) -> pd.DataFrame:
        """Extract structured shortage data from TFDA news articles."""
        records = []

        # Dosage form suffixes that indicate a drug name
        dosage_forms = (
            "注射劑", "注射液", "錠", "膠囊", "片", "丸",
            "口服液", "軟膏", "乳膏", "懸液劑", "粉劑",
            "噴霧劑", "貼片", "眼藥水", "點眼液", "糖漿",
            "散劑", "溶液", "凍晶注射劑",
        )
        dosage_pattern = "|".join(re.escape(f) for f in dosage_forms)

        for item in news_items:
            title = item.get("標題", "")
            content_html = item.get("內容", "")
            pub_date = item.get("發布日期", "")

            # Clean HTML content
            content_text = re.sub(r"<[^>]+>", " ", content_html)
            content_text = re.sub(r"&\w+;", " ", content_text)
            content_text = re.sub(r"\s+", " ", content_text).strip()

            # Extract drug names: 2+ Chinese chars ending in a dosage form,
            # optionally followed by parenthetical English/generic name
            drug_mentions = re.findall(
                r"([\u4e00-\u9fff]{2,}(?:" + dosage_pattern + r"))"
                r"(?:\s*[\(\uff08]\s*([A-Za-z][A-Za-z\s\-,]+?)\s*[\)\uff09])?",
                content_text,
            )

            # Also extract standalone English drug names in parentheses
            eng_drug_mentions = re.findall(
                r"[\(\uff08]\s*([A-Za-z][A-Za-z\s\-]{2,})\s*[\)\uff09]",
                content_text,
            )

            # Filter drug mentions: require at least 2 meaningful Chinese chars
            # before the dosage form suffix (exclude single-char prefixes like
            # "項同成分劑" which are sentence fragments)
            valid_drugs = []
            seen_names = set()
            for chinese_name, eng_name in drug_mentions:
                # Strip the dosage form to check the base name length
                base = chinese_name
                for form in dosage_forms:
                    if base.endswith(form):
                        base = base[: -len(form)]
                        break
                # Require at least 2 chars in the base name
                if len(base) >= 2 and chinese_name not in seen_names:
                    seen_names.add(chinese_name)
                    valid_drugs.append((chinese_name, eng_name))

            if valid_drugs:
                for chinese_name, eng_name in valid_drugs:
                    records.append({
                        "medicine_name": chinese_name.strip(),
                        "active_substance": eng_name.strip() if eng_name else "",
                        "strength": "",
                        "package_size": "",
                        "status": "shortage",
                        "shortage_reason": title,
                        "shortage_start": self._parse_date(pub_date),
                        "estimated_end": None,
                        "announcement_title": title,
                        "announcement_date": pub_date,
                        "data_source": "news_api",
                    })
            else:
                # If no specific drugs found, record the announcement itself
                records.append({
                    "medicine_name": title,
                    "active_substance": ", ".join(eng_drug_mentions) if eng_drug_mentions else "",
                    "strength": "",
                    "package_size": "",
                    "status": "shortage",
                    "shortage_reason": "",
                    "shortage_start": self._parse_date(pub_date),
                    "estimated_end": None,
                    "announcement_title": title,
                    "announcement_date": pub_date,
                    "data_source": "news_api",
                })

        df = pd.DataFrame(records)
        return self._standardize_dataframe(df)

    def _standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns exist and add metadata."""
        # Add country and source metadata
        df.insert(0, "country_code", self.country_code)
        df.insert(1, "country_name", self.country_name)
        df.insert(2, "source", self.source_name)

        # Ensure required columns exist
        for col in ["medicine_name", "active_substance", "strength", "package_size",
                     "shortage_start", "estimated_end", "status"]:
            if col not in df.columns:
                df[col] = ""

        # Parse dates
        if "shortage_start" in df.columns:
            df["shortage_start"] = df["shortage_start"].apply(self._parse_date)
        if "estimated_end" in df.columns:
            df["estimated_end"] = df["estimated_end"].apply(self._parse_date)

        # Normalize status
        if "status" in df.columns:
            df["status"] = df["status"].apply(self._normalize_status)

        df["scraped_at"] = datetime.now().isoformat()

        return df

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Strategy 1: TFDA Open Data API
        print("  Trying TFDA Open Data API...")
        df = self._try_open_data_api()
        if df is not None and len(df) > 0:
            print(f"  Total: {len(df)} shortage records scraped (source: Open Data API)")
            return df

        # Strategy 2: DSMS (Drug Shortage Management System)
        print("  Open Data API unavailable. Trying DSMS...")
        df = self._try_dsms()
        if df is not None and len(df) > 0:
            print(f"  Total: {len(df)} shortage records scraped (source: DSMS)")
            return df

        # Strategy 3: TFDA News API (drug shortage announcements)
        print("  DSMS unavailable. Trying TFDA News API fallback...")
        df = self._try_news_api()
        if df is not None and len(df) > 0:
            print(f"  Total: {len(df)} shortage records scraped (source: News API)")
            return df

        # If all strategies fail, return empty DataFrame with correct schema
        print("  WARNING: All data sources unavailable. Returning empty DataFrame.")
        print("  The TFDA Open Data platform (data.fda.gov.tw) may be under maintenance.")
        print("  The DSMS system (dsms.fda.gov.tw) may restrict external access.")
        return pd.DataFrame(columns=[
            "country_code", "country_name", "source",
            "medicine_name", "active_substance", "strength", "package_size",
            "shortage_start", "estimated_end", "status", "scraped_at",
        ])
