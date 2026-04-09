"""Scraper for Greece EOF shortage data from PDF list."""

import re
import tempfile
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class GrEofScraper(BaseScraper):
    """Scraper for EOF (National Organization for Medicines) shortage PDF."""

    SHORTAGE_PAGE = "https://www.eof.gr/web/guest/drugshortage"

    # Standard column names after header row
    COLUMNS = [
        "barcode", "product_description", "atc_code", "active_substance",
        "distribution_method", "marketing_auth_holder", "shortage_start",
        "estimated_end", "reason", "therapeutic_alternatives",
    ]

    def __init__(self):
        super().__init__(
            country_code="GR",
            country_name="Greece",
            source_name="EOF",
            base_url="https://www.eof.gr",
        )

    def _find_pdf_url(self) -> str:
        """Find the latest shortage PDF URL from the EOF page."""
        response = requests.get(self.SHORTAGE_PAGE, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        # First find the link to the shortage list page (not directly PDF)
        list_page_url = None
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).upper()
            if "ΛΙΣΤΑ" in text and "ΠΕΡΙΟΡΙΣΜ" in text:
                list_page_url = a["href"]
                break

        if not list_page_url:
            raise ValueError("Could not find shortage list link on EOF page")

        # Follow the link to find the actual PDF
        response2 = requests.get(list_page_url, timeout=30,
                                 headers={"User-Agent": "Mozilla/5.0"})
        response2.raise_for_status()
        soup2 = BeautifulSoup(response2.text, "lxml")

        for a in soup2.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf") and "ΛΙΣΤΑ" in href.upper():
                if href.startswith("/"):
                    return self.base_url + href
                return href

        raise ValueError("Could not find PDF download link on shortage list page")

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        for fmt in ("%d/%m/%y", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        pdf_url = self._find_pdf_url()
        print(f"  Found PDF: {pdf_url}")

        # Download PDF to temp file
        resp = requests.get(pdf_url, timeout=60,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        print(f"  Downloaded {len(resp.content) / 1024:.0f} KB PDF")

        # Parse PDF tables
        import tabula
        dfs = tabula.read_pdf(tmp_path, pages="all", lattice=True)
        print(f"  Found {len(dfs)} table(s) across pages")

        # Combine all tables, skipping header rows
        all_rows = []
        for df in dfs:
            # Reset columns to numeric indices
            df.columns = range(len(df.columns))
            for _, row in df.iterrows():
                first_val = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                # Skip header rows and title rows
                if any(kw in first_val.upper() for kw in ["BARCODE", "ΛΙΣΤΑ", "NAN"]):
                    continue
                if not first_val or len(first_val) < 5:
                    continue
                all_rows.append(row.tolist())

        print(f"  Parsed {len(all_rows)} data rows")

        records = []
        for row in all_rows:
            # Ensure we have enough columns (pad if needed)
            while len(row) < 10:
                row.append("")

            barcode = str(row[0]).strip() if pd.notna(row[0]) else ""
            product = str(row[1]).strip().replace("\r", " ") if pd.notna(row[1]) else ""
            atc = str(row[2]).strip() if pd.notna(row[2]) else ""
            substance = str(row[3]).strip().replace("\r", ", ") if pd.notna(row[3]) else ""
            dist_method = str(row[4]).strip() if pd.notna(row[4]) else ""
            mah = str(row[5]).strip().replace("\r", " ") if pd.notna(row[5]) else ""
            start_date = str(row[6]).strip() if pd.notna(row[6]) else ""
            end_date = str(row[7]).strip() if pd.notna(row[7]) else ""
            reason = str(row[8]).strip().replace("\r", " ") if pd.notna(row[8]) else ""
            alternatives = str(row[9]).strip().replace("\r", " ") if pd.notna(row[9]) else ""

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": product,
                "active_substance": substance,
                "strength": "",
                "package_size": "",
                "product_no": barcode,
                "atc_code": atc,
                "distribution_method": dist_method,
                "marketing_auth_holder": mah,
                "shortage_start": self._parse_date(start_date),
                "estimated_end": self._parse_date(end_date),
                "status": "shortage",
                "reason": reason,
                "therapeutic_alternatives": alternatives,
                "scraped_at": datetime.now().isoformat(),
            })

        # Clean up temp file
        import os
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
