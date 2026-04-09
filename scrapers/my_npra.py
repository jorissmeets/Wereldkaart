"""Scraper for Malaysia NPRA medicine shortage data via Google Sheets."""

import requests
import pandas as pd
from datetime import datetime
from io import StringIO

from scrapers.base_scraper import BaseScraper


class MyNpraScraper(BaseScraper):
    """Scraper for NPRA (Malaysia) medicine shortage/discontinuation data.

    Data is maintained in a public Google Sheet by the Malaysian
    National Pharmaceutical Regulatory Agency.
    """

    SHEET_ID = "1wH8oW7PMUULnIvn2AnEmYliHQ_CJdn3VLUpYvhDVsyo"
    CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

    def __init__(self):
        super().__init__(
            country_code="MY",
            country_name="Malaysia",
            source_name="NPRA",
            base_url="https://www.npra.gov.my",
        )

    def _parse_date(self, val) -> str | None:
        if pd.isna(val) or not val:
            return None
        val = str(val).strip()
        if not val:
            return None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%B %Y", "%b %Y"):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(
            self.CSV_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        resp.raise_for_status()

        raw = pd.read_csv(StringIO(resp.text))
        print(f"  Downloaded {len(raw)} rows")

        # Filter out empty/header rows
        raw = raw.dropna(subset=["Product Name"])

        records = []
        for _, row in raw.iterrows():
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(row.get("Product Name", "")),
                "active_substance": str(row.get("Active Ingredient", "")) if pd.notna(row.get("Active Ingredient")) else "",
                "strength": str(row.get("Strength", "")) if pd.notna(row.get("Strength")) else "",
                "package_size": "",
                "dosage_form": str(row.get("Dosage Form", "")) if pd.notna(row.get("Dosage Form")) else "",
                "registration_number": str(row.get("Product Registration (MAL)", "")) if pd.notna(row.get("Product Registration (MAL)")) else "",
                "atc_code": str(row.get("ATC Product Code", "")) if pd.notna(row.get("ATC Product Code")) else "",
                "company_name": str(row.get("Product Registration Holder (PRH)", "")) if pd.notna(row.get("Product Registration Holder (PRH)")) else "",
                "manufacturer": str(row.get("Product Manufacturer", "")) if pd.notna(row.get("Product Manufacturer")) else "",
                "disruption_type": str(row.get("Type of Disruption", "")) if pd.notna(row.get("Type of Disruption")) else "",
                "status": str(row.get("Status", "")) if pd.notna(row.get("Status")) else "",
                "reason": str(row.get("Reason for Discontinuation / Shortage", "")) if pd.notna(row.get("Reason for Discontinuation / Shortage")) else "",
                "shortage_start": self._parse_date(row.get("Supply Impact Start Date")),
                "estimated_end": self._parse_date(row.get("Supply Impact End Date")),
                "mitigation_prh": str(row.get("Mitigation Plan by PRH for shortage status", "")).strip() if pd.notna(row.get("Mitigation Plan by PRH for shortage status")) else "",
                "mitigation_npra": str(row.get("Mitigation Plan by NPRA", "")).strip() if pd.notna(row.get("Mitigation Plan by NPRA")) else "",
                "alternatives": str(row.get("Alternative Registered Products Available", "")).strip() if pd.notna(row.get("Alternative Registered Products Available")) else "",
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage/discontinuation records scraped")
        return df
