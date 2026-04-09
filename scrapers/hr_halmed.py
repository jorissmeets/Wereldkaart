"""Scraper for Croatia HALMED shortage data from PDF."""

import re
import tempfile
import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class HrHalmedScraper(BaseScraper):
    """Scraper for HALMED (Croatian Agency) shortage PDF."""

    PDF_URL = "https://www.halmed.hr/fdsak3jnFsk1Kfa/ostale_stranice/Nestasice-lijekova-tablica-za-objavu-WEB.pdf"

    def __init__(self):
        super().__init__(
            country_code="HR",
            country_name="Croatia",
            source_name="HALMED",
            base_url="https://www.halmed.hr",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip().rstrip(".")
        for fmt in ("%d.%m.%Y", "%d.%m.%y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _parse_period(self, period_str) -> tuple[str | None, str | None]:
        """Parse shortage period like '01.06.2016. - 30.05.2026.' or '09.03.2022. - nepoznato'."""
        if not period_str or not isinstance(period_str, str):
            return None, None
        parts = period_str.split(" - ")
        start = self._parse_date(parts[0].strip()) if len(parts) > 0 else None
        end = None
        if len(parts) > 1:
            end_str = parts[1].strip()
            if end_str.lower() != "nepoznato":
                end = self._parse_date(end_str)
        return start, end

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(self.PDF_URL, timeout=60,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        print(f"  Downloaded {len(resp.content) / 1024:.0f} KB PDF")

        import tabula
        dfs = tabula.read_pdf(tmp_path, pages="all", lattice=True)
        print(f"  Found {len(dfs)} table(s) across pages")

        all_rows = []
        for df in dfs:
            df.columns = range(len(df.columns))
            for _, row in df.iterrows():
                first_val = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                # Skip header rows
                if "Broj odobrenja" in first_val or "nan" in first_val.lower():
                    continue
                if not first_val or len(first_val) < 3:
                    continue
                all_rows.append(row.tolist())

        print(f"  Parsed {len(all_rows)} data rows")

        records = []
        for row in all_rows:
            while len(row) < 7:
                row.append("")

            approval_no = str(row[0]).strip() if pd.notna(row[0]) else ""
            mah = str(row[1]).strip().replace("\r", " ") if pd.notna(row[1]) else ""
            name_pkg = str(row[2]).strip().replace("\r", " ") if pd.notna(row[2]) else ""
            substance = str(row[3]).strip().replace("\r", ", ") if pd.notna(row[3]) else ""
            notif_date = str(row[4]).strip() if pd.notna(row[4]) else ""
            reason = str(row[5]).strip().replace("\r", " ") if pd.notna(row[5]) else ""
            period = str(row[6]).strip().replace("\r", " ") if pd.notna(row[6]) else ""

            start, end = self._parse_period(period)

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name_pkg,
                "active_substance": substance,
                "strength": "",
                "package_size": "",
                "authorisation_number": approval_no,
                "marketing_auth_holder": mah,
                "notification_date": self._parse_date(notif_date),
                "shortage_start": start,
                "estimated_end": end,
                "shortage_period": period,
                "status": "shortage",
                "reason": reason,
                "scraped_at": datetime.now().isoformat(),
            })

        import os
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
