"""Scraper for Romania ANMDMR shortage data from PDF."""

import re
import tempfile
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class RoAnmScraper(BaseScraper):
    """Scraper for ANMDMR (Romania) discontinuation notifications PDF."""

    NOTIF_PAGE = "https://www.anm.ro/medicamente-de-uz-uman/autorizare-medicamente/notificari-discontinuitate-medicamente/"

    def __init__(self):
        super().__init__(
            country_code="RO",
            country_name="Romania",
            source_name="ANMDMR",
            base_url="https://www.anm.ro",
        )

    def _find_pdf_url(self) -> str:
        """Find the latest discontinuation PDF URL."""
        response = requests.get(self.NOTIF_PAGE, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf") and "discontinuitate" in href.lower():
                if href.startswith("/"):
                    return self.base_url + href
                return href

        raise ValueError("Could not find discontinuation PDF on ANMDMR page")

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        for fmt in ("%d.%m.%Y", "%d.%m.%y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        pdf_url = self._find_pdf_url()
        print(f"  Found PDF: {pdf_url}")

        resp = requests.get(pdf_url, timeout=60,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        print(f"  Downloaded {len(resp.content) / 1024:.0f} KB PDF")

        import tabula
        dfs = tabula.read_pdf(tmp_path, pages="all", lattice=True)
        print(f"  Found {len(dfs)} table(s) across pages")

        # Expected columns: Nr crt, Denumire comerciala, Forma Farmaceutica,
        # Concentratie, Firma Detinatoare, Tara Detinatoare, DCI,
        # Data adresa, Tip Notificare, Data estimativa reluare, Observatii
        all_rows = []
        for df in dfs:
            df.columns = range(len(df.columns))
            for _, row in df.iterrows():
                first_val = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                # Skip header rows
                if any(kw in first_val.lower() for kw in ["nr crt", "nr.", "nan"]):
                    continue
                # Must have a numeric row number
                try:
                    int(first_val.strip())
                except (ValueError, AttributeError):
                    continue
                all_rows.append(row.tolist())

        print(f"  Parsed {len(all_rows)} data rows")

        records = []
        for row in all_rows:
            while len(row) < 11:
                row.append("")

            name = str(row[1]).strip().replace("\r", " ") if pd.notna(row[1]) else ""
            form = str(row[2]).strip().replace("\r", " ") if pd.notna(row[2]) else ""
            strength = str(row[3]).strip() if pd.notna(row[3]) else ""
            mah = str(row[4]).strip().replace("\r", " ") if pd.notna(row[4]) else ""
            mah_country = str(row[5]).strip() if pd.notna(row[5]) else ""
            substance = str(row[6]).strip().replace("\r", ", ") if pd.notna(row[6]) else ""
            notif_date = str(row[7]).strip() if pd.notna(row[7]) else ""
            notif_type = str(row[8]).strip().replace("\r", " ") if pd.notna(row[8]) else ""
            resume_date = str(row[9]).strip().replace("\r", " ") if pd.notna(row[9]) else ""
            observations = str(row[10]).strip().replace("\r", " ") if pd.notna(row[10]) else ""

            # Map notification type to status
            if "permanenta" in notif_type.lower():
                status = "permanent_discontinuation"
            elif "temporara" in notif_type.lower():
                status = "temporary_discontinuation"
            else:
                status = "discontinuation"

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name,
                "active_substance": substance,
                "strength": strength,
                "dosage_form": form,
                "marketing_auth_holder": mah,
                "mah_country": mah_country,
                "notification_date": self._parse_date(notif_date),
                "shortage_start": self._parse_date(notif_date),
                "estimated_end": self._parse_date(resume_date) if resume_date and resume_date.lower() != "nan" else None,
                "status": status,
                "notification_type": notif_type,
                "reason": observations if observations.lower() != "nan" else "",
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
