"""Scraper for Slovakia ŠÚKL supply notification data via CSV export."""

import io
import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class SkSuklScraper(BaseScraper):
    """Scraper for Slovak ŠÚKL supply interruption notifications."""

    CSV_URL = "https://portal.sukl.sk/PreruseniePublic/?act=PrerusenieOznList&export=csv"
    DRUG_DETAIL_URL = "https://portal.sukl.sk/LiekDetail/?act=LiekDetailInfo&kodLP="

    SUBJECT_MAP = {
        "R": "supply_interruption",
        "O": "supply_resumption",
        "Z": "supply_discontinuation",
        "U": "first_introduction",
    }

    def __init__(self):
        super().__init__(
            country_code="SK",
            country_name="Slovakia",
            source_name="SUKL",
            base_url="https://portal.sukl.sk",
        )

    def _lookup_substance(self, kod: str) -> str:
        """Look up active substance from ŠÚKL drug detail page."""
        if not kod:
            return ""
        try:
            resp = requests.get(
                f"{self.DRUG_DETAIL_URL}{kod}",
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # Look for "Účinná látka:" or "Liečivo:" pattern
            for pattern in (
                r"[ÚU]činn[áa]\s+l[áa]tka:\s*([^\n;]+?)(?:\s*(?:Sila|ATC|Cesta|$))",
                r"[Ll]iečivo:\s*([^\n;]+?)(?:\s*(?:Sila|ATC|Cesta|$))",
            ):
                m = re.search(pattern, text)
                if m:
                    substance = m.group(1).strip().rstrip(",. ")
                    if substance and len(substance) >= 3:
                        return substance
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.CSV_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        raw_df = pd.read_csv(io.BytesIO(response.content), sep=";", encoding="utf-8-sig")
        print(f"  Downloaded {len(raw_df)} records")

        # Filter to supply interruptions and discontinuations (R and Z)
        shortage_df = raw_df[raw_df["Predmet"].isin(["R", "Z"])].copy()
        print(f"  Filtered to {len(shortage_df)} shortage/discontinuation records")

        # Batch-lookup unique product codes for active substances
        unique_kods = {str(row.get("Kód", "")).strip() for _, row in shortage_df.iterrows() if row.get("Kód")}
        unique_kods.discard("")
        unique_kods.discard("nan")
        print(f"  Looking up active substances for {len(unique_kods)} unique products...")
        kod_substance_map: dict[str, str] = {}
        for i, kod in enumerate(unique_kods):
            kod_substance_map[kod] = self._lookup_substance(kod)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_kods)} lookups done")
            time.sleep(0.15)
        found = sum(1 for v in kod_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(unique_kods)} products")

        records = []
        for _, row in shortage_df.iterrows():
            subject = str(row.get("Predmet", "")).strip()
            kod = str(row.get("Kód", "")).strip()

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(row.get("Liek", "")).strip(),
                "active_substance": kod_substance_map.get(kod, ""),
                "strength": "",
                "package_size": "",
                "product_no": kod,
                "marketing_auth_holder": str(row.get("Držiteľ", "")).strip(),
                "notification_date": str(row.get("Podanie", ""))[:10],
                "shortage_start": str(row.get("Účinnosť", "")).strip(),
                "status": self.SUBJECT_MAP.get(subject, subject),
                "notification_type": subject,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
