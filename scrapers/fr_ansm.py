"""Scraper for France ANSM (Agence nationale de sécurité du médicament)."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class FrAnsmScraper(BaseScraper):
    """Scraper for https://ansm.sante.fr/disponibilites-des-produits-de-sante/medicaments"""

    URL = "https://ansm.sante.fr/disponibilites-des-produits-de-sante/medicaments"

    STATUS_MAP = {
        "rupture de stock": "shortage",
        "tension d'approvisionnement": "supply tension",
        "remise à disposition": "resolved",
        "arrêt de commercialisation": "discontinued",
    }

    def __init__(self):
        super().__init__(
            country_code="FR",
            country_name="France",
            source_name="ANSM",
            base_url="https://ansm.sante.fr",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str:
            return None
        date_str = str(date_str).strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.URL, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        table = soup.find("table")
        if not table:
            raise RuntimeError("No table found on ANSM page")

        rows = table.find_all("tr")
        print(f"  Found {len(rows) - 1} table rows")

        records = []
        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            status_raw = cells[0].get_text(strip=True)
            update_date = cells[1].get_text(strip=True)
            specialty_raw = cells[2].get_text(strip=True)
            remise_date = cells[3].get_text(strip=True) if len(cells) > 3 else ""

            # Parse specialty: "Name dosage, form – [substance]"
            medicine_name = specialty_raw
            active_substance = ""
            match = re.match(r"(.+?)\s*–\s*\[(.+?)\]", specialty_raw)
            if match:
                medicine_name = match.group(1).strip()
                active_substance = match.group(2).strip()

            status = self.STATUS_MAP.get(status_raw.lower(), status_raw)

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "active_substance": active_substance,
                "strength": "",
                "package_size": "",
                "product_no": "",
                "shortage_start": self._parse_date(update_date),
                "estimated_end": self._parse_date(remise_date),
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
