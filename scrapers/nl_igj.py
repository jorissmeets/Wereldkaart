"""Scraper for Netherlands IGJ exemption decisions (vrijstellingsbesluiten) for medicine shortages."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class NlIgjScraper(BaseScraper):
    """Scraper for IGJ (Inspectie Gezondheidszorg en Jeugd) exemption decisions."""

    URL = "https://www.igj.nl/zorgsectoren/geneesmiddelen/beschikbaarheid-van-geneesmiddelen/overzicht-vrijstellingsbesluiten"

    def __init__(self):
        super().__init__(
            country_code="NL",
            country_name="Netherlands",
            source_name="IGJ",
            base_url="https://www.igj.nl",
        )

    @staticmethod
    def _extract_substance(product_name: str) -> str:
        """Extract active substance from Dutch product name.

        IGJ product names typically follow the pattern:
        'Substance strength dosageform' e.g. 'Methotrexaat 2,5 mg tabletten'
        or 'Brand (substance) strength' e.g. 'Sandimmune (ciclosporine) 100 mg'
        """
        if not product_name:
            return ""
        name = product_name.strip()

        # Check for parenthesized substance: "Brand (substance) ..."
        m = re.search(r"\(([^)]+)\)", name)
        if m:
            return m.group(1).strip()

        # Take everything before the first number as the substance
        m = re.match(r"^([A-Za-zÀ-ÿ\s/-]+?)(?:\s+\d|$)", name)
        if m:
            substance = m.group(1).strip().rstrip(" -/")
            if substance and len(substance) >= 3:
                return substance

        return ""

    def _parse_date(self, date_str) -> str | None:
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if date_str == "-":
            return None
        # Dutch month names
        months = {
            "januari": "01", "februari": "02", "maart": "03", "april": "04",
            "mei": "05", "juni": "06", "juli": "07", "augustus": "08",
            "september": "09", "oktober": "10", "november": "11", "december": "12",
        }
        for nl, num in months.items():
            date_str = date_str.replace(nl, num)
        try:
            return datetime.strptime(date_str, "%d %m %Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.URL, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        tables = soup.find_all("table")
        print(f"  Found {len(tables)} tables on page")

        records = []
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:  # skip header
                cells = row.find_all(["td", "th"])
                if len(cells) < 6:
                    continue

                decision = cells[0].get_text(strip=True)
                product = cells[1].get_text(strip=True)
                form_strength = cells[2].get_text(strip=True)
                rvg = cells[3].get_text(strip=True)
                valid_from = cells[4].get_text(strip=True)
                valid_until = cells[5].get_text(strip=True)

                start = self._parse_date(valid_from)
                end = self._parse_date(valid_until)

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": product,
                    "active_substance": self._extract_substance(product),
                    "strength": form_strength,
                    "package_size": "",
                    "product_no": rvg,
                    "shortage_start": start,
                    "estimated_end": end,
                    "status": "shortage",
                    "decision_reference": decision,
                    "scraped_at": datetime.now().isoformat(),
                })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} exemption records scraped")
        return df
