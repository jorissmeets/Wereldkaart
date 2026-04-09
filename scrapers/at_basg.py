"""Scraper for Austria BASG medicine shortage data via XML export."""

import requests
import pandas as pd
from datetime import datetime
from xml.etree import ElementTree

from scrapers.base_scraper import BaseScraper


class AtBasgScraper(BaseScraper):
    """Scraper for BASG (Bundesamt für Sicherheit im Gesundheitswesen) shortage data."""

    XML_URL = "https://webservices.basg.gv.at/medicineshortage/export/v1/download"

    def __init__(self):
        super().__init__(
            country_code="AT",
            country_name="Austria",
            source_name="BASG",
            base_url="https://www.basg.gv.at",
        )

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.XML_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0",
                                          "Accept": "application/xml"})
        response.raise_for_status()

        root = ElementTree.fromstring(response.content)

        # Structure: VEASP > Packungen > Packung (repeated)
        packungen = root.find("Packungen")
        if packungen is None:
            raise ValueError("Could not find Packungen element in XML")

        entries = packungen.findall("Packung")
        print(f"  Found {len(entries)} Packung entries")

        records = []
        for entry in entries:
            name = entry.findtext("Bezeichnung_Arzneispezialitaet") or ""
            if not name.strip():
                continue

            strength = entry.findtext("Staerke") or ""
            unit = entry.findtext("Unit") or ""
            if strength and unit:
                strength = f"{strength} {unit}"

            pkg_size = entry.findtext("Packungsgroesse") or ""
            pkg_unit = entry.findtext("Packungseinheit") or ""
            if pkg_size and pkg_unit:
                pkg_size = f"{pkg_size} {pkg_unit}"

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name.strip(),
                "active_substance": (entry.findtext("Wirkstoffe") or "").strip(),
                "strength": strength.strip(),
                "dosage_form": (entry.findtext("Darreichungsform") or "").strip(),
                "package_size": pkg_size.strip(),
                "package_description": (entry.findtext("Packungsbeschreibung") or "").strip(),
                "atc_code": (entry.findtext("ATCCodes") or "").strip(),
                "marketing_auth_holder": (entry.findtext("Zulassungsinhaber") or "").strip(),
                "reporter": (entry.findtext("Melder") or "").strip(),
                "status": (entry.findtext("Status") or "").strip(),
                "reason": (entry.findtext("Grund") or "").strip(),
                "parallel_export_ban": (entry.findtext("Parallelexportverbot") or "").strip(),
                "legal_basis": (entry.findtext("Rechtsgrundlage_Meldung") or "").strip(),
                "healthcare_notice": (entry.findtext("Mitteilung_Fachkreise") or "").strip(),
                "basg_note": (entry.findtext("Hinweis_BASG") or "").strip(),
                "shortage_start": "",
                "estimated_end": "",
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
