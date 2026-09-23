"""Scraper for Austria BASG medicine shortage data via XML export.

LET OP bij het uitbreiden: de datums staan als ATTRIBUTEN op <Packung>, niet als
child-elementen. findtext() geeft daar None op, en juist dat is hier lang misgegaan --
shortage_start stond hard op "" terwijl de bron voor elke niet-beschikbare verpakking een
echte begindatum levert. De kaart viel daardoor terug op de scrapedatum, wat elk Oostenrijks
tekort even oud maakte als de laatste draai.

    <Packung PZN="..." Datum_Meldung="2020-11-30" Datum_letzte_Aenderung="2026-04-07"
             Beginn_Vertriebseinschraenkung="2020-11-30"
             Datum_voraussichtliche_Wiederbelieferung="2026-10-15"> ... </Packung>
"""

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

    @staticmethod
    def _datum(entry, *namen: str) -> str:
        """Eerste bruikbare datum uit de opgegeven ATTRIBUTEN van <Packung>.

        De bron bevat tikfouten in het jaartal ("0206-08-26" in plaats van "2006-08-26").
        Zo'n waarde is erger dan geen waarde: hij komt op de kaart terecht als een tekort
        dat achttien eeuwen loopt. Alles buiten 1990..volgend jaar gaat daarom weg.
        """
        grens = datetime.now().year + 1
        for naam in namen:
            waarde = (entry.attrib.get(naam) or "").strip()[:10]
            if len(waarde) != 10:
                continue
            try:
                jaar = datetime.strptime(waarde, "%Y-%m-%d").year
            except ValueError:
                continue
            if 1990 <= jaar <= grens:
                return waarde
        return ""

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
                # Begin van de vertriebseinschraenkung; valt die weg, dan de meldingsdatum.
                # Alle 678 niet-beschikbare verpakkingen hebben de eerste; de terugval raakt
                # dus vrijwel alleen de wel-beschikbare regels.
                "shortage_start": self._datum(entry, "Beginn_Vertriebseinschraenkung",
                                              "Datum_Meldung"),
                "estimated_end": self._datum(entry, "Datum_voraussichtliche_Wiederbelieferung"),
                "last_updated": self._datum(entry, "Datum_letzte_Aenderung", "Datum_Meldung"),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
