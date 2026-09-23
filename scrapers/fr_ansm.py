"""Scraper for France ANSM (Agence nationale de sécurité du médicament)."""
from __future__ import annotations

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

    EXPORT_URL = ("https://ansm.sante.fr/disponibilites-des-produits-de-sante/"
                  "medicaments/export")

    @staticmethod
    def _sleutel(naam: str) -> str:
        """Normaliseer een specialiteitsnaam tot een vergelijkbare sleutel."""
        return re.sub(r"[^a-z0-9]", "", str(naam or "").lower())

    def _haal_export(self) -> dict:
        """Naam -> {start, eind, bijgewerkt} uit de Excel-export van dezelfde pagina.

        Twee eigenaardigheden. De export is een .xls die xlrd als beschadigd ziet (een bekend
        euvel bij gegenereerde bestanden), vandaar ignore_workbook_corruption. En de titel in
        de export is de specialiteitsnaam PLUS " - [werkzame stof]", terwijl de HTML-tabel die
        twee gescheiden heeft; we knippen het achtervoegsel er weer af om exact te kunnen
        matchen in plaats van op een prefix te gokken.
        """
        try:
            import xlrd
            r = requests.get(self.EXPORT_URL, timeout=90,
                             headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            r.raise_for_status()
            bk = xlrd.open_workbook(file_contents=r.content, ignore_workbook_corruption=True)
            sh = bk.sheet_by_index(0)
            kop = [str(sh.cell_value(0, c)).strip() for c in range(sh.ncols)]
            idx = {naam: kop.index(naam) for naam in
                   ("Titre", "Date de début de situation", "Date de mise à jour",
                    "Date de remise à disposition") if naam in kop}
            if "Titre" not in idx or "Date de début de situation" not in idx:
                print("  LET OP: ANSM-export heeft niet de verwachte kolommen; geen datums")
                return {}
            uit = {}
            for rij in range(1, sh.nrows):
                titel = str(sh.cell_value(rij, idx["Titre"])).strip()
                if not titel:
                    continue
                kaal = re.sub(r"\s*[-\u2013\u2014]\s*\[.*?\]\s*$", "", titel)
                uit[self._sleutel(kaal)] = {
                    "start": self._parse_date(str(sh.cell_value(rij, idx["Date de début de situation"])).strip()),
                    "bijgewerkt": self._parse_date(str(sh.cell_value(rij, idx.get("Date de mise à jour", 0))).strip())
                                  if "Date de mise à jour" in idx else "",
                    "eind": self._parse_date(str(sh.cell_value(rij, idx.get("Date de remise à disposition", 0))).strip())
                            if "Date de remise à disposition" in idx else "",
                }
            print(f"  Export: {len(uit)} meldingen met begindatum")
            return uit
        except Exception as e:                      # noqa: BLE001
            print(f"  LET OP: ANSM-export niet bruikbaar ({type(e).__name__}: {str(e)[:60]}); "
                  f"geen startdatums")
            return {}

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
        # De HTML-tabel heeft geen startdatum -- maar de officiele Excel-export van dezelfde
        # pagina wel, voor alle 294 meldingen. Die export is er altijd geweest; hier stond
        # jarenlang "ANSM levert geen startdatum", en Frankrijk had daardoor 0 startdatums
        # op de kaart. We houden de HTML als basis, want die splitst de werkzame stof af, en
        # halen de datums uit de export. Valt de export weg, dan draait de scraper door zoals
        # voorheen.
        extra = self._haal_export()

        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            status_raw = cells[0].get_text(strip=True)
            # ANSM-kolommen: Statut | Mise à jour | Spécialité | Remise à disposition | Domaines.
            # Er is GEEN startdatum-kolom; 'Mise à jour' is enkel de bijwerkdatum -> last_updated
            # (niet shortage_start, dat gaf een onjuiste 'start'). 'Remise à disposition' =
            # verwachte hersteldatum -> estimated_end.
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
                "shortage_start": extra.get(self._sleutel(medicine_name), {}).get("start", ""),
                "estimated_end": (extra.get(self._sleutel(medicine_name), {}).get("eind")
                                  or self._parse_date(remise_date)),
                "last_updated": (extra.get(self._sleutel(medicine_name), {}).get("bijgewerkt")
                                 or self._parse_date(update_date)),
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
