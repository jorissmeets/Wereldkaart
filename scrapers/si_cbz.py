"""Scraper for Slovenia CBZ (Centralna baza zdravil) medicine shortage data."""

import io
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class SiCbzScraper(BaseScraper):
    """Scraper for CBZ (Central Medicine Database) shortage data.

    Downloads the full medicine register CSV and filters for medicines
    with active supply disruptions (market presence codes 3-6).
    """

    CSV_URL = "https://www.cbz.si/cbz2/sif22.csv"

    # Market presence codes indicating supply issues
    SHORTAGE_CODES = {3, 4, 5, 6}

    # Status mapping from market presence codes
    STATUS_MAP = {
        3: "upcoming",     # Napovedana motnja v preskrbi (Announced disruption)
        4: "upcoming",     # Napovedano začasno prenehanje (Announced temporary stop)
        5: "shortage",     # Potekajoča motnja v preskrbi (Ongoing disruption)
        6: "shortage",     # Potekajoče začasno prenehanje (Ongoing temporary stop)
    }

    def __init__(self):
        super().__init__(
            country_code="SI",
            country_name="Slovenia",
            source_name="CBZ",
            base_url="https://www.cbz.si",
        )

    JAZMP_URL = ("https://www.jazmp.si/fileadmin/datoteke/seznami/SFE/Prisotnost/"
                 "Seznam_24_HUM_prenehanja_motnje.xlsx")

    @staticmethod
    def _dag(waarde) -> str:
        """Eerste tien tekens, maar alleen als het een geloofwaardig jaartal is.

        JAZMP gebruikt 2126-12-31 als 'einde onbekend'. Die waarde ongefilterd doorlaten
        levert een tekort op dat een eeuw duurt.
        """
        t = str(waarde or "").strip()[:10]
        if len(t) != 10 or not t[:4].isdigit():
            return ""
        jaar = int(t[:4])
        return t if 1990 <= jaar <= datetime.now().year + 5 else ""

    def _haal_jazmp(self) -> dict:
        """Nationale code -> {start, eind, ontvangen} uit het JAZMP-meldingenbestand.

        Twee dingen om op te letten. De BLADNAAM bevat een datum ('Zadnja obvestila
        21-sep-2026') en verandert dus elke week; we zoeken op het voorvoegsel. En het
        bestand bevat ALLE meldingstypes, ook 'eerste marktintroductie' -- blind koppelen
        zou een marktintroductie als tekortstart neerzetten. We houden alleen meldingen
        over een motnja (verstoring) of een prenehanje (staking), en per code de laatste.
        """
        try:
            r = requests.get(self.JAZMP_URL, timeout=120,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            xl = pd.ExcelFile(io.BytesIO(r.content))
            blad = next((b for b in xl.sheet_names if b.lower().startswith("zadnja obvestila")),
                        xl.sheet_names[0])
            d = pd.read_excel(xl, blad, dtype=str).fillna("")
            d = d[d["Vrsta obvestila"].str.contains("motnj|prenehanj", case=False, na=False)]
            d = d.sort_values("Datum prejema obvestila")
            # Expliciet op kolomNAAM, niet op positie: itertuples hernoemt kolommen met
            # diakrieten tot _0/_1/... en dan schuift alles stil op zodra JAZMP een kolom
            # invoegt. Ontbreekt een verwachte kolom, dan stoppen we liever met een melding.
            nodig = ("DŠ zdravila", "Datum začetka", "Napovedani datum konca",
                     "Datum prejema obvestila")
            mist = [k for k in nodig if k not in d.columns]
            if mist:
                print(f"  LET OP: JAZMP mist kolommen {mist}; geen datums")
                return {}
            uit = {}
            for _, rij in d.iterrows():
                code = str(rij["DŠ zdravila"] or "").strip().zfill(6)
                if not code or code == "000000":
                    continue
                uit[code] = {
                    "start": self._dag(rij["Datum začetka"]),
                    "eind": self._dag(rij["Napovedani datum konca"]),
                    "ontvangen": self._dag(rij["Datum prejema obvestila"]),
                }
            print(f"  JAZMP: {len(uit)} codes met een motnja-/prenehanje-melding")
            return uit
        except Exception as e:                       # noqa: BLE001
            print(f"  LET OP: JAZMP-bestand niet bruikbaar ({type(e).__name__}); geen datums")
            return {}

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.CSV_URL, timeout=120,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        print(f"  Downloaded {len(response.content) / 1024 / 1024:.1f} MB CSV")

        df_raw = pd.read_csv(
            BytesIO(response.content),
            sep=";",
            encoding="cp1252",
            low_memory=False,
        )
        # De CBZ-export heeft kolomnamen MET spaties eraan ('Nacionalna sifra '). Zonder
        # deze strip geeft row.get("Nacionalna sifra") stil None, en omdat we die waarde
        # zfill(6)-en werd dat "000000" voor alle 530 rijen -- een sleutel die nergens op
        # matcht, zonder dat er iets faalt.
        df_raw.columns = [str(c).strip() for c in df_raw.columns]
        print(f"  Total medicines in register: {len(df_raw)}")

        # Filter for active supply disruptions
        shortage = df_raw[df_raw["Šifra prisotnosti na trgu"].isin(self.SHORTAGE_CODES)].copy()
        print(f"  Medicines with supply issues: {len(shortage)}")

        # De CBZ-lijst zegt DAT een middel een leveringsprobleem heeft, maar niet sinds
        # wanneer: Slovenie stond op nul startdatums. JAZMP publiceert de onderliggende
        # meldingen wel, met datums, in een apart bestand dat op de nationale code koppelt.
        datums = self._haal_jazmp()

        records = []
        for _, row in shortage.iterrows():
            name = str(row.get("Ime zdravila", "")).strip()
            if not name or name == "nan":
                continue

            atc = str(row.get("ATC oznaka", "")).strip()
            if atc == "nan":
                atc = ""

            code = int(row.get("Šifra prisotnosti na trgu", 5))
            status = self.STATUS_MAP.get(code, "shortage")

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name,
                "full_name": str(row.get("Poimenovanje zdravila", "")).strip().replace("nan", ""),
                "active_substance": str(row.get("Latinski opis ATC", "")).strip().replace("nan", ""),
                "strength": "",
                "package_size": str(row.get("Pakiranje", "")).strip().replace("nan", ""),
                "dosage_form": str(row.get("Slovenski naziv farmacevtske oblike", "")).strip().replace("nan", ""),
                "atc_code": atc,
                "marketing_auth_holder": str(row.get("Naziv imetnika dovoljenja", "")).strip().replace("nan", ""),
                "market_status": str(row.get("Naziv prisotnosti na trgu", "")).strip().replace("nan", ""),
                "market_status_code": code,
                "product_no": (str(row.get("Nacionalna šifra", "")).strip().replace("nan", "").zfill(6)
                               if str(row.get("Nacionalna šifra", "")).strip() not in ("", "nan") else ""),
                "shortage_start": datums.get(str(row.get("Nacionalna šifra", "")).strip().zfill(6), {}).get("start", ""),
                "estimated_end": datums.get(str(row.get("Nacionalna šifra", "")).strip().zfill(6), {}).get("eind", ""),
                "last_updated": datums.get(str(row.get("Nacionalna šifra", "")).strip().zfill(6), {}).get("ontvangen", ""),
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        gev = int((df["shortage_start"].astype(str).str.strip() != "").sum()) if len(df) else 0
        print(f"  Startdatum uit JAZMP: {gev}/{len(df)}")
        print(f"  Total: {len(df)} shortage records scraped")
        return df
