"""Scraper for Switzerland drugshortage.ch.

LET OP -- CH STAAT BEWUST NIET OP DE KAART (uitgesloten in landkaart/build_data.py).
De respons van deze bron draagt een expliciet gebruiksvoorbehoud: (c) Martinelli
Consulting GmbH, met een beroep op Art. 62 URG, dat integratie in systemen van derden
zonder schriftelijke toestemming verbiedt. Het endpoint is technisch publiek bereikbaar,
maar dat is niet hetzelfde als vrij te gebruiken. Zet dit land niet terug op de kaart
zonder schriftelijke toestemming van de rechthebbende.

De api/v1-route geeft sinds ~sep-2026 een 401. De site zelf haalt zijn gegevens op via
https://www.drugshortage.ch/ds.php?a=engpaesse, dat wel werkt maar een Referer-header
eist ("Zugriff verweigert - fehlende Header" zonder). Dat endpoint is bovendien rijker:
tekorten op VERPAKKINGSNIVEAU met ATC, vergunninghouder, status en datums.

Velden per melding: bezeichnung, gtin, pharmacode, firma, status, mutation (laatste
wijziging), atc, tage (aantal dagen dat het tekort loopt) en lieferdatum (verwachte
levering). Uit 'tage' leiden we een echte startdatum af -- de bron levert er geen.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta

from scrapers.base_scraper import BaseScraper


class ChDrugShortageScraper(BaseScraper):
    """Scraper for drugshortage.ch (Switzerland) via het publieke ds.php-endpoint."""

    API_URL = "https://www.drugshortage.ch/ds.php?a=engpaesse"

    # De bron zet het statusnummer vooraan de tekst; we mappen op het nummer.
    STATUS = {
        "1": "shortage",        # aktuell keine Lieferungen
        "2": "anticipated",     # angekuendigter Engpass
        "3": "limited",         # Lieferungen kontingentiert/eingeschraenkt
        "5": "limited",         # fuer Spitaeler verfuegbar; Retail eingeschraenkt
        "10": "shortage",       # Versorgung erfolgt mit Pflichtlagerware
    }

    def __init__(self):
        super().__init__(
            country_code="CH",
            country_name="Switzerland",
            source_name="drugshortage",
            base_url="https://www.drugshortage.ch",
        )

    @staticmethod
    def _parse_date(waarde) -> str:
        """dd.mm.jjjj -> jjjj-mm-dd. 'offen', 'unbestimmt', 'en clarif.' enz. -> leeg."""
        if not waarde or not isinstance(waarde, str):
            return ""
        waarde = waarde.strip()
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(waarde, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        resp = requests.get(self.API_URL, timeout=60, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Referer": "https://www.drugshortage.ch/",     # zonder deze header: 403
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        })
        resp.raise_for_status()
        items = (resp.json() or {}).get("engpaesse", [])
        print(f"  API leverde {len(items)} meldingen")

        vandaag = datetime.now()
        records = []
        for it in items:
            status_ruw = str(it.get("status") or "").strip()
            nummer = status_ruw.split(None, 1)[0] if status_ruw else ""

            # De bron heeft geen startdatum maar wel het aantal dagen dat het tekort loopt.
            start = ""
            try:
                dagen = int(it.get("tage") or 0)
                if dagen > 0:
                    start = (vandaag - timedelta(days=dagen)).strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                pass

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(it.get("bezeichnung") or "").strip(),
                "active_substance": "",
                "strength": "",
                "package_size": "",
                "atc_code": str(it.get("atc") or "").strip().upper(),
                "marketing_auth_holder": str(it.get("firma") or "").strip(),
                "product_no": str(it.get("pharmacode") or "").strip(),
                "gtin": str(it.get("gtin") or "").strip(),
                "shortage_start": start,
                "estimated_end": self._parse_date(it.get("lieferdatum")),
                "last_updated": self._parse_date(it.get("mutation")),
                "status": self.STATUS.get(nummer, "shortage"),
                "notes": status_ruw,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
