"""Scraper for Greece EOF shortage list (ΛΙΣΤΑ ... ΠΕΡΙΟΡΙΣΜΕΝΗΣ ΔΙΑΘΕΣΙΜΟΤΗΤΑΣ).

EOF publiceert de lijst maandelijks als PDF. De HTML-pagina eparkeia-agoras waar de
oude scraper naar keek is GEEN goede ingang: die linkte in september 2026 nog naar de
lijst van 30 april (vijf maanden oud) en zet de href bovendien zonder aanhalingstekens
neer, waardoor het linkzoeken faalde.

We gaan daarom via de open WordPress-media-API van eof.gr en pakken het NIEUWSTE
PDF-bestand op uploaddatum. Zoeken gebeurt op het stabiele deel van de bestandsnaam
(ΠΕΡΙΟΡΙΣΜΕΝΗΣ-ΔΙΑΘΕΣΙΜΟΤΗΤΑΣ); de rest van de naam wisselt per maand van spelling.
Selecteren op datum en niet op bestandsnaam is nodig omdat dezelfde maand soms
meermaals wordt geupload met een -1/-2-suffix.

De PDF heeft een echte tekstlaag (geen scan) met tien kolommen, waaronder ATC,
vergunninghouder, start- en einddatum.
"""

import re
import tempfile
from datetime import datetime

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper


class GrEofScraper(BaseScraper):
    """Scraper for EOF (National Organization for Medicines) shortage PDF."""

    MEDIA_API = "https://www.eof.gr/wp-json/wp/v2/media"
    ZOEKTERM = "ΠΕΡΙΟΡΙΣΜΕΝΗΣ-ΔΙΑΘΕΣΙΜΟΤΗΤΑΣ"
    BARCODE = re.compile(r"^\d{13}$")

    def __init__(self):
        super().__init__(
            country_code="GR",
            country_name="Greece",
            source_name="EOF",
            base_url="https://www.eof.gr",
        )
        self.headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/120.0 Safari/537.36"}

    def _nieuwste_pdf(self) -> str:
        """URL van de meest recent geuploade tekortenlijst."""
        resp = requests.get(self.MEDIA_API, timeout=60, headers=self.headers, params={
            "search": self.ZOEKTERM, "per_page": 10, "orderby": "date", "order": "desc",
            "_fields": "id,date,source_url,mime_type"})
        resp.raise_for_status()
        for m in resp.json() or []:
            if m.get("mime_type") == "application/pdf" and m.get("source_url"):
                print(f"  nieuwste lijst: {m['date'][:10]}")
                return m["source_url"]
        raise RuntimeError("Geen tekorten-PDF gevonden in de EOF-mediabibliotheek")

    @staticmethod
    def _parse_date(waarde) -> str:
        """DD/MM/YY -> JJJJ-MM-DD; leeg bij een onherkenbare waarde."""
        waarde = str(waarde or "").strip()
        if not waarde:
            return ""
        for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(waarde, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        import pdfplumber

        url = self._nieuwste_pdf()
        pdf_bytes = requests.get(url, timeout=120, headers=self.headers).content
        print(f"  PDF: {len(pdf_bytes)} bytes")

        records = []
        with tempfile.NamedTemporaryFile(suffix=".pdf") as fh:
            fh.write(pdf_bytes)
            fh.flush()
            with pdfplumber.open(fh.name) as pdf:
                for bladzijde in pdf.pages:
                    for tabel in bladzijde.extract_tables() or []:
                        for rij in tabel:
                            # Een datarij herken je aan de 13-cijferige barcode vooraan;
                            # kop- en vervolgregels vallen daarmee vanzelf af.
                            cel = str(rij[0] or "").strip().replace("\n", "")
                            if not self.BARCODE.match(cel):
                                continue
                            k = [str(c or "").replace("\n", " ").strip() for c in rij]
                            k += [""] * (10 - len(k))
                            records.append({
                                "country_code": self.country_code,
                                "country_name": self.country_name,
                                "source": self.source_name,
                                "product_no": cel,
                                "medicine_name": k[1],
                                "atc_code": k[2].upper(),
                                "active_substance": k[3],
                                "package_size": "",
                                "strength": "",
                                "marketing_auth_holder": k[5],
                                "shortage_start": self._parse_date(k[6]),
                                "estimated_end": self._parse_date(k[7]),
                                "reason": k[8],
                                "notes": k[9],
                                "status": "shortage",
                                "scraped_at": datetime.now().isoformat(),
                            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
