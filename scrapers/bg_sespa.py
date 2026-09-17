"""Scraper for Bulgaria SESPA shortage list (Списък на ЛП с установен недостиг).

Bron: Ministerie van Volksgezondheid / Изпълнителна агенция по лекарствата (IAL),
publiek Oracle-APEX-rapport op https://sespa.mh.government.bg/sespa/f?p=100 (geen login).
Kolommen: registratiecode | naam | INN (stof, Engels) | hoeveelheid | eenheid | vorm | verpakking.
Geen ATC of datums in de bron -> ATC wordt afgeleid uit de INN (enrich_atc_llm).
"""
from __future__ import annotations

import html as _html
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class BgSespaScraper(BaseScraper):
    """SESPA publieke lijst van geneesmiddelen met vastgesteld tekort (APEX f?p=100)."""

    URL = "https://sespa.mh.government.bg/sespa/f?p=100"

    def __init__(self):
        super().__init__(
            country_code="BG",
            country_name="Bulgaria",
            source_name="SESPA",
            base_url="https://sespa.mh.government.bg",
        )

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        sess = requests.Session()
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                   "Accept-Language": "bg,en;q=0.9"}
        # SESPA's certificaatketen mist soms een intermediate -> val bij een SSL-fout terug op
        # verify=False (publieke, read-only overheidslijst; geen gevoelige data).
        import urllib3
        resp = None
        for verify in (True, False):
            try:
                if not verify:
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    print("  SSL-verificatie faalde -> terugval op verify=False")
                resp = sess.get(self.URL, timeout=60, headers=headers, verify=verify)
                resp.raise_for_status()
                break
            except requests.exceptions.SSLError:
                continue
        if resp is None:
            raise RuntimeError("SESPA niet bereikbaar (SSL)")
        soup = BeautifulSoup(resp.text, "lxml")

        # Kies de datatabel: die met de INN-kolom (of de productnaam-kop)
        table = None
        for t in soup.find_all("table"):
            heads = " ".join(th.get_text(" ", strip=True) for th in t.find_all("th"))
            if "INN" in heads or "лекарствения продукт" in heads:
                table = t
                break
        if table is None:
            raise RuntimeError("SESPA-tekortentabel niet gevonden (structuur gewijzigd?)")

        records = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 6:
                continue
            c = [_html.unescape(td.get_text(" ", strip=True)) for td in tds]
            code, name, inn, qty, unit, form = c[0], c[1], c[2], c[3], c[4], c[5]
            pkg = c[6] if len(c) > 6 else ""
            if not name.strip():
                continue
            strength = qty if unit.strip() in ("", "-") else f"{qty} {unit}"
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": name.strip(),
                "active_substance": inn.strip(),
                "strength": strength.strip(),
                "dosage_form": form.strip(),
                "package_size": pkg.strip(),
                "product_no": code.strip(),
                "shortage_start": "",
                "estimated_end": "",
                "status": "shortage",
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
