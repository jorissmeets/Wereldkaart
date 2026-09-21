"""Scraper for Saudi Arabia SFDA — lijst 'Currently in shortage'.

De Excel-export (GetExcel.php?ftype=CurrentlyInShortage) geeft sinds ~sep-2026 een 404
en komt niet terug; ook de JSON-route is dicht (401 MISSING_AUTHORIZATION_HEADER).
Wat er nog wel publiek is, is een server-rendered HTML-tabel:

    https://www.sfda.gov.sa/en/currentlyInShortageList?page=N     (N vanaf 0, 10 rijen)

LET OP, dit is een duidelijke verschraling ten opzichte van de oude Excel. De tabel
heeft nog maar vier kolommen: stofnaam, beschikbaarheidsstatus, aantal merknamen en
een detaillink. ATC-code, start- en einddatum, vergunninghouder, sterkte, vorm en de
reden van het tekort zijn NIET meer publiek beschikbaar. De ATC wordt dus verderop in
de pijplijn uit de stofnaam afgeleid.

Twee valkuilen: de parameter heet `page` (`pg` wordt stilzwijgend genegeerd en geeft
altijd pagina 0 terug), en het einde van de lijst herken je niet aan de statuscode maar
aan het ONTBREKEN van een tabel — voorbij de laatste pagina blijft het HTTP 200.
"""

import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class SaSfdaScraper(BaseScraper):
    """SFDA shortage list via de publieke HTML-tabel."""

    LIST_URL = "https://www.sfda.gov.sa/en/currentlyInShortageList"
    MAX_PAGES = 200          # ruime bovengrens; we stoppen op de eerste pagina zonder tabel

    def __init__(self):
        super().__init__(
            country_code="SA",
            country_name="Saudi Arabia",
            source_name="SFDA",
            base_url="https://www.sfda.gov.sa",
        )

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "en,ar;q=0.8",
        })

        records = []
        for bladzijde in range(self.MAX_PAGES):
            try:
                resp = sess.get(self.LIST_URL, params={"page": bladzijde}, timeout=60)
                resp.raise_for_status()
            except Exception as e:
                print(f"  pagina {bladzijde}: {str(e)[:70]} — stop")
                break

            tabel = BeautifulSoup(resp.text, "html.parser").find("table")
            if tabel is None:
                break                                  # voorbij de laatste pagina

            rijen = [r for r in tabel.find_all("tr") if r.find_all("td")]
            if not rijen:
                break
            for r in rijen:
                cellen = [c.get_text(" ", strip=True) for c in r.find_all("td")]
                stof = cellen[0] if cellen else ""
                if not stof or stof.lower() == "generic name":
                    continue
                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": "",
                    "active_substance": stof,
                    "strength": "",
                    "package_size": "",
                    "product_no": "",
                    "shortage_start": "",
                    "estimated_end": "",
                    "status": "shortage",
                    # De status staat ook op de Engelse pagina in het Arabisch en is per
                    # lijst constant ('niet beschikbaar'), dus alleen als notitie bewaard.
                    "notes": cellen[1] if len(cellen) > 1 else "",
                    "scraped_at": datetime.now().isoformat(),
                })
            if bladzijde and bladzijde % 20 == 0:
                print(f"  ... {bladzijde} pagina's, {len(records)} rijen")
            time.sleep(0.2)

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
