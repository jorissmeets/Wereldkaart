"""Scraper for Finland FIMEA shortage data via RSS feed.

De RSS-feed is een UITGEKLEEDE weergave van dezelfde meldingen die Fimea op de
saatavuushäiriö-pagina toont. De feed geeft per melding alleen titel, ATC,
pakkauskoko, vergunninghouder en datums -- geen werkzame stof en geen sterkte.
De tabel op de pagina heeft die kolommen wél ("Vaikuttavat aineet", "Vahvuus",
"Lääkemuoto") en draait op een DataTables-endpoint met exact hetzelfde aantal
meldingen (recordsTotal == aantal RSS-items). We houden de RSS als bron van de
records -- die levert de stabiele guid en de ISO-datums -- en vullen daar de
ontbrekende velden bij uit dat endpoint. De koppeling loopt over de guid: die
blijkt de VNR-code te zijn en komt 1-op-1 overeen met de vnr-kolom van de tabel.
Valt het endpoint weg, dan blijft de RSS-uitvoer ongewijzigd werken.
"""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class FiFimeaScraper(BaseScraper):
    """Scraper for FIMEA shortage RSS feed."""

    RSS_URL = "https://fimea.fi/c/laakehaut_ja_luettelot/saatavuushairio-uusi/rss"
    PAGE_URL = "https://fimea.fi/laakehaut_ja_luettelot/saatavuushairio-uusi"

    # Kolomposities in de DataTables-respons, in de volgorde van de tabelkoppen.
    COL_NAME, COL_STRENGTH, COL_FORM = 1, 2, 3
    COL_ATC, COL_SUBSTANCE, COL_VNR = 5, 6, 13

    def __init__(self):
        super().__init__(
            country_code="FI",
            country_name="Finland",
            source_name="FIMEA",
            base_url="https://fimea.fi",
        )

    def _fetch_table_index(self) -> dict[str, dict]:
        """{vnr: {substance, strength, dosage_form}} uit het tabel-endpoint van de site.

        Geeft {} terug zodra er iets niet klopt; de RSS blijft dan de enige bron.
        Het endpoint weigert zonder de DataTables-parameters (het serveert dan de
        cookiebanner in plaats van JSON), en accepteert maximaal ~100 rijen per pagina.
        """
        index: dict[str, dict] = {}
        try:
            sessie = requests.Session()
            sessie.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept-Language": "fi-FI,fi;q=0.9",
            })
            pagina = sessie.get(self.PAGE_URL, timeout=60)
            pagina.raise_for_status()

            # Het instance-achtervoegsel van de portlet verandert bij een herinrichting
            # van de pagina; lees het uit de pagina zelf in plaats van het vast te zetten.
            match = re.search(r"(fi_yja_fimea_med_web_AvailabilityFailurePortlet_INSTANCE_\w+)",
                              pagina.text)
            if not match:
                print("  LET OP: tabel-endpoint niet gevonden op de pagina — alleen RSS-velden")
                return {}

            url = (f"{self.PAGE_URL}?p_p_id={match.group(1)}&p_p_lifecycle=2&p_p_state=normal"
                   f"&p_p_mode=view&p_p_resource_id=%2Fsaatavuushairio%2Fsearch"
                   f"&p_p_cacheability=cacheLevelPage")
            kop = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest",
                   "Referer": self.PAGE_URL}

            start, totaal, draw = 0, None, 1
            while True:
                resp = sessie.get(url, timeout=60, headers=kop, params={
                    "draw": draw, "start": start, "length": 100, "search": "",
                    "startDate": "", "endDate": "",
                    "order[0][column]": 0, "order[0][dir]": "desc",
                })
                if resp.status_code != 200 or "json" not in (resp.headers.get("content-type") or ""):
                    print(f"  LET OP: tabel-endpoint gaf {resp.status_code} "
                          f"({resp.headers.get('content-type')}) — alleen RSS-velden")
                    return {}
                blok = resp.json()
                totaal = blok.get("recordsTotal")
                rijen = blok.get("data") or []
                for rij in rijen:
                    vnr = str(rij[self.COL_VNR] or "").strip()
                    # De bron zet letterlijk "null" in de vnr-kolom als hij er geen heeft;
                    # daarop koppelen zou meldingen aan een willekeurige stof plakken.
                    if not vnr or vnr.lower() == "null":
                        continue
                    index.setdefault(vnr, {
                        "substance": str(rij[self.COL_SUBSTANCE] or "").strip(),
                        "strength": str(rij[self.COL_STRENGTH] or "").strip(),
                        "dosage_form": str(rij[self.COL_FORM] or "").strip(),
                        "atc": str(rij[self.COL_ATC] or "").strip().upper(),
                    })
                start += 100
                draw += 1
                if not rijen or (totaal is not None and start >= totaal):
                    break
            print(f"  Tabel-endpoint: {len(index)} producten met stofnaam "
                  f"(bron meldt {totaal} rijen)")
        except (requests.RequestException, ValueError, KeyError, IndexError) as exc:
            print(f"  LET OP: tabel-endpoint onbruikbaar ({type(exc).__name__}: {exc}) "
                  f"— alleen RSS-velden")
            return {}
        return index

    def _parse_dates(self, content_text):
        """Parse start/end dates from content like 'Saatavuushäiriö alkaa: 2025-12-15, saatavuushäiriö päättyy: 2026-04-06'."""
        start = None
        end = None
        m = re.search(r"alkaa:\s*(\d{4}-\d{2}-\d{2})", content_text)
        if m:
            start = m.group(1)
        m = re.search(r"p(?:ä|a)(?:ä|a)ttyy:\s*(\d{4}-\d{2}-\d{2})", content_text)
        if m:
            end = m.group(1)
        return start, end

    @staticmethod
    def _extract_substance(content: str, categories: dict) -> str:
        """Extract active substance from RSS content or categories.

        Checks for 'vaikuttavaAine' category first, then parses content for
        'Vaikuttava aine: ...' pattern.
        """
        # Check category fields
        for key in ("vaikuttavaAine", "vaikuttava_aine", "substance"):
            if key in categories and categories[key]:
                return categories[key].strip()

        # Parse from content: "Vaikuttava aine: ..." pattern
        m = re.search(r"[Vv]aikuttava\s+aine:\s*([^,<\n]+)", content)
        if m:
            return m.group(1).strip()

        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.RSS_URL, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "xml")

        items = soup.find_all("item")
        print(f"  Found {len(items)} items in RSS feed")

        tabel = self._fetch_table_index()
        atc_conflicts = 0

        records = []
        for item in items:
            title = item.find("title").get_text(strip=True) if item.find("title") else ""
            desc = item.find("description").get_text(strip=True) if item.find("description") else ""
            content_el = item.find("content:encoded") or item.find("encoded")
            content = content_el.get_text(strip=True) if content_el else ""
            categories = {c.get("domain", ""): c.get_text(strip=True) for c in item.find_all("category")}
            guid = item.find("guid").get_text(strip=True) if item.find("guid") else ""
            creator_el = item.find("dc:creator") or item.find("creator")
            creator = creator_el.get_text(strip=True) if creator_el else ""

            start, end = self._parse_dates(content)

            # guid == VNR-code; daarmee koppelen we de rijkere tabelrij aan deze melding.
            extra = tabel.get(guid, {}) if guid and guid.lower() != "null" else {}
            # Zelfcontrole: de gekoppelde rij moet dezelfde ATC hebben als de RSS-melding.
            # Verschuift Fimea ooit een kolom, dan plakken we zo geen stof van een ander
            # middel aan deze melding maar laten we het veld leeg.
            rss_atc = str(categories.get("atc", "")).strip().upper()
            if extra and rss_atc and extra.get("atc") and extra["atc"] != rss_atc:
                atc_conflicts += 1
                extra = {}

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": title,
                "active_substance": (self._extract_substance(content, categories)
                                     or extra.get("substance", "")),
                "strength": extra.get("strength", ""),
                "dosage_form": extra.get("dosage_form", ""),
                "package_size": categories.get("pakkauskoko", ""),
                "product_no": guid,
                "atc_code": categories.get("atc", ""),
                "shortage_start": start,
                "estimated_end": end,
                "status": "shortage",
                "marketing_auth_holder": creator,
                "notes": desc,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        gevuld = int(df["active_substance"].astype(str).str.strip().ne("").sum()) if len(df) else 0
        print(f"  Stofnaam gevuld voor {gevuld}/{len(df)} meldingen")
        if atc_conflicts:
            print(f"  LET OP: {atc_conflicts} koppelingen geweigerd wegens ATC-verschil "
                  f"tussen RSS en tabel — controleer de kolomvolgorde van de tabel")
        print(f"  Total: {len(df)} shortage records scraped")
        return df
