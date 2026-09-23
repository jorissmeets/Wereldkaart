"""Scraper for Denmark Lægemiddelstyrelsen shortage data."""

import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class DkLmstScraper(BaseScraper):
    """Scraper for Lægemiddelstyrelsen medicine shortage notices."""

    URL = "https://laegemiddelstyrelsen.dk/da/godkendelse/kontrol-og-inspektion/mangel-paa-medicin/meddelelser-om-forsyning-af-medicin/"
    PAGE_SIZE = 1000          # ruim boven het totaal: alles in een enkele call

    # GUIDs for table fields (from data-data attribute)
    FIELD_PRODUCT = "{4BE8272E-F07F-4CD6-BDEB-D175115B5B47}"
    FIELD_PERIOD = "{D05F2686-DFE8-4E1F-BD45-4D48E7D9A266}"
    FIELD_REASON = "{27F80008-4F2B-4D25-8520-AF8681A909BB}"

    # Deense maandnamen voor het parsen van 'expected_period' (bv. "Start juli - slut september 2026")
    DK_MONTHS = {
        "januar": 1, "februar": 2, "marts": 3, "april": 4, "maj": 5, "juni": 6,
        "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "december": 12,
    }

    # De lege datum die het Sitecore-CMS teruggeeft als er geen publicatiedatum is gezet
    # (.NET DateTime.MinValue). Geen datum dus -- geen 1 januari van het jaar 1.
    NULL_DATE = "0001-01-01"

    def __init__(self):
        super().__init__(
            country_code="DK",
            country_name="Denmark",
            source_name="LMST",
            base_url="https://laegemiddelstyrelsen.dk",
        )
        self.source_total: int | None = None

    def _is_template(self, item: dict, table_data: dict) -> bool:
        """Is dit een leeg CMS-sjabloon in plaats van een tekortmelding?

        De lijst van Lægemiddelstyrelsen bevat vier redactiepagina's die nooit gevuld zijn
        maar wel in de dynamiclists-API meekomen. Ze zijn aan drie onafhankelijke kenmerken
        te herkennen, die alle drie exact dezelfde vier records aanwijzen (meting 23-09 op
        366 live records):

          1. de titel/URL noemt zichzelf sjabloon: "AA SKABELON - KAN DUPLIKERES";
          2. de titel is "<product>; forsyningsvanskelighed" met een LEEG product, dus een
             titel die met ';' begint (slug wordt dan '/-forsyningsvanskelighed/'): 3 stuks,
             voor human-2025, human-2026 en veterinaer-2025;
          3. het reden-veld bevat nog de hele ONGEKOZEN keuzelijst ("Produktionsproblemer /
             Øget salg/efterspørgsel / Leveringsvanskeligheder / Kommercielle årsager /
             Forsinkelser hos fremstiller"): 5 schuine strepen, terwijl een ingevulde reden
             er 0 heeft of 1 (alleen "Øget salg/efterspørgsel" heeft er intern een).

        BEWUST NIET gefilterd op een ontbrekende datum of een leeg veld op zich:
          - "Sulfasalazin \"Hexal\" 500 mg enterotabletter" heeft ook datum 0001-01-01 maar
            is een ECHTE melding (stof Sulfasalazin, ATC A07EC01, Sandoz A/S, permanent
            markedsophør) -- die hoort te blijven;
          - Previcox en Curamox Vet. (2021) hebben een leeg productveld in de lijst terwijl
            de detailpagina wel product, stof en ATC noemt -- die horen ook te blijven.
        """
        naam = (item.get("name") or "").strip()
        url = (item.get("url") or "").lower()
        reden = (table_data.get(self.FIELD_REASON) or "").strip()

        if "skabelon" in naam.lower() or "skabelon" in url:
            return True
        if naam.startswith(";"):
            return True
        if reden.count("/") >= 3:        # ongekozen keuzelijst i.p.v. één reden
            return True
        return False

    def _scrape_detail_substance(self, detail_url: str) -> str:
        """Scrape active substance from a detail page."""
        if not detail_url:
            return ""
        url = detail_url if detail_url.startswith("http") else f"{self.base_url}{detail_url}"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # Look for "Aktivt stof:" or "Indholdsstof:" or "Active substance:" patterns
            for pattern in (
                r"[Aa]ktivt?\s+stof(?:fer)?:\s*([^\n.;]+)",
                r"[Ii]ndholdsstof(?:fer)?:\s*([^\n.;]+)",
                r"[Aa]ctive\s+substance:\s*([^\n.;]+)",
                r"[Gg]enerisk\s+navn:\s*([^\n.;]+)",
            ):
                m = re.search(pattern, text)
                if m:
                    substance = m.group(1).strip()
                    if substance and len(substance) >= 3:
                        return substance
        except Exception:
            pass
        return ""

    def _fetch_all(self) -> list:
        """Haal ALLE actuele meldingen in een keer op via de dynamiclists-API.

        De pagina zelf rendert altijd maar de eerste 20 rijen: zowel `page` als `pageSize`
        in de URL worden door de server genegeerd (een eerdere versie van deze scraper
        dacht daardoor te pagineren en haalde 19x dezelfde 20 records op). Het doorbladeren
        gebeurt in werkelijkheid door een Vue-app die POST naar
        /content/api/dynamiclists/{listId}.

        Twee dingen zijn daarbij essentieel:
          - `filterQueries` moet als LIJST van {key, value} worden gestuurd, terwijl de
            pagina het als object meegeeft. Stuur je het object, dan antwoordt de server
            met een redirect naar de foutpagina.
          - De headers RequestVerificationToken / SitecoreId / SitecoreLanguage komen uit
            de data-attributen van het app-element.

        Met pageSize ruim boven het totaal komt alles in een enkele call binnen.
        """
        sess = requests.Session()
        sess.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                                           "Chrome/120.0 Safari/537.36"})
        pagina = sess.get(self.URL, timeout=60)
        pagina.raise_for_status()
        el = BeautifulSoup(pagina.text, "lxml").find(attrs={"data-results": True})
        if el is None:
            raise RuntimeError("dynamiclist-app niet gevonden op de LMST-pagina")

        query = json.loads(el.get("data-query", "{}"))
        query["filterQueries"] = [{"key": k, "value": v}
                                  for k, v in (query.get("filterQueries") or {}).items()]
        query["pagingOptions"] = {"pageSize": self.PAGE_SIZE, "page": 0, "disablePaging": False}

        lijst_id = json.loads(el.get("data-data", "{}")).get("id")
        resp = sess.post(
            f"{self.base_url}/content/api/dynamiclists/{lijst_id}",
            headers={"RequestVerificationToken": el.get("data-antiforgery-token", ""),
                     "SitecoreId": el.get("data-id", ""),
                     "SitecoreLanguage": el.get("data-lang", "da"),
                     "Accept": "application/json",
                     "Content-Type": "application/json"},
            json=query, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        teller = data.get("counter") or {}
        if teller:
            print(f"  bron meldt: {teller.get('itemCount')}")
            self.source_total = teller.get("totalResults")
        return data.get("results") or []

    @classmethod
    def _pub_to_date(cls, s: str) -> str:
        """ISO-timestamp (2026-07-22T13:14:00Z) -> 2026-07-22.

        De CMS-nuldatum 0001-01-01 betekent 'geen publicatiedatum gezet' en wordt leeg
        teruggegeven. Zou je hem laten staan, dan leest de kaart hem als een startdatum van
        ruim tweeduizend jaar geleden en verbergt de >1-jaar-inactiefregel de melding.
        Leeg laten is hier het eerlijke antwoord; een scrapedatum invullen is dat niet.
        """
        s = (s or "").strip()
        if s.startswith(cls.NULL_DATE):
            return ""
        return s[:10] if len(s) >= 10 and s[4:5] == "-" else ""

    def _period_end(self, period: str) -> str:
        """Leid een geschatte einddatum af uit de Deense periode-tekst.

        "Start juli - slut september 2026" -> 2026-09-25 ; "Fra midt oktober 2026" (alleen
        start) -> "" ; "... - ukendt" (einde onbekend) -> "".
        """
        p = (period or "").strip().lower()
        if not p:
            return ""
        end_part = p.split("-")[-1].strip() if "-" in p else p
        if "ukendt" in end_part:
            return ""
        if "-" not in p and p.startswith("fra"):
            return ""  # alleen een startdatum, geen einde
        m = re.search(r"(20\d{2})", end_part) or re.search(r"(20\d{2})", p)
        if not m:
            return ""
        year = int(m.group(1))
        month = next((num for name, num in self.DK_MONTHS.items() if name in end_part), None)
        if not month:
            return ""
        day = 5 if ("start" in end_part or "begynd" in end_part) else 15 if "midt" in end_part else 25
        return f"{year:04d}-{month:02d}-{day:02d}"

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_results = self._fetch_all()
        print(f"  Downloaded {len(all_results)} records")
        if self.source_total is not None and self.source_total != len(all_results):
            print(f"  LET OP: bron noemt {self.source_total} resultaten, "
                  f"opgehaald {len(all_results)}")

        sjablonen = [it for it in all_results
                     if self._is_template(it, it.get("dynamicTableData") or {})]
        if sjablonen:
            all_results = [it for it in all_results if it not in sjablonen]
            print(f"  {len(sjablonen)} lege CMS-sjabloonpagina's overgeslagen "
                  f"(geen tekortmelding): "
                  + "; ".join(sorted((it.get("name") or "?").strip() for it in sjablonen)))

        # Batch-lookup substances from detail pages
        detail_urls = sorted({item.get("url", "") for item in all_results if item.get("url")})
        print(f"  Scraping {len(detail_urls)} detail pages for active substances...")
        # Parallel: een voor een duurde bij 364 meldingen ruim drie minuten en liep daarmee
        # tegen de time-out van de rerun-runner aan.
        with ThreadPoolExecutor(max_workers=12) as ex:
            stoffen = list(ex.map(self._scrape_detail_substance, detail_urls))
        url_substance_map: dict[str, str] = dict(zip(detail_urls, stoffen))
        found = sum(1 for v in url_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(detail_urls)} products")

        records = []
        for item in all_results:
            table_data = item.get("dynamicTableData", {})
            detail_url = item.get("url", "")
            # Ook hier de CMS-nuldatum niet doorgeven: build_data valt terug op
            # published_date als shortage_start leeg is, en zou 0001-01-01 opnieuw als
            # startdatum lezen.
            gepubliceerd = (item.get("date") or "").strip()
            if gepubliceerd.startswith(self.NULL_DATE):
                gepubliceerd = ""

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": table_data.get(self.FIELD_PRODUCT, item.get("name", "")),
                "active_substance": url_substance_map.get(detail_url, ""),
                "strength": "",
                "package_size": "",
                "product_no": "",
                "shortage_start": self._pub_to_date(gepubliceerd),
                "estimated_end": self._period_end(table_data.get(self.FIELD_PERIOD, "")),
                "expected_period": table_data.get(self.FIELD_PERIOD, ""),
                "status": "shortage",
                "reason": table_data.get(self.FIELD_REASON, ""),
                "detail_url": detail_url,
                "published_date": gepubliceerd,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
