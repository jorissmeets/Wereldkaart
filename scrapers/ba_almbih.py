"""Scraper voor Bosnië en Herzegovina — ALMBIH, Evidencija nestašice lijekova.

WAT DIT IS
----------
Het register waarin vergunninghouders een onderbreking van de levering melden aan het
Agentschap voor geneesmiddelen en medische hulpmiddelen van BiH. Per melding: vergunning-
houder, product (met vergunningsnummer), ATC, INN, fabrikant, meldingsdatum, soort
onderbreking, reden, toelichting en de verwachte periode ("Od 01.11.2025. do 31.01.2026").
Het is een meldingenlog, geen momentopname: een melding verdwijnt niet als het tekort
voorbij is. Daarom leidt deze scraper de status af uit soort en periode:

  - 'privremeni' (tijdelijk) en 'Povlačenje serije' (terugroeping van een partij):
    einde verstreken -> resolved; einde in de toekomst of 'nepoznato' -> shortage.
    De bron kent geen werkelijke einddatum; de verwachte is het enige wat er is. Een
    verlenging komt als nieuwe melding binnen.
  - 'trajni' (blijvend), niet verlengde of ingetrokken vergunning, of periode 'do trajno'
    -> discontinued.

BRON
----
https://lijekovi.almbih.gov.ba/EvidencijaNestasiceLijekova.aspx — ASP.NET WebForms-grid,
100 rijen per pagina, bladeren via __doPostBack. Publiek, geen inlog, geen robots.txt,
geen gebruiksvoorwaarden op de site gevonden (14-11-2026).

De pagina meldt het totaal ("Page 1 of 2 (173 items)"). Komt het aantal gelezen rijen daar
niet mee overeen, dan weigert de scraper: een halve lijst ziet er op de kaart hetzelfde uit
als een rustige maand.
"""

import html
import re
from datetime import date, datetime

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper


class BaAlmbihScraper(BaseScraper):
    URL = "https://lijekovi.almbih.gov.ba/EvidencijaNestasiceLijekova.aspx"
    KOLOMMEN = ["Nosilac Dozvole", "Lijek", "ATC", "INN", "Proizvođač",
                "Datum dostavljanja obavještenja", "Vrsta obustave", "Razlog obustave",
                "Obrazloženje", "Očekivano razdoblje trajanja nestašice"]

    def __init__(self):
        super().__init__(
            country_code="BA",
            country_name="Bosnia and Herzegovina",
            source_name="ALMBIH",
            base_url="https://lijekovi.almbih.gov.ba",
        )

    @staticmethod
    def _datum(s: str) -> str | None:
        m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s or "")
        if not m:
            return None
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
        except ValueError:
            return None

    @staticmethod
    def _rijen(pagina: str) -> list[list[str]]:
        rijen = []
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", pagina, re.S):
            cellen = [re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", td))).strip()
                      for td in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
            if len(cellen) == 10 and any(cellen) and cellen[0] != "Nosilac Dozvole":
                rijen.append(cellen)
        return rijen

    @staticmethod
    def _formulier(pagina: str) -> dict:
        velden = re.findall(r'<input type="hidden" name="([^"]+)" id="[^"]*" value="([^"]*)"', pagina)
        return {k: html.unescape(v) for k, v in velden}

    def _status(self, vrsta: str, periode: str, einde: str | None) -> str:
        v = vrsta.lower()
        if ("trajni" in v or "nepodnošenju zahtjeva za obnovu" in v or "ukidanje dozvole" in v
                or "trajno" in periode.lower()):
            return "discontinued"
        if einde and einde < date.today().isoformat():
            return "resolved"
        return "shortage"

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0"
        pagina = s.get(self.URL, timeout=60).text

        kop = re.search(r"Page (\d+) of (\d+) \((\d+) items\)", pagina)
        if not kop:
            raise RuntimeError("paginatelling niet gevonden; is de grid veranderd?")
        n_pag, totaal = int(kop.group(2)), int(kop.group(3))

        alle = self._rijen(pagina)
        for nr in range(2, n_pag + 1):
            doel = re.search(r"__doPostBack\(&#39;([^&]+_top_%d)&#39;" % nr, pagina)
            if not doel:
                raise RuntimeError(f"link naar pagina {nr} niet gevonden")
            formulier = self._formulier(pagina)
            formulier.update({"__EVENTTARGET": html.unescape(doel.group(1)), "__EVENTARGUMENT": ""})
            pagina = s.post(self.URL, data=formulier, timeout=60).text
            if f"Page {nr} of {n_pag}" not in pagina:
                raise RuntimeError(f"pagina {nr} kwam niet terug")
            alle += self._rijen(pagina)
            print(f"  Pagina {nr}/{n_pag}: totaal {len(alle)} rijen")

        if len(alle) != totaal:
            raise RuntimeError(f"{len(alle)} rijen gelezen, de bron meldt er {totaal}; onvolledig, niet opgeslagen")

        records = []
        nu = datetime.now().isoformat()
        for mah, lijek, atc, inn, proizv, gemeld, vrsta, razlog, obrazl, periode in alle:
            vergunning = re.search(r"\(([0-9][0-9./-]+)\)\s*$", lijek)
            naam = lijek[:vergunning.start()].strip() if vergunning else lijek
            delen = re.split(r"\s+do\s+", periode, maxsplit=1)
            start = self._datum(delen[0])
            einde = self._datum(delen[1]) if len(delen) > 1 else None
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": naam,
                "active_substance": inn,
                "strength": "",
                "package_size": "",
                "authorisation_number": vergunning.group(1) if vergunning else "",
                "atc_code": atc,
                "marketing_auth_holder": mah,
                "manufacturer": proizv,
                "notification_date": self._datum(gemeld),
                "shortage_start": start,
                "estimated_end": einde,
                "shortage_period": periode,
                "status": self._status(vrsta, periode, einde),
                "interruption_type": vrsta,
                "reason": razlog,
                "notes": obrazl,
                "scraped_at": nu,
            })
        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} records ({df['status'].value_counts().to_dict()})")
        return df
