"""Scraper voor het Verenigd Koninkrijk — geneesmiddelentekorten (MSN / SSP / CAS).

WAAROM DEZE SCRAPER OPNIEUW IS GEBOUWD
--------------------------------------
De vorige versie haalde de GOV.UK-feed `drug-device-alerts.atom` op. Dat is een
recall-/defect-/hulpmiddelenveiligheidsregister, GEEN tekortenlijst. Het trefwoord
"natpsa" matchte daar National Patient Safety Alerts over hulpmiddelen, waardoor de
scraper rijen als "Patient hoists and slings" en "ResMed Astral ventilators" opleverde:
nul geneesmiddelen, nul tekorten. Dat is dezelfde fout als eerder bij LT/TR/EE — een
register verwarren met een tekortenlijst — plus het Estland-probleem van een filter dat
op de verkeerde waarde staat. De Atom-route is daarom volledig verwijderd en komt niet
terug: die bron BEVAT per definitie geen tekorten, hoe je hem ook filtert.

DRIE BRONNEN, BEWUST GECOMBINEERD
---------------------------------
1. CPE (hoofdbron, ~95 records) — Community Pharmacy England republiceert de Medicine
   Supply Notifications van DHSC, inclusief de officiele MSN-nummers.
2. NHSBSA (~90 records) — de Serious Shortage Protocols. Enige bron die HARD onderscheid
   maakt tussen lopend (Active) en afgelopen (Expired), met echte begin- en einddatums.
3. MHRA CAS (~23 records) — National Patient Safety Alerts van DHSC.

LET OP VOOR HET PROJECT (keuze voor Jesper/Nicky, niet stilzwijgend ingevuld):
CPE is GEEN toezichthouder maar de onderhandelingsorganisatie van de apotheken. Ze
republiceren wel letterlijk de DHSC-MSN's met officieel MSN-nummer. NHSBSA en CAS/MHRA
zijn wel officiele instanties. Accepteert de Landkaart uitsluitend toezichthoudersbronnen,
dan vallen de CPE-rijen weg en zakt GB naar circa 30 rijen. Het veld `source_detail` staat
er juist voor om die schifting achteraf te kunnen maken zonder opnieuw te scrapen.

WAT DEZE BRONNEN NIET LEVEREN — en dus leeg blijft
--------------------------------------------------
Geen enkele GB-bron geeft een ATC-code, een package_size of een vergunninghoudernummer.
Die velden blijven leeg. ATC hoort via de bestaande naam-naar-PRK/ATC-pijplijn te komen;
in de scraper gokken levert precies de stille fouten op waar dit project eerder op is
stukgelopen. `active_substance` zit meestal IN de medicijnnaam, maar niet als apart veld;
we splitsen het niet uit, want dat zou raden zijn.
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class GbMhraScraper(BaseScraper):
    """Verenigd Koninkrijk: DHSC-tekortmeldingen via CPE, NHSBSA-SSP's en MHRA CAS."""

    CPE_FEED_URL = "https://cpe.org.uk/our-latest-news-category/shortage/feed/"
    NHSBSA_SSP_URL = (
        "https://www.nhsbsa.nhs.uk/pharmacies-gp-practices-and-appliance-contractors/"
        "serious-shortage-protocols-ssps"
    )
    CAS_SEARCH_URL = "https://www.cas.mhra.gov.uk/SearchAlerts.aspx"
    CAS_BASE = "https://www.cas.mhra.gov.uk/"

    # ASP.NET-prefix van de CAS-zoekcontrol. Staat als constante apart omdat hij in zowel
    # de zoek-POST als in ELKE paginerings-POST opnieuw meemoet (zie _scrape_cas).
    CAS_CTL = "ctl00$ContentPlaceHolder1$AlertSearchResults1$"
    CAS_GRID = CAS_CTL + "gvwAlertList"

    # Alleen 37 = "National Patient Safety Alert - DHSC". De andere aanbieders in de
    # keuzelijst (28 = SDA, 27 = DHSC Supply Disruption Alert, 16 = DH Supply Disruption)
    # zijn DODE ARCHIEVEN: hun nieuwste alert dateert van respectievelijk 2021, 2019 en
    # 2016. Die binnenhalen zou tientallen jaren afgesloten tekorten als actuele meldingen
    # de Landkaart in duwen — de Griekenland-valkuil (oude publicatie voor actueel aanzien).
    # Wil je ze toch, zet ze hier bij en zet de status van die rijen expliciet op resolved.
    CAS_ENTITIES = ("37",)

    MAX_FEED_PAGES = 40  # ruime bovengrens; de echte stop is HTTP 404 of een lege pagina

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    MONTHS = (
        "January|February|March|April|May|June|July|August|September|October|"
        "November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
    )

    # Toedieningsvormen die letterlijk in de productnaam staan. We LEZEN ze daar alleen uit;
    # we leiden niets af wat er niet staat.
    DOSAGE_FORMS = (
        "orodispersible tablets", "modified-release tablets", "gastro-resistant capsules",
        "prolonged-release tablets", "effervescent tablets", "soluble tablets",
        "powder for solution for injection", "solution for injection", "oral solution",
        "oral suspension", "oral powder sachets", "eye drops", "nasal spray",
        "transdermal patches", "suppositories", "pessaries", "vaginal gel",
        "prefilled syringe", "pre-filled syringe", "inhaler", "injection", "infusion",
        "capsules", "tablets", "sachets", "cream", "ointment", "granules", "ampoules",
    )

    def __init__(self) -> None:
        super().__init__(
            country_code="GB",
            country_name="United Kingdom",
            source_name="MHRA",
            base_url="https://www.cas.mhra.gov.uk",
        )
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    # ─── Datumhulpjes ───────────────────────────────────────────────────────

    def _parse_date_token(self, token: str) -> str | None:
        """Parse een enkele datumnotatie naar ISO. Geeft None als het niet lukt.

        De GB-bronnen hanteren door elkaar heen "25 August 2026", "19th December 2024",
        "01/09/2026" en (in CAS) "08-Apr-2026". De ordinale achtervoegsels moeten eerst
        weg, anders faalt strptime op alle formaten.
        """
        token = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", token.strip(), flags=re.I)
        token = token.replace("Sept ", "Sep ").strip(" .,;")
        for fmt in ("%d %B %Y", "%d %b %Y", "%d/%m/%Y", "%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(token, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _find_dates(self, text: str) -> list[str]:
        """Geef ALLE datums in een stuk tekst terug, op volgorde van voorkomen.

        Nodig omdat brondvelden vaak meer dan een datum bevatten:
        "16/06/2025 (updated 14/08/2025)" en "30 October 2026 This SSP was amended on
        23 July 2026". De eerste datum is steeds de datum die het veld bedoelt; de rest is
        een wijzigingsdatum die in last_updated hoort. Pak je hier klakkeloos het hele veld,
        dan faalt strptime en verlies je de datum volledig.
        """
        pattern = re.compile(
            rf"\b(\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{self.MONTHS})\s+\d{{4}}"
            rf"|\d{{1,2}}/\d{{1,2}}/\d{{4}}"
            rf"|\d{{1,2}}-(?:{self.MONTHS})-\d{{4}})\b",
            re.I,
        )
        out: list[str] = []
        for m in pattern.finditer(text or ""):
            iso = self._parse_date_token(m.group(1))
            if iso:
                out.append(iso)
        return out

    def _first_date(self, text: str) -> str | None:
        dates = self._find_dates(text)
        return dates[0] if dates else None

    @staticmethod
    def _norm_key(name: str) -> str:
        """Sleutel om dezelfde melding uit twee bronnen te herkennen."""
        s = (name or "").lower().replace("®", " ").replace(" ", " ")
        s = re.sub(r"[^a-z0-9]+", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    def _dosage_form(self, name: str) -> str:
        low = (name or "").lower()
        for form in self.DOSAGE_FORMS:  # langste vormen staan vooraan in de tuple
            if form in low:
                return form
        return ""

    def _blank(self) -> dict:
        """Lege recordvorm. Alle kolommen die build_data verwacht staan hier, zodat een
        ontbrekend veld altijd leeg is en nooit per ongeluk van een ander record erft."""
        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "source_detail": "",
            "medicine_name": "",
            "active_substance": "",
            "strength": "",
            "dosage_form": "",
            "package_size": "",
            "product_no": "",
            "atc_code": "",
            "marketing_auth_holder": "",
            "shortage_start": None,
            "estimated_end": None,
            "estimated_end_text": "",
            "actual_end": None,
            "status": "",
            "severity": "",
            "reason": "",
            "last_updated": None,
            "url": "",
            "scraped_at": datetime.now().isoformat(),
        }

    # ─── Bron 1: CPE-feed met de DHSC Medicine Supply Notifications ──────────

    def _scrape_cpe(self) -> list[dict]:
        """Loop de RSS-feed door en zet elke MSN om in een record.

        PAGINERING: ?paged=N werkt hier echt (pagina 2 bevat andere items dan pagina 1;
        gecontroleerd op unieke links). Twee valkuilen:
          * ?paged=1 geeft een 301 naar de feed zonder parameter — dat is correct en moet
            gevolgd worden; vraag pagina 1 daarom zonder parameter op.
          * de feed eindigt met een HTTP 404 na de laatste pagina. Stop DAAROP, niet op een
            hardgecodeerd aantal pagina's, anders mis je records zodra DHSC er meer publiceert.
        """
        records: list[dict] = []
        seen_links: set[str] = set()

        for page in range(1, self.MAX_FEED_PAGES + 1):
            url = self.CPE_FEED_URL if page == 1 else f"{self.CPE_FEED_URL}?paged={page}"
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                break
            resp.raise_for_status()

            items = BeautifulSoup(resp.text, "lxml-xml").find_all("item")
            if not items:
                break

            new_on_page = 0
            for item in items:
                link = item.find("link").get_text(strip=True) if item.find("link") else ""
                # Dubbele links betekenen dat de server onze paginaparameter negeert
                # (de Denemarken-valkuil). Dan stoppen we, want doorgaan levert alleen
                # kopieen van pagina 1 op.
                if link and link in seen_links:
                    continue
                if link:
                    seen_links.add(link)
                new_on_page += 1

                title = item.find("title").get_text(strip=True) if item.find("title") else ""
                # Vangt zowel "Medicine Supply Notification: X" als
                # "Updated Medicine Supply Notification: X". De feed bevat daarnaast
                # nieuwsberichten en SSP-aankondigingen; die zijn geen meldingen op zich.
                if "medicine supply notification" not in title.lower():
                    continue

                encoded = item.find("encoded")
                body = (
                    BeautifulSoup(encoded.get_text(), "lxml").get_text("\n", strip=True)
                    if encoded
                    else ""
                )
                records.append(self._parse_cpe_item(title, body, link, item))

            if new_on_page == 0:
                break

        return records

    def _parse_cpe_item(self, title: str, body: str, link: str, item) -> dict:
        rec = self._blank()
        rec["source_detail"] = "CPE (DHSC Medicine Supply Notification)"
        rec["url"] = link

        # Alles na de eerste dubbele punt is de productnaam; het achtervoegsel "- Updated"
        # hoort bij de melding, niet bij het product.
        name = title.split(":", 1)[1] if ":" in title else title
        name = re.sub(r"\s*[–—-]\s*updated\s*$", "", name, flags=re.I).strip()
        rec["medicine_name"] = name
        rec["dosage_form"] = self._dosage_form(name)

        msn = re.search(r"MSN/\d{4}/\d+", body)
        if msn:
            rec["product_no"] = msn.group(0)  # 2 van de 95 hebben geen MSN-nummer: leeg laten

        # "Date of issue:" is de ENIGE juiste startdatum. Gebruik NOOIT de RSS-pubDate
        # (dat is het moment dat CPE het bericht plaatste, soms dagen later) en al helemaal
        # nooit de scrapedatum — precies die fout zit in de AT- en DK-scrapers.
        issue = re.search(r"Date of issue:\s*([^\n]+)", body, re.I)
        if issue:
            dates = self._find_dates(issue.group(1))
            if dates:
                rec["shortage_start"] = dates[0]
                if len(dates) > 1:
                    rec["last_updated"] = dates[-1]  # "(updated 14/08/2025)"

        if not rec["last_updated"]:
            pub = item.find("pubDate")
            if pub:
                try:
                    rec["last_updated"] = datetime.strptime(
                        pub.get_text(strip=True)[:25].strip(), "%a, %d %b %Y %H:%M:%S"
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        tier = re.search(r"Tier\s*([1-4])\s*[–—-]?\s*([a-z ]*impact)?", body, re.I)
        if tier:
            # Tier is een ernstgraad, GEEN status. In status zetten zou de statuslogica
            # van build_data breken (die kent alleen shortage/resolved/...).
            # CPE schrijft nu eens "Tier 2 - Medium impact" en dan weer "medium impact".
            # Zonder lower() levert dat vier verschillende severity-waarden op voor twee
            # begrippen, en valt elke groepering op severity in het dashboard uiteen.
            rec["severity"] = f"Tier {tier.group(1)}" + (
                f" - {tier.group(2).strip().lower()}" if tier.group(2) else ""
            )

        reason = re.search(
            r"\b(?:due to|because of|as a result of)\s+([^.\n]{10,200})", body, re.I
        )
        if reason:
            rec["reason"] = reason.group(1).strip()

        # estimated_end: zie _note_estimated_end. CPE noemt alleen maand-niveau
        # ("until early January 2027"), nooit een dag.
        self._note_estimated_end(rec, body)

        rec["status"] = "shortage"
        return rec

    def _note_estimated_end(self, rec: dict, body: str) -> None:
        """Bewaar de einddatum-uitspraak ZONDER er een dag bij te verzinnen.

        WAAROM estimated_end hier vrijwel altijd leeg blijft: van de 95 MSN's noemt er
        GEEN ENKELE een einddatum op dagniveau. Ze zeggen "until early January 2027",
        "until mid-September 2026", "until the end of October 2026". De pijplijn
        (landkaart/build_data.parse_date) accepteert uitsluitend YYYY-MM-DD, dus elke
        invulling zou betekenen dat we zelf een dag kiezen die de bron nooit genoemd heeft.
        Dat is exact het soort verzonnen datum waar dit project eerder op is stukgelopen,
        dus: de letterlijke formulering gaat naar estimated_end_text en estimated_end
        blijft leeg. Zegt een melding ooit wel een volledige datum, dan wordt die wel gevuld.
        """
        full = re.search(
            rf"until\s+(\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{self.MONTHS})\s+\d{{4}})", body, re.I
        )
        if full:
            rec["estimated_end"] = self._parse_date_token(full.group(1))
            rec["estimated_end_text"] = re.sub(r"\s+", " ", full.group(1)).strip()
            return

        vague = re.search(
            rf"until\s+((?:[a-z/\-]+\s+){{0,3}}(?:{self.MONTHS})\s+\d{{4}})", body, re.I
        )
        if vague:
            # Regeleindes uit de HTML meenemen levert waarden als "w/c\nlate September 2026".
            rec["estimated_end_text"] = re.sub(r"\s+", " ", vague.group(1)).strip()

    # ─── Bron 2: NHSBSA Serious Shortage Protocols ──────────────────────────

    def _scrape_nhsbsa(self) -> list[dict]:
        """Parse de twee SSP-tabellen los van elkaar.

        De kop BOVEN de tabel bepaalt de status: "Active SSPs" -> lopend,
        "Expired SSPs" -> afgelopen. Dit is de enige GB-bron die dat hard vastlegt.
        Daarmee hoeven we niet te leunen op de inactive-regel die volgens de validatie van
        Jesper lopende tekorten ten onrechte verbergt: hier staat zwart op wit wat nog loopt.
        """
        resp = self.session.get(self.NHSBSA_SSP_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        records: list[dict] = []
        for table in soup.find_all("table"):
            heading = table.find_previous(["h2", "h3"])
            head_text = heading.get_text(" ", strip=True).lower() if heading else ""
            if "active ssp" in head_text:
                status = "shortage"
            elif "expired ssp" in head_text:
                status = "resolved"
            else:
                continue  # onbekende tabel: liever overslaan dan gokken

            for row in table.find_all("tr"):
                # NHSBSA zet in de datumcel geregeld een harde spatie (U+00A0) rond "to":
                # "18 Nov 2021\xa0to\xa012 Jan 2022". Die is ONZICHTBAAR in de output maar
                # laat `partition(" to ")` hieronder falen, waardoor de einddatum stilletjes
                # wegvalt (13 van de 90 rijen). Daarom hier normaliseren, voor het parsen.
                cells = [
                    c.get_text(" ", strip=True).replace("\xa0", " ")
                    for c in row.find_all("td")
                ]
                if len(cells) < 2:
                    continue  # kopregel
                records.append(self._parse_nhsbsa_row(cells, status))

        return records

    def _parse_nhsbsa_row(self, cells: list[str], status: str) -> dict:
        rec = self._blank()
        rec["source_detail"] = "NHSBSA (Serious Shortage Protocol)"
        rec["url"] = self.NHSBSA_SSP_URL
        rec["status"] = status

        name = cells[0]
        ref = re.match(r"\s*(SSP\d+)\s*(.*)", name, re.I)
        if ref:
            rec["product_no"] = ref.group(1).upper()
            name = ref.group(2)
        # "(PDF:211KB)" is een bijlage-aanduiding, geen onderdeel van de productnaam.
        name = re.sub(r"\(PDF[^)]*\)", "", name, flags=re.I)
        name = re.sub(r"\s+", " ", name).replace(" ®", "®").strip(" .,")
        rec["medicine_name"] = name
        rec["dosage_form"] = self._dosage_form(name)

        period = cells[1] if len(cells) > 1 else ""
        # "09 September 2026 to 15 January 2027" — maar de cel bevat vaak nog een staart als
        # "This SSP was amended on 23 July 2026". Per helft de EERSTE datum pakken, anders
        # sleep je die wijzigingsdatum de einddatum in.
        start_part, _, end_part = period.partition(" to ")
        rec["shortage_start"] = self._first_date(start_part)
        if end_part:
            rec["estimated_end"] = self._first_date(end_part)
            if rec["estimated_end"]:
                rec["estimated_end_text"] = rec["estimated_end"]

        amended = re.search(r"amended on\s+([^.]{4,30})", period, re.I)
        if amended:
            rec["last_updated"] = self._first_date(amended.group(1))
        # "withdrawn early on <datum>" is de ECHTE einddatum: de SSP is toen gestopt,
        # eerder dan de geplande einddatum in dezelfde cel.
        withdrawn = re.search(r"withdrawn early on\s+([^.]{4,30})", period, re.I)
        if withdrawn:
            rec["actual_end"] = self._first_date(withdrawn.group(1))
            rec["reason"] = "SSP withdrawn early"

        return rec

    # ─── Bron 3: MHRA Central Alerting System (ASP.NET webform) ─────────────

    def _cas_tokens(self, html: str) -> dict:
        soup = BeautifulSoup(html, "lxml")
        tokens = {}
        for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            el = soup.find("input", {"name": name})
            if el:
                tokens[name] = el.get("value", "")
        return tokens

    def _scrape_cas(self) -> list[dict]:
        """Doorloop het CAS-zoekresultaat per aanbieder.

        TWEE VALKUILEN, allebei kostbaar:

        1. HET FILTER VALT WEG BIJ PAGINERING. Stuur je bij een Page$-POST alleen
           __EVENTTARGET/__EVENTARGUMENT mee, dan valt CAS terug op ALLE alerts: dan krijg je
           189 pagina's met 1874 meldingen uit het hele register in plaats van de 23 van
           DHSC. Precies de Estland-fout (verkeerde filterwaarde), hier verstopt in de
           paginering. lstOriginatingEntities moet dus bij ELKE POST opnieuw mee.

        2. STOP OP DE PAGER, NIET OP EEN TELLER. De pager toont alleen de bereikbare
           Page$-nummers. Houd bij welke je al gehad hebt en stop zodra er geen onbezochte
           meer bij staat, anders blijf je de laatste pagina herhalen.

        De detailpagina's halen we ALLEEN op voor rijen met een afgekapte titel (zie
        _parse_cas_row). Ze bevatten namelijk geen reason en geen vergunninghouder — de
        broadcast-content is leeg en de inhoud zit in een bijlage-PDF — dus ze alle 23
        ophalen zou 23 requests kosten voor nul extra velden.
        """
        records: list[dict] = []

        for entity in self.CAS_ENTITIES:
            resp = self.session.get(self.CAS_SEARCH_URL, timeout=30)
            resp.raise_for_status()

            payload = self._cas_tokens(resp.text)
            payload[self.CAS_CTL + "lstOriginatingEntities"] = entity
            payload[self.CAS_CTL + "bttSearch"] = "Search"
            # Zonder Referer geeft dit soort .NET-formulieren geregeld een 403/302 zonder
            # uitleg (dezelfde fix als bij de Zwitserse bron).
            post_headers = {"Referer": self.CAS_SEARCH_URL, "Origin": self.base_url}
            resp = self.session.post(
                self.CAS_SEARCH_URL, data=payload, headers=post_headers, timeout=30
            )
            resp.raise_for_status()

            visited = {1}
            while len(visited) <= self.MAX_FEED_PAGES:
                soup = BeautifulSoup(resp.text, "lxml")
                grid = soup.find("table", {"id": re.compile("gvwAlertList")})
                if grid is None:
                    break

                for row in grid.find_all("tr"):
                    cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
                    link = row.find("a", href=re.compile("AlertID="))
                    if len(cells) < 5 or link is None:
                        continue
                    records.append(self._parse_cas_row(cells, link))

                todo = [
                    int(n)
                    for n in set(re.findall(r"Page\$(\d+)", str(grid)))
                    if int(n) not in visited
                ]
                if not todo:
                    break
                nxt = min(todo)
                visited.add(nxt)

                payload = self._cas_tokens(resp.text)
                payload[self.CAS_CTL + "lstOriginatingEntities"] = entity  # zie valkuil 1
                payload["__EVENTTARGET"] = self.CAS_GRID
                payload["__EVENTARGUMENT"] = f"Page${nxt}"
                resp = self.session.post(
                    self.CAS_SEARCH_URL, data=payload, headers=post_headers, timeout=30
                )
                resp.raise_for_status()

        return records

    def _parse_cas_row(self, cells: list[str], link) -> dict:
        rec = self._blank()
        rec["source_detail"] = "MHRA CAS (National Patient Safety Alert - DHSC)"
        rec["product_no"] = cells[0]
        rec["url"] = self.CAS_BASE + link.get("href", "").lstrip("/")

        title = cells[1]
        # VALKUIL: de resultaattabel KAPT lange titels af en zet er " ..." achter. Zonder
        # deze controle eindigt een product als "... Glucose 2. ..." in de dataset: een naam
        # die nergens op te matchen is. De volledige titel staat wel op de detailpagina,
        # dus die halen we op — maar alleen voor de paar rijen die het nodig hebben.
        if title.rstrip().endswith("..."):
            full = self._cas_full_title(rec["url"])
            if full:
                title = full

        # De titel luidt "Shortage of X" / "Supply of X" / "X - Supply Disruption".
        # Die woorden zijn de meldingssoort, niet de productnaam.
        name = re.sub(r"^(?:shortage|supply|discontinuation)\s+of\s+", "", title, flags=re.I)
        name = re.sub(r"\s*[-–]\s*supply\s+disruption.*$", "", name, flags=re.I).strip()
        rec["medicine_name"] = name or title
        rec["dosage_form"] = self._dosage_form(name)

        rec["shortage_start"] = self._first_date(cells[3])  # kolom "Issue Date"

        # CAS "Status" gaat over de afhandeling van de alert (Issued/Cancelled), niet over de
        # voorraad; het is dus GEEN ernstgraad en hoort niet in severity (die kolom bevat de
        # DHSC-tier). Alleen 'Cancelled' zegt iets hards; de rest blijft shortage.
        rec["status"] = "resolved" if "cancel" in cells[4].lower() else "shortage"
        return rec

    def _cas_full_title(self, url: str) -> str:
        """Haal de niet-afgekapte titel van een CAS-detailpagina."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException:
            return ""  # liever de afgekapte titel dan de hele rij verliezen
        el = BeautifulSoup(resp.text, "lxml").find(id=re.compile("lblViewTitle"))
        return el.get_text(" ", strip=True) if el else ""

    # ─── Main ───────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        records: list[dict] = []

        # De hoofdbron mag hard falen: zonder CPE is er geen zinnige GB-dataset en dan is
        # een luide fout beter dan een half gevulde CSV.
        cpe = self._scrape_cpe()
        print(f"  CPE (DHSC MSN): {len(cpe)} meldingen")
        records.extend(cpe)

        # De aanvullingen mogen falen zonder de hoofdbron mee te slepen, maar het MOET
        # zichtbaar zijn in de log — een stil weggevallen bron is hoe DK maanden 5% leverde.
        for label, fn in (("NHSBSA SSP", self._scrape_nhsbsa), ("MHRA CAS", self._scrape_cas)):
            try:
                extra = fn()
                print(f"  {label}: {len(extra)} meldingen")
                records.extend(extra)
            except Exception as exc:  # noqa: BLE001
                print(f"  LET OP: {label} MISLUKT ({type(exc).__name__}: {exc}) - "
                      f"deze rijen ontbreken in de output")

        # Dedupliceren op genormaliseerde naam + startdatum. Bewust streng: hetzelfde
        # product met een ANDERE startdatum is een andere melding (een MSN en een latere
        # SSP over hetzelfde middel zijn twee afzonderlijke instrumenten) en blijft staan.
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for rec in records:
            key = (self._norm_key(rec["medicine_name"]), rec["shortage_start"] or "")
            if key in seen:
                continue
            seen.add(key)
            deduped.append(rec)

        df = pd.DataFrame(deduped)
        dropped = len(records) - len(deduped)
        print(f"  Totaal: {len(df)} rijen ({dropped} duplicaten verwijderd), "
              f"{df['medicine_name'].nunique()} unieke producten")
        return df
