"""Scraper voor Nieuw-Zeeland — Pharmac Medicine Notices.

WAAROM DEZE SCRAPER ER ZO UITZIET
---------------------------------
De vorige versie liep via `https://www.pharmac.govt.nz/sitemap.xml`. Dat is géén
platte URL-lijst maar een sitemap-INDEX (~892 bytes, 4 geneste <sitemap><loc>-
verwijzingen). Geen enkele <loc> daarin bevat "/medicine-notices/", dus het filter
matchte niets, de loop draaide nul keer en scrape() gaf een lege DataFrame terug —
zonder exception, met HTTP 200. Die storing is daardoor maandenlang onzichtbaar
gebleven. Vandaar dat deze versie overal hard faalt in plaats van leeg terug te geven.

De sitemap-route is bovendien niet alleen kapot maar structureel fout: de 4
child-sitemaps samen bevatten 183 notice-URL's terwijl de live index er 93 heeft.
De overige ~90 zijn gearchiveerde/opgeloste pagina's (bv. "Glucagen Hypokit: Supply
issue resolved" uit april 2023). Die zouden als actuele tekorten binnenkomen — exact
de CA-resolved-ruis en de GR-verouderde-paginaval. Daarom: nooit terug naar de sitemap.

BRON
----
De indexpagina draait op een Vue-app die zijn data uit een JSON-API haalt:
    /api/medicineindex/data/{root}?mType={typeId}
Die API is gewoon met `requests` te benaderen (geen cookie, geen token, geen browser).
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class NzPharmacScraper(BaseScraper):
    """Nieuw-Zeeland — Pharmac medicine supply notices via de medicineindex-API."""

    INDEX_URL = "https://www.pharmac.govt.nz/medicine-funding-and-supply/medicine-notices"
    API_TMPL = "https://www.pharmac.govt.nz/api/medicineindex/data/{root}"

    # Fallback als de indexpagina onverwacht geen data-appData meer bevat. Het echte
    # nummer wordt uit de pagina gelezen (zie _read_app_data): een hardgecodeerde root
    # is een stille tijdbom zodra Pharmac de pagina opnieuw aanmaakt.
    FALLBACK_ROOT = 7967

    # mType-waarden komen uit TypeList op de indexpagina zelf. LET OP: de parameter heet
    # `mType`. Schrijf je `type=2`, dan wordt die parameter STIL GENEGEERD en krijg je alle
    # 93 notices terug — inclusief 21 brand changes, 5 medicine updates, 2 "New look" en
    # 1 access change. Dat is precies de Estland-fout (filter stond op "beide"), alleen dan
    # zonder enige foutmelding. Geverifieerd: ?type=2 -> Total 93, ?mType=2 -> Total 62.
    MTYPE_SUPPLY_ISSUE = 2
    MTYPE_DISCONTINUATION = 4

    # Discontinuations komen als aparte rijen met status="discontinued" binnen. Ze mogen
    # NOOIT als tekort meetellen; downstream filtert op status. Brand changes (mType=1)
    # halen we bewust niet op: een merkwissel is geen leveringsprobleem.
    INCLUDE_DISCONTINUATIONS = True

    # Sleutels in het <li><strong>Key:</strong>value</li>-blok op de detailpagina.
    PRODUCT_KEYS = {"chemical", "brand", "presentation", "pharmacode", "subsidy", "measure / qty"}

    # Detailpagina's SEQUENTIEEL ophalen, nooit concurrent: onder parallelle requests lekken
    # gecachte footers tussen pagina's door en krijg je de datums van een andere melding.
    #
    # De pauze is klein omdat _rerun_targeted.py een harde SIGALRM van 150s per scraper zet
    # en er 68 detailpagina's van ~530 KB opgehaald moeten worden. Gemeten: 1,2s per pagina
    # gemiddeld, maar met flinke spreiding (0,5s tot 3,7s). Een run met 0,35s pauze duurde
    # 140s — dat is 7% marge en dus te krap: bij een trage dag verliest de scraper de hele
    # run aan de time-out. Met 0,2s zit een normale run rond de 95s. De feitelijke belasting
    # blijft laag (~0,7 requests/seconde) omdat het ophalen zelf al ruim een seconde kost.
    # Wordt dit ooit weer te traag: verlaag de pauze NIET verder maar verhoog de time-out in
    # de runner, of laat de discontinuations weg — niet parallelliseren.
    REQUEST_PAUSE_S = 0.2

    # Na dit aantal seconden worden de resterende detailpagina's overgeslagen, zodat de
    # scraper binnen de 150s van de runner klaar is met een VOLLEDIGE meldingenlijst in
    # plaats van dood te gaan met nul rijen. Zie de uitleg bij het budget in scrape().
    DETAIL_BUDGET_S = 120

    # Boven deze looptijd waarschuwen we in de log. Anders zie je pas dat deze scraper tegen
    # de 150s-time-out aan kruipt op het moment dat hij er overheen gaat en stilvalt.
    SLOW_RUN_WARN_S = 110

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    }

    def __init__(self) -> None:
        super().__init__(
            country_code="NZ",
            country_name="New Zealand",
            source_name="Pharmac",
            base_url="https://www.pharmac.govt.nz",
        )
        # Eén sessie voor alles: keep-alive scheelt bijna 2 seconden per detailpagina
        # (gemeten 2,6s zonder sessie tegen 0,9s met sessie). Bij 68 pagina's is dat het
        # verschil tussen binnen en buiten de 150s-time-out van de batchrunner.
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    # ─── Indexpagina: root-id en de door de bron zelf genoemde aantallen ─────

    def _read_app_data(self) -> dict:
        """Lees het JSON-blob uit <div id="medicineIndexApp" data-appData="...">.

        Levert {"Root": 7967, "TypeList": [{ID,Title,Count}, ...], "TotalCount": 93}.
        TypeList is goud waard: de bron noemt daar zélf hoeveel supply issues er zijn,
        dus we kunnen ons API-resultaat ertegen afrekenen in plaats van maar te hopen.

        VALKUIL 1: er staan ZES elementen met een data-appData-attribuut op deze pagina
        (vijf zoekbalk-apps plus deze). Selecteer dus expliciet op id="medicineIndexApp"
        en pak nooit "de eerste die je tegenkomt".

        VALKUIL 2: BeautifulSoup verkleint attribuutnamen bij het HTML-parsen, dus
        `div.get("data-appData")` geeft None terwijl het attribuut er wel staat. Je moet
        `data-appdata` opvragen. Hier vragen we beide varianten op zodat een andere
        parser (html.parser/html5lib) dit bestand niet stilletjes sloopt.
        """
        resp = self.session.get(self.INDEX_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        div = soup.find("div", id="medicineIndexApp")
        if div is None:
            raise RuntimeError(
                "NZ: <div id='medicineIndexApp'> niet gevonden op de indexpagina — "
                "de pagina is verbouwd; root-id en controle-aantallen zijn niet af te leiden."
            )

        raw = div.get("data-appdata") or div.get("data-appData")
        if not raw:
            raise RuntimeError("NZ: medicineIndexApp heeft geen data-appData-attribuut meer.")
        return json.loads(raw)

    @staticmethod
    def _expected_count(app_data: dict, mtype: int) -> int | None:
        """Het aantal dat Pharmac zelf voor dit type opgeeft, of None als het ontbreekt."""
        for entry in app_data.get("TypeList") or []:
            if entry.get("ID") == mtype:
                return entry.get("Count")
        return None

    # ─── API: de notices van één type ───────────────────────────────────────

    def _fetch_notices(self, root: int, mtype: int, expected: int | None) -> list[dict]:
        """Haal alle notices van één mType op en reken het resultaat af tegen de bron.

        Alles wat hier misgaat gooit een exception. Een lege of halve lijst stilletjes
        doorlaten is hoe deze scraper maanden 0 rijen leverde zonder dat iemand het zag.
        """
        url = self.API_TMPL.format(root=root)
        params = {"mType": mtype}
        # Referer/X-Requested-With zijn niet strikt nodig voor deze endpoint, maar kosten
        # niets en zijn precies wat bij de Zwitserse bron het verschil maakte tussen 403
        # en 200. Meesturen dus, voordat iemand concludeert dat de bron dicht zit.
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": self.INDEX_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = self.session.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = (resp.json() or {}).get("Data") or {}

        items = data.get("Items") or []
        total = data.get("Total")
        pages = data.get("Pages")

        if not items:
            raise RuntimeError(
                f"NZ: API gaf 0 notices terug voor mType={mtype} ({resp.url}). "
                "Bewust een fout in plaats van een lege DataFrame: juist het stille falen "
                "heeft deze bron maanden onopgemerkt kapot gehouden."
            )

        # Pagineringscontrole. Vandaag is Pages altijd 1 en past alles in één respons.
        # Wordt dat ooit meer, dan moet je pagineren ÉN controleren dat pagina 2 andere
        # ID's bevat dan pagina 1 — bij de Deense bron werd ?page= volledig genegeerd en
        # haalde de scraper 19x dezelfde 20 records op zonder ook maar iets te merken.
        # (Getest: ?mType=2&page=2 geeft hier Items=[] terug, dus blind doorpagineren zou
        # daar niets dupliceren, maar reken daar niet op.)
        if pages not in (None, 0, 1):
            raise RuntimeError(
                f"NZ: API meldt Pages={pages} voor mType={mtype}. Deze scraper haalt maar "
                "één pagina op. Bouw paginering en verifieer dat pagina 2 ANDERE ID's "
                "bevat dan pagina 1 voordat je dit weghaalt."
            )
        if total is not None and total != len(items):
            raise RuntimeError(
                f"NZ: API is intern inconsistent voor mType={mtype}: Total={total} maar "
                f"{len(items)} items in de respons."
            )

        ids = [it.get("ID") for it in items]
        if len(set(ids)) != len(ids):
            raise RuntimeError(f"NZ: dubbele notice-ID's in de API-respons voor mType={mtype}.")

        # Kruiscontrole tegen wat de bron zelf op de indexpagina zegt. Dit is de enige
        # check die een verkeerd FILTER zou betrappen: een genegeerde parameter geeft
        # keurig HTTP 200 met te veel rijen, en zonder deze vergelijking merk je dat nooit.
        if expected is not None and expected != len(items):
            raise RuntimeError(
                f"NZ: aantal klopt niet voor mType={mtype}: API geeft {len(items)}, maar de "
                f"indexpagina noemt zelf {expected}. Controleer of de parameter nog `mType` "
                "heet — met `type` wordt hij genegeerd en krijg je alle types terug."
            )
        return items

    # ─── Detailpagina ───────────────────────────────────────────────────────

    def _fetch_detail(self, path: str) -> tuple[list[dict], str]:
        """Haal één detailpagina op en geef (productblokken, intro-/reden-tekst) terug.

        De pagina is statische HTML. We lezen uitsluitend het gestructureerde
        <li><strong>Key:</strong>value</li>-blok en negeren de footer volledig:
          - "First Published: ..." in de voettekst en <time itemprop="datePublished">
            zijn onbetrouwbaar. Die <time> draagt bovendien kapotte datums
            (datetime="2026-05-1", zonder nul), en "Last Updated" staat er als label bij
            terwijl het datePublished heet. De datums komen uit de API, punt.
          - De tag <span class="tag">Active</span> staat op ALLE 93 notices op "Active",
            ook op de twee die volgens hun eigen titel zijn opgelost. Waardeloos dus;
            de status leiden we uitsluitend uit de titel af.
        """
        url = self.base_url + path
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Binnen <article> blijven: daarbuiten staan nav- en footer-lijsten die ook <li>
        # bevatten en die je anders als productgegevens binnenhaalt.
        article = soup.find("article") or soup

        blocks: list[dict] = []
        for ul in article.find_all("ul"):
            block: dict[str, str] = {}
            for li in ul.find_all("li", recursive=False):
                text = li.get_text(" ", strip=True).replace("\xa0", " ")
                if ":" not in text:
                    continue
                # De opmaak van deze regels is inconsistent — alle drie de vormen komen
                # voor en alleen splitsen op de hele li-tekst vangt ze alle drie:
                #   <li><strong>Brand:</strong> Progynova</li>      (dubbele punt in strong)
                #   <li><strong>Chemical</strong>: Oestradiol</li>  (dubbele punt erbuiten)
                #   <li><strong>Pharmacode: 2619423</strong></li>   (waarde ook in strong)
                # Filteren op "strong-tekst eindigt op :" mist daardoor Chemical én
                # Pharmacode — en Pharmacode is nu juist het veld waar de rijen op splitsen.
                key, value = text.split(":", 1)
                key = re.sub(r"\s+", " ", key).strip().lower()
                value = re.sub(r"\s+", " ", value).strip()
                if key in self.PRODUCT_KEYS and value:
                    block[key] = value
            if block:
                blocks.append(block)

        intro = article.find("p", class_="typography-intro-text")
        reason = intro.get_text(" ", strip=True) if intro else ""
        return blocks, re.sub(r"\s+", " ", reason).strip()

    # ─── Normaliseren ───────────────────────────────────────────────────────

    @staticmethod
    def _parse_date(value: str | None) -> str | None:
        """'27 May 2026' -> '2026-05-27'. Geeft None terug i.p.v. iets te verzinnen."""
        if not value:
            return None
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    @staticmethod
    def _split_pharmacodes(value: str | None) -> list[str]:
        """Eén Pharmacode-regel kan meerdere codes bevatten.

        Voorbeeld uit de praktijk: hydralazine heeft "Pharmacode: 274550 and 2446227" —
        twee producten in één regel. Niet splitsen levert een onvindbaar productnummer op.
        Codes verschillen in lengte (6 én 7 cijfers komen voor), dus niet op lengte filteren.
        """
        if not value:
            return []
        return [c for c in re.findall(r"\d{5,9}", value)]

    @staticmethod
    def _dosage_form(presentation: str) -> str:
        """Herken de toedieningsvorm in de Presentation-tekst, of geef leeg terug.

        Bewust conservatief: alleen vormen die letterlijk in de brontekst staan. Niets
        herkend is leeg, nooit een gok. De volledige Presentation blijft sowieso in
        `strength` staan, dus er gaat geen informatie verloren als dit niets vindt.
        """
        forms = {
            "tab": "tablet", "cap": "capsule", "inj": "injection", "susp": "suspension",
            "syr": "syrup", "crm": "cream", "cream": "cream", "oint": "ointment",
            "gel": "gel", "sol": "solution", "soln": "solution", "drop": "drops",
            "patch": "patch", "pessary": "pessary", "supp": "suppository",
            "inhaler": "inhaler", "powder": "powder", "sachet": "sachet",
            "granules": "granules", "lotion": "lotion", "spray": "spray",
        }
        for word in re.findall(r"[A-Za-z]+", (presentation or "").lower()):
            if word in forms:
                return forms[word]
        return ""

    def _rows_for_notice(self, item: dict, status: str, with_detail: bool = True) -> list[dict]:
        """Zet één notice om in rijen: één per Pharmacode, anders één voor de melding.

        `with_detail=False` slaat de detailpagina over (zie het tijdbudget in scrape()).
        De melding komt dan alsnog binnen met de gegevens uit de API — naam, datums en
        status kloppen dan gewoon, alleen de productvelden blijven leeg. Nooit opvullen.
        """
        title = (item.get("Title") or "").strip()
        path = item.get("Link") or ""
        blocks, reason = self._fetch_detail(path) if (path and with_detail) else ([], "")

        # Titel is "<middel>: <status-frase>". Het deel vóór de dubbele punt is de beste
        # naam die we hebben als de detailpagina geen Brand noemt.
        title_medicine = title.split(":", 1)[0].strip() if ":" in title else title

        base = {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            # LEEG LATEN: geen van de 93 notice-pagina's noemt ergens een ATC-code.
            # Koppeling gebeurt downstream via de PRK/G-standaard-laag. Niet afleiden uit
            # de stofnaam — dat is verzinnen.
            "atc_code": "",
            # LEEG LATEN: er is geen gestructureerde handelsvergunninghouder. "Brand" is een
            # MERKnaam (Progynova, Noumed) en geen MAH, en "Supplier:" staat op precies
            # 1 van de 62 pagina's. Eén veld invullen uit een ander veld is hoe je een
            # kolom vol onware waarden krijgt.
            "marketing_auth_holder": "",
            # LEEG LATEN: een einddatum staat alleen in vrije tekst ("expects the shipment
            # to arrive by the end of September 2026"). Niet parsebaar zonder te gokken.
            "estimated_end": "",
            # OpenedDate, NOOIT PublishDate en NOOIT de scrapedatum. PublishDate is een
            # hersorteer-/heropduikdatum: Progynova staat op PublishDate 22 Sep 2026 (exact
            # de dag van scrapen) terwijl OpenedDate 27 May 2026 is. PublishDate gebruiken
            # reproduceert letterlijk de AT/DK-bug waarbij de scrapedatum als startdatum
            # in de data belandde.
            "shortage_start": self._parse_date(item.get("OpenedDate")),
            "last_updated": self._parse_date(item.get("LastUpdatedDate")),
            "status": status,
            "reason": reason,
            "notice_title": title,
            "notice_url": self.base_url + path if path else "",
            # Provenance: is de detailpagina daadwerkelijk gelezen? Zonder dit veld zou een
            # afgekapte run er downstream uitzien als "deze melding heeft nu eenmaal geen
            # productgegevens", en dat is precies het soort stille kwaliteitsverlies waar
            # dit project eerder op is stukgelopen.
            "detail_fetched": with_detail,
            "scraped_at": datetime.now().isoformat(),
        }

        # Blokken uitvouwen naar één regel per Pharmacode.
        expanded: list[dict] = []
        for block in blocks:
            codes = self._split_pharmacodes(block.get("pharmacode"))
            if codes:
                for code in codes:
                    expanded.append({**block, "pharmacode": code})
            else:
                expanded.append({**block, "pharmacode": ""})

        # Ontdubbelen binnen de melding. Pagina's herhalen dezelfde Pharmacode verderop in
        # de tekst, soms als kale <li>Pharmacode: x</li> zonder verdere velden (bv.
        # bicillinla noemt 2258072 twee keer). We houden per code het RIJKSTE blok over,
        # anders verlies je merk en verpakking aan een lege herhaling.
        by_code: dict[str, dict] = {}
        codeless: list[dict] = []
        for block in expanded:
            code = block.get("pharmacode") or ""
            if code:
                prev = by_code.get(code)
                if prev is None or len(block) > len(prev):
                    by_code[code] = block
            elif block not in codeless:
                # Blokken zonder Pharmacode blijven staan als ze wél iets zeggen (naproxen
                # heeft twee bulkverpakkingen met merk maar zonder code). Weggooien zou
                # echte producten laten verdwijnen; product_no blijft dan gewoon leeg.
                codeless.append(block)

        product_blocks = list(by_code.values()) + codeless
        if not product_blocks:
            # 15 van de 62 meldingen hebben geen gestructureerd productblok (bv. een
            # algemene melding over het Midden-Oosten-conflict). Eén rij, productvelden
            # leeg — niets invullen wat er niet staat.
            product_blocks = [{}]

        rows: list[dict] = []
        for block in product_blocks:
            presentation = block.get("presentation", "")
            rows.append({
                **base,
                # medicine_name komt uit de TITEL, niet uit Brand. Brand is vaak de naam van
                # een generieke fabrikant ("Noumed", "Relonchem (S29)") en zegt niets over
                # het middel; de titel bevat juist wél de stofnaam ("Mirtazapine (30 mg
                # tablets)"). Brand blijft in een eigen kolom staan, zodat twee rijen van
                # dezelfde melding nog steeds uit elkaar te houden zijn.
                "medicine_name": title_medicine,
                "brand": block.get("brand", ""),
                "active_substance": block.get("chemical", ""),
                "strength": presentation,
                "dosage_form": self._dosage_form(presentation),
                "package_size": block.get("measure / qty", ""),
                "product_no": block.get("pharmacode", ""),
            })
        return rows

    # ─── Main ───────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        started = time.time()

        app_data = self._read_app_data()
        root = app_data.get("Root") or self.FALLBACK_ROOT
        if root != self.FALLBACK_ROOT:
            print(f"  Let op: root-id is {root} (was {self.FALLBACK_ROOT}) — uit de pagina gelezen")

        wanted: list[tuple[int, str]] = [(self.MTYPE_SUPPLY_ISSUE, "shortage")]
        if self.INCLUDE_DISCONTINUATIONS:
            wanted.append((self.MTYPE_DISCONTINUATION, "discontinued"))

        rows: list[dict] = []
        skipped_detail = 0
        for mtype, default_status in wanted:
            expected = self._expected_count(app_data, mtype)
            items = self._fetch_notices(root, mtype, expected)
            print(f"  mType={mtype}: {len(items)} meldingen (bron noemt zelf: {expected})")

            for i, item in enumerate(items, 1):
                # Status uitsluitend uit de titel. De on-page tag staat op alle notices op
                # "Active", ook op de opgeloste, en is dus onbruikbaar.
                title_lower = (item.get("Title") or "").lower()
                status = "resolved" if "resolv" in title_lower else default_status

                # Tijdbudget. Het ophalen van 68 detailpagina's duurt 95s tot 145s, afhankelijk
                # van hoe traag pharmac.govt.nz die dag is, en de batchrunner kapt af op 150s.
                # Zonder budget verliest een trage dag ALLE 68 meldingen aan de time-out. Met
                # budget leveren we altijd de volledige lijst met correcte datums en statussen
                # en verliezen we hooguit de productverrijking van de laatste meldingen — die
                # rijen staan dan op detail_fetched=False en worden hieronder hard geteld.
                with_detail = (time.time() - started) < self.DETAIL_BUDGET_S
                if not with_detail:
                    skipped_detail += 1
                rows.extend(self._rows_for_notice(item, status, with_detail=with_detail))

                if i % 20 == 0:
                    print(f"    ... {i}/{len(items)} detailpagina's")
                if with_detail and i < len(items):
                    time.sleep(self.REQUEST_PAUSE_S)

        df = pd.DataFrame(rows)

        # Kolomvolgorde zoals build_data verwacht; extra velden achteraan.
        columns = ["country_code", "country_name", "source", "medicine_name", "active_substance",
                   "strength", "package_size", "product_no", "atc_code", "marketing_auth_holder",
                   "shortage_start", "estimated_end", "status", "scraped_at",
                   "reason", "last_updated", "dosage_form", "brand", "notice_title",
                   "notice_url", "detail_fetched"]
        df = df.reindex(columns=columns)

        n_products = df.loc[df["product_no"].astype(str).str.strip() != "", "product_no"].nunique()
        elapsed = time.time() - started
        print(f"  Total: {len(df)} NZ rijen uit {df['notice_url'].nunique()} meldingen, "
              f"{n_products} unieke Pharmacodes ({elapsed:.0f}s)")
        print(f"  Status: {df['status'].value_counts().to_dict()}")
        if skipped_detail:
            print(f"  LET OP: bij {skipped_detail} van de {df['notice_url'].nunique()} meldingen "
                  "is de detailpagina overgeslagen wegens het tijdbudget; die rijen hebben lege "
                  "productvelden en staan op detail_fetched=False. Datums en status kloppen wel.")
        if elapsed > self.SLOW_RUN_WARN_S:
            print(f"  WAARSCHUWING: {elapsed:.0f}s is dicht bij de 150s-time-out van de "
                  "batchrunner. Zie REQUEST_PAUSE_S voor wat je hier wel en niet aan moet doen.")
        return df
