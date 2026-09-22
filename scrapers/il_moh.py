"""Israël — Ministry of Health: meldingen van stopzetting/tekort van geneesmiddelen.

BRONWISSEL (belangrijk voor wie deze scraper later aanpast)
-----------------------------------------------------------
De vorige versie las `israeldrugs.health.gov.il/GovServiceList/IDRServer/SearchByAdv`.
Dat is het GENEESMIDDELENREGISTER en geen tekortenbron. De velden die daar werden
gebruikt (`fromCanceledDrags` / `bitulDate` / `iscanceled`) betekenen "registratie
ingetrokken", niet "niet leverbaar". Die werden weggeschreven als shortage_start met
status="shortage" — precies dezelfde fout als eerder bij LT, TR en EE: een register
als tekortenlijst presenteren. Het register heeft überhaupt geen tekort-endpoint, dus
die bron is hier onbruikbaar en volledig losgelaten.

De juiste bron is de wettelijke meldplichtdatabase van het ministerie: de
vergunninghouder MOET elke stopzetting van de marketing of tijdelijke onbeschikbaarheid
melden. Die meldingen staan in de "dynamic collector" drug-marketing-stoped:

    UI   https://www.gov.il/he/Departments/DynamicCollectors/drug-marketing-stoped
    API  POST https://www.gov.il/he/api/DynamicCollector
         {"DynamicTemplateID": "<template>", "QueryFilters": {}, "From": <n>}

Er bestaat daarnaast een tweede collector (`drug-shortage`, template
12a3c8f5-73aa-4578-a1b2-44437965f380) met wekelijkse XLSX-deltarapporten. NIET als
primaire bron gebruiken: dat zijn deltarapporten, en de kolom "תכשירים שחזרו לשיווק"
(teruggekeerde producten) is een LOSSE parallelle lijst in hetzelfde sheet. Wie dat
rij-voor-rij inleest koppelt willekeurige producten aan willekeurige terugkeerregels.
Hooguit bruikbaar als kruiscontrole.
"""

from __future__ import annotations

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper


class IlMohScraper(BaseScraper):
    """Haalt alle meldingen uit de gov.il collector drug-marketing-stoped."""

    API_URL = "https://www.gov.il/he/api/DynamicCollector"
    UI_URL = "https://www.gov.il/he/Departments/DynamicCollectors/drug-marketing-stoped"
    TEMPLATE_ID = "e12d5d93-a826-4850-934d-944759f673ee"

    # Cloudflare zit voor heel gov.il en geeft 403 op een "kale" requests-call.
    # Uitgetest met leave-one-out (22-09-2026): doorslaggevend zijn User-Agent,
    # X-Requested-With én Accept-Language. Laat je er één van weg -> 403 HTML.
    # Content-Type, Origin en Referer maken niets uit maar houden we aan omdat de
    # browser ze ook stuurt. Een browser (curl_cffi/Playwright) is NIET nodig; die
    # route is geprobeerd en levert exact hetzelfde resultaat tegen extra dependencies.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.gov.il",
        "Referer": UI_URL,
    }

    # De paginagrootte staat hard op 20 in de collector zelf (zie ng-init in de UI-HTML).
    # Size/PageSize/Take/To/Limit meesturen wordt genegeerd; je krijgt altijd 20 terug.
    PAGE_SIZE = 20

    # De sortering van de collector is INSTABIEL: dezelfde offset levert bij herhaling
    # net iets andere records. Eén nette pass met stap 20 haalt daardoor maar ~97,7%.
    # Daarom meerdere passes met verschoven offset; na pass 2 zitten we op ~99,9% en
    # daarna vlakt het af op ~99,95% (4 records blijven onbereikbaar, zie COVERAGE_*).
    PASSES = ((0, 20), (10, 20), (5, 10), (15, 10))
    COVERAGE_GOOD = 0.995   # zodra we hier zijn stoppen we met extra passes
    COVERAGE_WARN = 0.99    # daaronder: waarschuwing in de log
    COVERAGE_FAIL = 0.95    # daaronder: hard falen i.p.v. stil te weinig data leveren

    # _rerun_targeted.py kapt elke scraper af op 150 s (SIGALRM). De volledige crawl
    # (~400 requests per pass) duurt met 4 workers ~16 s per pass, dus 2 passes ≈ 35 s.
    # Dit budget is de noodrem als het netwerk traag is: liever een eerlijke dekkings-
    # melding dan door SIGALRM worden doodgeslagen zonder enige output.
    TIME_BUDGET_S = 110
    WORKERS = 4

    # res_stop_markt, sleutels uit de UI (res_stop_marktMultiChoiseValues).
    REASONS = {
        "1": "operationeel",   # סיבות תפעוליות
        "2": "commercieel",    # סיבות מסחריות
        "3": "overig",         # אחר
    }

    # Israël kent geen apart statusveld. De status zit in het vrije-tekstveld re_markt
    # ("צפי לחידוש שיווק" = verwachte hervatting van de marketing). Zonder deze
    # classificatie zou ~47% van de rijen (opgelost + permanent uit de handel) als
    # lopend tekort op de kaart komen.
    RE_RETURNED = "חזר"        # "(het middel) is teruggekeerd (naar voorraad/markt)"
    RE_POSTPONED = "דחיי"      # "uitstel" — staat ook in "uitstel van de terugkeer";
    #                            zonder deze uitzondering telt zo'n melding als opgelost.
    RE_PERMANENT = ("לצמיתות", "לצמיתוות", "לצתיתות")  # incl. twee typefouten in de bron
    RE_UNKNOWN = ("הודעה חדשה", "לא ידוע", "n/a", "na")  # "tot nader bericht" / "onbekend"

    def __init__(self):
        super().__init__(
            country_code="IL",
            country_name="Israel",
            source_name="MOH",
            base_url="https://www.gov.il",
        )
        self._local = threading.local()

    # ── transport ────────────────────────────────────────────────────────────────

    def _session(self) -> requests.Session:
        """Eén Session per thread; requests.Session is niet thread-safe."""
        if not hasattr(self._local, "session"):
            s = requests.Session()
            s.headers.update(self.HEADERS)
            self._local.session = s
        return self._local.session

    def _fetch(self, offset: int, attempts: int = 3) -> list[dict] | None:
        """Haal één pagina op. Geeft None terug als de pagina definitief mislukt,
        zodat de aanroeper het verschil ziet tussen "leeg" en "niet gelukt"."""
        for poging in range(attempts):
            try:
                resp = self._session().post(
                    self.API_URL,
                    json={
                        "DynamicTemplateID": self.TEMPLATE_ID,
                        # QueryFilters moet leeg blijven: de UI biedt weliswaar een
                        # zoekveld op eng_name/drug_name/reg_name/active_ingredient,
                        # maar elke gevulde variant die we geprobeerd hebben ({veld:
                        # waarde}, [{key,value}], [{Key,Value}]) geeft een 404 met een
                        # HTML-pagina. Er is ook géén datumfilter (geen enkel veld heeft
                        # DisplaySearchFromDate), dus serverside een venster kiezen kan
                        # niet — we halen alles op en filteren hier.
                        "QueryFilters": {},
                        # ALLEEN "From" werkt. skip/Skip/Offset/page worden genegeerd en
                        # geven stilzwijgend pagina 1 terug — dat is het Denemarken-
                        # scenario waarbij een scraper 19x dezelfde 20 records ophaalde.
                        "From": offset,
                    },
                    timeout=45,
                )
            except requests.RequestException:
                time.sleep(0.5 * (poging + 1))
                continue
            if resp.status_code != 200:
                time.sleep(0.5 * (poging + 1))
                continue
            try:
                data = resp.json()
            except ValueError:
                time.sleep(0.5 * (poging + 1))
                continue
            # Voorbij het einde van de lijst is Results null, niet [].
            return data.get("Results") or []
        return None

    def _total_results(self) -> int:
        """Het totaal dat de bron zélf noemt. Dit is de enige harde maatstaf waaraan
        we onze dekking kunnen meten."""
        for poging in range(3):
            try:
                resp = self._session().post(
                    self.API_URL,
                    json={"DynamicTemplateID": self.TEMPLATE_ID,
                          "QueryFilters": {}, "From": 0},
                    timeout=45,
                )
                resp.raise_for_status()
                return int(resp.json()["TotalResults"])
            except (requests.RequestException, ValueError, KeyError, TypeError) as e:
                if poging == 2:
                    raise RuntimeError(
                        f"gov.il DynamicCollector niet bereikbaar of gewijzigd: {e}"
                    ) from e
                time.sleep(1.0 * (poging + 1))
        return 0

    # ── parsing ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _txt(data: dict, key: str) -> str:
        """Veldwaarde als string. Sommige velden komen als lijst terug
        (res_stop_markt kan meerdere redenen bevatten), sommige als None."""
        val = data.get(key)
        if isinstance(val, list):
            val = " | ".join(str(v) for v in val if v is not None)
        if val is None:
            return ""
        return str(val).strip()

    _DMY = re.compile(r"^(\d{1,2})([./-])(\d{1,2})\2(\d{4})$")

    @classmethod
    def _parse_date(cls, raw: str) -> str:
        """Datum uit markt_stop/re_markt naar ISO. Geeft "" als het géén volledige
        datum is — NOOIT de scrapedatum invullen, daar is dit project eerder op
        stukgelopen.

        Wat er allemaal in deze velden staat behalve datums (gemeten over 7945
        records): "מיידי" (= per direct, 874x), "קיים מחסור" (= er is een tekort),
        maand-only "12.2014"/"01/2019", Hebreeuwse omschrijvingen als
        "תחילת מרץ 2023" (= begin maart 2023) en typefouten als "30.4.025".
        Al die gevallen leveren bewust een leeg veld op.

        Scheidingsteken is . of / (en zelden -). De volgorde is normaal dd/MM
        (4574 records hebben een eerste component > 12, wat dd bewijst), maar 10
        records zijn per ongeluk in MM/dd ingevoerd ("7/14/2026"). Daarom kijken we
        naar welke component > 12 is; is geen van beide dat, dan geldt dd/MM.
        """
        raw = (raw or "").strip()
        m = cls._DMY.match(raw)
        if not m:
            return ""
        a, b, year = int(m.group(1)), int(m.group(3)), int(m.group(4))
        if a > 12 and b <= 12:
            day, month = a, b
        elif b > 12 and a <= 12:
            day, month = b, a
        elif a <= 12 and b <= 12:
            day, month = a, b          # Israëlische conventie: dag eerst
        else:
            return ""
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return ""                  # bv. 31.11.2025 bestaat niet

    @staticmethod
    def _parse_publication(raw: str) -> str:
        """publication_date ("קבלת הודעה" = ontvangst van de melding) komt als
        UTC-instant: 2026-09-17T21:00:00Z.

        VALKUIL: de eerste 10 tekens pakken levert een datum die één dag te vroeg is.
        Het CMS slaat een datum-zónder-tijd op als middernacht Israëlische tijd en
        serialiseert dat naar UTC. Bewijs 1: er komen maar twee tijdstippen voor,
        T22:00:00Z uitsluitend in nov-feb (UTC+2) en T21:00:00Z uitsluitend in apr-sep
        (UTC+3), met maart en oktober gemengd — exact de zomertijdovergang.
        Bewijs 2: de bron zet in de PDF-bestandsnaam zelf het publicatieweek-bereik
        (NEWS_recalls_drugs_<ddmmyyyy>-<ddmmyyyy>_...). Omgerekend naar Asia/Jerusalem
        valt 89,8% van de meldingen binnen die eigen week; als kale UTC-datum maar
        77,1%. Dus terugrekenen naar Asia/Jerusalem, niet afkappen.
        """
        raw = (raw or "").strip()
        if not raw:
            return ""
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return ""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo("Asia/Jerusalem")).strftime("%Y-%m-%d")

    @classmethod
    def _classify(cls, re_markt: str) -> tuple[str, str]:
        """Bepaal (status, estimated_end) uit het vrije-tekstveld re_markt.

        Waarom dit de belangrijkste stap is: de collector bevat ALLE meldingen sinds
        2013, inclusief de meldingen die allang zijn opgelost en de permanente
        stopzettingen. Een permanente stopzetting is géén tekort en hoort niet als
        lopend tekort op de kaart. We leveren alle drie de statussen mét expliciete
        waarde, zodat build_data.py ze uit elkaar kan houden (die kent "resolved" en
        "discontinued" al als aparte categorie) — maar nooit stilzwijgend als tekort.
        """
        val = (re_markt or "").strip()
        low = val.lower()

        # "uitstel van de terugkeer naar de markt" bevat óók het woord "teruggekeerd";
        # zonder deze uitzondering wordt zo'n lopende melding als opgelost geteld.
        if cls.RE_RETURNED in val and cls.RE_POSTPONED not in val:
            return "resolved", ""
        if any(p in val for p in cls.RE_PERMANENT):
            return "discontinued", ""
        if any(u in val for u in cls.RE_UNKNOWN[:2]) or low in cls.RE_UNKNOWN[2:]:
            return "shortage", ""

        einde = cls._parse_date(val)
        # Geen bruikbare datum (maand-only, Hebreeuwse omschrijving, typefout, leeg):
        # lopend tekort met onbekende einddatum. Liever leeg dan een verzonnen datum.
        return "shortage", einde

    # ── crawl ────────────────────────────────────────────────────────────────────

    def _crawl(self, total: int) -> tuple[dict[str, dict], int]:
        """Haal de hele collector op via overlappende passes. Sleutel voor dedup is
        de volledige recordinhoud, niet UrlName: UrlName is bijna-uniek maar er is
        minstens één UrlName die tweemaal met ándere inhoud voorkomt."""
        gezien: dict[str, dict] = {}
        mislukt_totaal = 0
        t0 = time.monotonic()

        for nr, (start, stap) in enumerate(self.PASSES, start=1):
            offsets = list(range(start, total + self.PAGE_SIZE, stap))
            voor = len(gezien)
            mislukt: list[int] = []

            with ThreadPoolExecutor(max_workers=self.WORKERS) as pool:
                for offset, res in zip(offsets, pool.map(self._fetch, offsets)):
                    if res is None:
                        mislukt.append(offset)
                        continue
                    for rec in res:
                        gezien[self._key(rec)] = rec

            # Mislukte pagina's nog eens serieel proberen; een stille gat-in-de-data
            # is erger dan een paar seconden extra.
            for offset in mislukt:
                res = self._fetch(offset, attempts=2)
                if res is None:
                    mislukt_totaal += 1
                else:
                    for rec in res:
                        gezien[self._key(rec)] = rec

            nieuw = len(gezien) - voor
            dekking = len(gezien) / total if total else 0.0
            print(f"    pass {nr} (From {start}, stap {stap}): +{nieuw} nieuw, "
                  f"{len(gezien)}/{total} = {dekking:.2%} "
                  f"({time.monotonic() - t0:.0f}s)")

            if nr >= 2 and dekking >= self.COVERAGE_GOOD:
                break
            if nr >= 2 and nieuw == 0:
                break   # sortering levert niets nieuws meer; extra passes zijn zinloos
            if time.monotonic() - t0 > self.TIME_BUDGET_S:
                print("    tijdbudget op, geen extra passes meer")
                break

        return gezien, mislukt_totaal

    @staticmethod
    def _key(rec: dict) -> str:
        d = rec.get("Data") or {}
        return "|".join([
            str(rec.get("UrlName") or ""),
            str(d.get("reg_number") or ""),
            str(d.get("markt_stop") or ""),
            str(d.get("re_markt") or ""),
            str(d.get("publication_date") or ""),
            str(d.get("eng_name") or d.get("drug_name") or ""),
        ])

    @staticmethod
    def _normaliseer_regnr(raw: str) -> str:
        """Het registratienummer staat in de bron in vier schrijfwijzen door elkaar:
        "146-97-33472-00", "113 78 29600 00", "131.32.31021.00" en "170573700999".
        291 nummers komen in meer dan één schrijfwijze voor, wat het aantal unieke
        producten kunstmatig met ~344 opblaast. Puur het scheidingsteken uniformeren
        (geen cijfer wordt toegevoegd of weggelaten) voor het standaard 12-cijferige
        formaat; alle afwijkende lengtes laten we exact zoals de bron ze geeft.
        """
        cijfers = re.sub(r"\D", "", raw or "")
        if len(cijfers) == 12:
            return f"{cijfers[:3]}-{cijfers[3:5]}-{cijfers[5:10]}-{cijfers[10:]}"
        return (raw or "").strip()

    # ── hoofdroutine ─────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        total = self._total_results()
        print(f"  Bron meldt TotalResults = {total}")
        if total <= 0:
            raise RuntimeError("gov.il DynamicCollector meldt 0 records — bron gewijzigd?")

        # Denemarken-controle: bewijs dat pagineren echt pagineert voordat we 400
        # requests doen. Als pagina 2 dezelfde records bevat als pagina 1, negeert de
        # server "From" en zou de scraper N keer dezelfde 20 records verzamelen.
        p1, p2 = self._fetch(0), self._fetch(self.PAGE_SIZE)
        if not p1 or p2 is None:
            raise RuntimeError("Eerste twee pagina's konden niet worden opgehaald.")
        if {r.get("UrlName") for r in p1} == {r.get("UrlName") for r in p2}:
            raise RuntimeError(
                "Paginering werkt niet: From=20 geeft dezelfde records als From=0. "
                "De API negeert de offset — niet verder scrapen, dit levert 20 records."
            )

        gezien, mislukt = self._crawl(total)
        dekking = len(gezien) / total
        if mislukt:
            print(f"  WAARSCHUWING: {mislukt} pagina's definitief mislukt")
        if dekking < self.COVERAGE_FAIL:
            raise RuntimeError(
                f"Dekking te laag: {len(gezien)}/{total} = {dekking:.2%}. "
                "Liever geen data dan stilzwijgend een fractie van de tekorten."
            )
        if dekking < self.COVERAGE_WARN:
            print(f"  WAARSCHUWING: dekking {dekking:.2%} onder {self.COVERAGE_WARN:.0%}")

        nu = datetime.now().isoformat()
        records = []
        for rec in gezien.values():
            d = rec.get("Data") or {}

            eng = self._txt(d, "eng_name")
            heb = self._txt(d, "drug_name")
            naam = eng or heb
            if not naam:
                continue

            status, estimated_end = self._classify(self._txt(d, "re_markt"))

            redenen = [self.REASONS.get(c.strip(), "")
                       for c in self._txt(d, "res_stop_markt").split("|")]
            reason = " | ".join(r for r in redenen if r)

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": naam,
                # active_ingredient is maar in ~15% van de meldingen gevuld. Leeg laten.
                # NIET terugvallen op alt_treat ("חלופות טיפוליות" = therapeutische
                # alternatieven): dat veld bevat soms wél de werkzame stof maar soms
                # het middel zelf of een concurrerend merk. ATC-verrijking gebeurt
                # downstream op naam; hier niets afleiden.
                "active_substance": self._txt(d, "active_ingredient"),
                # De bron kent geen aparte sterkte/verpakking/toedieningsvorm; de
                # sterkte zit in de productnaam ("SEROQUEL XR 150 MG"). Niet uit de
                # naam wegparsen: dat gaat bij combinatiepreparaten fout.
                "strength": "",
                "package_size": "",
                "dosage_form": "",
                "product_no": self._normaliseer_regnr(self._txt(d, "reg_number")),
                "atc_code": "",           # bron levert geen ATC
                # reg_name is de vergunninghouder, maar in 21 van de 7945 meldingen
                # heeft de invoerder hier de productnaam neergezet. Bronruis die we
                # niet kunnen repareren zonder te gokken; ongewijzigd doorgeven.
                "marketing_auth_holder": self._txt(d, "reg_name"),
                "shortage_start": self._parse_date(self._txt(d, "markt_stop")),
                "estimated_end": estimated_end,
                "status": status,
                "reason": reason,
                "last_updated": self._parse_publication(self._txt(d, "publication_date")),
                "scraped_at": nu,
            })

        df = pd.DataFrame(records)

        # Griekenland-controle: draaien we niet op een bevroren pagina? De collector
        # krijgt normaal wekelijks nieuwe meldingen.
        if not df.empty:
            nieuwste = df["last_updated"].max()
            if nieuwste:
                dagen = (datetime.now().date()
                         - datetime.strptime(nieuwste, "%Y-%m-%d").date()).days
                print(f"  Nieuwste melding: {nieuwste} ({dagen} dagen oud)")
                if dagen > 90:
                    print("  WAARSCHUWING: nieuwste melding >90 dagen oud — "
                          "bron mogelijk bevroren, controleer de collector.")

            verdeling = df["status"].value_counts().to_dict()
            print(f"  Status: {verdeling}")
            print(f"  Unieke registratienummers: {df['product_no'].nunique()}")
            # De bron publiceert een handvol meldingen twee keer onder een andere
            # UrlName maar met identieke inhoud. Bewust NIET hier weggooien: de CSV
            # blijft zo gelijk aan wat de bron zelf telt (TotalResults). build_data.py
            # dedupliceert al op alle kolommen behalve scraped_at.
            kolommen = [c for c in df.columns if c != "scraped_at"]
            dubbel = int(df.duplicated(subset=kolommen).sum())
            if dubbel:
                print(f"  Let op: {dubbel} identieke meldingen in de bron "
                      f"(build_data dedupliceert die)")
            print(f"  shortage_start gevuld: {(df['shortage_start'] != '').sum()}/{len(df)}, "
                  f"estimated_end gevuld: {(df['estimated_end'] != '').sum()}/{len(df)}, "
                  f"active_substance gevuld: {(df['active_substance'] != '').sum()}/{len(df)}")

        print(f"  Totaal: {len(df)} meldingen")
        return df
