"""Scraper voor de EMA-tekortencatalogus (EU/EEA-breed, centraal beoordeeld).

WAAROM DEZE SCRAPER ER ZO UITZIET (lees dit voordat je hem "verbetert"):

De oude versie schraapte HTML van https://www.ema.europa.eu/en/search?f[0]=...
Die route is definitief dicht: EMA heeft de zoekpagina achter een bot-gate gezet
(antwoord-header `x-ema-security-gate: challenge` met HTTP 401; een echte Chromium
krijgt 403 "Disallowed by robots.txt"). De BRON zelf is niet dood en niet verhuisd --
alleen die ene toegangsweg. EMA publiceert dezelfde catalogus namelijk zelf als
officiele Excel-export, dagelijks ververst en wel toegestaan door robots.txt.
Daarom halen we nu dat bestand op. Ga NIET terug naar HTML-scrapen van /en/search en
zet hier ook GEEN browser in: de Excel is de door EMA bedoelde machine-route.

WAT DEZE BRON WEL EN NIET IS:
Dit is een EU/EEA-brede bron, geen land. De lopende dossiers raken per definitie
meerdere lidstaten (de detailpagina noemt per dossier "Member States affected"), dus
als "EU" naast de 34 landen als eigen entiteit op de kaart komt ontstaat dubbeltelling
met de nationale registers. build_data.py sluit country_code "EU" daarom bewust uit van
de kaart. De waarde van deze bron zit in de verrijkings-/validatielaag: door EMA
beoordeelde cross-border tekorten met onderbouwde reden. Die afweging hoort bij het
Kernteam en niet in deze scraper -- we leveren hier dus gewoon nette, eerlijke rijen.
"""

import re
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class EuEmaScraper(BaseScraper):
    """Scraper voor de EMA shortage-catalogus via de officiele XLSX-export."""

    # De officiele export van exact dezelfde catalogus die achter /en/search zat.
    XLSX_URL = ("https://www.ema.europa.eu/en/documents/report/"
                "medicines-output-shortages-report_en.xlsx")
    SHEET_NAME = "Shortage"

    # Zusterbestand met ATC en vergunninghouder. Staat NIET in het tekortenbestand;
    # dit is de enige machineleesbare plek waar EMA die twee velden publiceert
    # (ook de detailpagina's per tekort hebben ze niet).
    MEDICINES_XLSX_URL = ("https://www.ema.europa.eu/en/documents/report/"
                          "medicines-output-medicines-report_en.xlsx")

    # Sommige EMA-endpoints weigeren een kale python-requests UA. Een gewone
    # browser-UA volstaat hier; Referer/Origin zijn voor deze documenten niet nodig.
    HEADERS = {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
    }

    # BEWUST FILTER (valkuil Estland: daar stond het bronfilter op "beide", waardoor
    # beeindigde middelen als tekort binnenkwamen). De bron kent drie statuswaarden;
    # "Medicine discontinued" is een uit de handel genomen middel en GEEN tekort.
    STATUS_MAP = {
        "Shortage ongoing": "shortage",
        "Shortage resolved": "resolved",
    }
    STATUS_EXCLUDE = {"Medicine discontinued"}

    # Kopregel staat vandaag op rij 9 (index 8), maar we zoeken hem op in plaats van
    # hem hard te zetten: schuift de kop, dan valt dat op i.p.v. dat de scraper
    # stilletjes de verkeerde rij als kolomnamen gebruikt.
    HEADER_ANCHOR = "Category"
    HEADER_SEARCH_ROWS = 15

    # Kolommen die we echt nodig hebben. Ontbreekt er een, dan is de export van vorm
    # veranderd en moet dat knallen, niet stilletjes een lege kolom opleveren.
    REQUIRED_COLUMNS = [
        "Category",
        "Medicine affected",
        "Supply shortage status",
        "International non-proprietary name (INN) or common name",
        "Pharmaceutical forms affected",
        "Strengths affected",
        "Availability of alternatives",
        "Start of shortage date",
        "Expected resolution date",
        "Expected resolution",
        "First published date",
        "Last updated date",
        "Shortage URL",
    ]

    # Hoe oud de export-stempel mag zijn voordat we waarschuwen (valkuil Griekenland:
    # daar haalde de scraper maandenlang keurig een pagina op die al vijf maanden
    # niet was bijgewerkt -- technisch geslaagd, inhoudelijk bevroren).
    MAX_EXPORT_AGE_DAYS = 7

    def __init__(self):
        super().__init__(
            country_code="EU",
            country_name="EU (EMA)",
            source_name="EMA",
            base_url="https://www.ema.europa.eu",
        )

    # ── helpers ────────────────────────────────────────────────────────────────

    def _get(self, url: str, session: requests.Session | None = None) -> bytes:
        get = (session or requests).get
        r = get(url, timeout=60, headers=self.HEADERS)
        r.raise_for_status()
        return r.content

    @staticmethod
    def _text(value) -> str:
        """Celwaarde naar nette string; NaN wordt leeg.

        Pandas maakt van een kolom met losse getallen soms floats, waardoor het
        jaartal 2027 als "2027.0" in de CSV zou belanden. Dat vangen we hier af.
        """
        if value is None:
            return ""
        if not isinstance(value, str) and pd.isna(value):
            return ""
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value).strip()

    @staticmethod
    def _parse_date(value) -> str:
        """dd/mm/YYYY (de enige vorm die EMA in deze export gebruikt) naar ISO.

        LEEG BLIJFT LEEG. Nooit terugvallen op vandaag of op een andere kolom:
        dat is precies waar AT en DK op stukliepen, waar de scrapedatum als
        tekortstart in de data belandde en elk tekort dus "vandaag begonnen" leek.
        """
        if value is None:
            return ""
        if isinstance(value, (datetime, pd.Timestamp)):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, float) and pd.isna(value):
            return ""
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return ""
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Onbekend formaat: liever leeg dan een verzonnen datum, maar wel luid melden.
        print(f"  LET OP: onherkenbare datumwaarde {text!r} -> leeg gelaten")
        return ""

    @staticmethod
    def _norm_name(name: str) -> str:
        """Normaliseer een merknaam voor de join met het medicijnenbestand.

        Alleen deterministisch opschonen (kleine letters, leestekens weg, en een
        toelichtende staart-parenthese zoals "Mycobutin (rifabutin)" eraf). GEEN
        fuzzy of prefix-matching: een gegokte match levert een verkeerde ATC op en
        een verkeerde ATC is schadelijker dan een lege.
        """
        text = re.sub(r"\s*\([^)]*\)\s*$", "", str(name).strip())
        text = re.sub(r"[^a-z0-9]+", " ", text.lower())
        return re.sub(r"\s+", " ", text).strip()

    def _find_header_row(self, content: bytes, sheet_name) -> int:
        """Zoek de rij waarop de kolomnamen staan i.p.v. index 8 hard te coderen."""
        head = pd.read_excel(BytesIO(content), sheet_name=sheet_name,
                             header=None, nrows=self.HEADER_SEARCH_ROWS)
        for i in range(len(head)):
            if any(str(v).strip() == self.HEADER_ANCHOR for v in head.iloc[i].tolist()):
                if i != 8:
                    print(f"  LET OP: kopregel staat op index {i}, niet op de "
                          f"gebruikelijke 8 -- controleer of het bestandsformaat wijzigde")
                return i
        raise RuntimeError(
            f"Kopregel niet gevonden: geen cel {self.HEADER_ANCHOR!r} in de eerste "
            f"{self.HEADER_SEARCH_ROWS} rijen van sheet {sheet_name!r}. "
            "Het exportformaat is waarschijnlijk gewijzigd."
        )

    def _check_freshness(self, content: bytes) -> None:
        """Lees en beoordeel de 'automatically generated ... on: dd/mm/YYYY - HH:MM'-stempel.

        Een bevroren export ziet er van buiten identiek uit aan een verse: zelfde URL,
        HTTP 200, zelfde aantal rijen. Alleen deze stempel verraadt het.
        """
        head = pd.read_excel(BytesIO(content), sheet_name=self.SHEET_NAME,
                             header=None, nrows=3)
        stamp = ""
        for cell in head.astype(str).to_numpy().ravel().tolist():
            m = re.search(r"\b(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}:\d{2})", cell)
            if m:
                stamp = f"{m.group(1)} - {m.group(2)}"
                break
        if not stamp:
            print("  LET OP: geen exportstempel gevonden -- versheid niet te controleren")
            return
        print(f"  Export gegenereerd op: {stamp}")
        try:
            generated = datetime.strptime(stamp.split(" - ")[0], "%d/%m/%Y")
        except ValueError:
            return
        age = datetime.now() - generated
        if age > timedelta(days=self.MAX_EXPORT_AGE_DAYS):
            print(f"  WAARSCHUWING: export is {age.days} dagen oud "
                  f"(> {self.MAX_EXPORT_AGE_DAYS}); mogelijk bevroren bij EMA")

    def _load_atc_index(self, session: requests.Session) -> dict[str, tuple[str, str]]:
        """Bouw {genormaliseerde merknaam: (atc_code, vergunninghouder)}.

        Dekking is ~60%: de rest van de tekortmeldingen gaat over nationaal
        geregistreerde of generieke middelen ("Methotrexate", "ADHD medicines") die
        per definitie niet in de lijst met centraal toegelaten EMA-geneesmiddelen
        staan. Die 40% laten we LEEG; de bestaande INN->ATC-mappinglaag van het
        project pakt die op. Nooit een ATC gokken.
        """
        try:
            content = self._get(self.MEDICINES_XLSX_URL, session)
            header = self._find_header_row(content, 0)
            med = pd.read_excel(BytesIO(content), sheet_name=0, header=header)
        except Exception as exc:
            # Optionele verrijking: valt hij weg, dan leveren we gewoon lege ATC's.
            print(f"  ATC-verrijking overgeslagen ({type(exc).__name__}: {exc})")
            return {}

        name_col = "Name of medicine"
        atc_col = "ATC code (human)"
        mah_col = "Marketing authorisation developer / applicant / holder"
        missing = [c for c in (name_col, atc_col, mah_col) if c not in med.columns]
        if missing:
            print(f"  ATC-verrijking overgeslagen: kolommen ontbreken {missing}")
            return {}

        med = med[med["Category"].astype(str).str.strip() == "Human"]
        index: dict[str, tuple[str, str]] = {}
        for _, row in med.iterrows():
            key = self._norm_name(row[name_col])
            if not key:
                continue
            atc = self._text(row[atc_col])
            mah = self._text(row[mah_col])
            existing = index.get(key)
            if existing and existing[0] and atc and existing[0] != atc:
                # Zelfde merknaam met twee verschillende ATC's: dan weten we het niet.
                # Leeg zetten i.p.v. willekeurig de eerste kiezen.
                index[key] = ("", existing[1])
                continue
            if existing is None:
                index[key] = (atc, mah)
        print(f"  Medicijnenbestand: {len(med)} rijen, {len(index)} unieke merknamen")
        return index

    def _fetch_detail(self, url: str, session: requests.Session) -> tuple[str, str, bool]:
        """Haal 'Reason for shortage' en 'Member States affected' van de detailpagina.

        Deze twee velden staan NIET in de Excel en zijn juist de toegevoegde waarde
        van EMA boven de nationale registers (beoordeelde oorzaak + welke lidstaten).
        Afgeronde dossiers hebben vaak helemaal geen accordeon meer; dan is de reden
        echt afwezig en laten we het veld leeg.

        Derde returnwaarde is of de PAGINA is opgehaald. Dat onderscheid is essentieel:
        een lege reden omdat het dossier geen accordeon heeft en een lege reden omdat
        EMA ons blokkeert zien er in de CSV identiek uit. EMA rate-limit deze host stevig
        (HTTP 429; gemeten: 37-38 van de 80 detailpagina's in een gewone tweede run), en
        zonder dit onderscheid zakt de verrijking stilletjes weg terwijl de run "geslaagd"
        heet -- exact de stille-storingsval waar dit project al op stukliep.

        Trager ophalen lost dat NIET op (getest met 0,4s pauze plus herkansing: nog steeds
        38 mislukt, en de run liep van 19s naar 225s). Het budget is een per-tijdvak-limiet
        van EMA, geen snelheidskwestie. We halen dus gewoon snel op en zijn eerlijk over
        wat er niet lukte.

        Hier wordt bewust alleen ECHT netwerkfalen opgevangen. _rerun_targeted.py bewaakt
        iedere scraper met een SIGALRM die een gewone Exception opgooit; vuurt die tijdens
        een socketoperatie, dan verpakt urllib3 hem tot een requests.ConnectionError. Een
        `except Exception` -- en ook een `except requests.RequestException` -- slikt de
        bewaking daarmee op: waargenomen is een run van 225s die toch netjes "OK" heette.
        Daarom kijken we naar de diepste oorzaak: echt netwerkfalen eindigt in een OSError,
        een opgeslikt stopsignaal niet. Dat laatste laten we door.
        """
        try:
            r = session.get(url, timeout=30, headers=self.HEADERS)
            r.raise_for_status()
        except requests.RequestException as exc:
            oorzaak = exc
            while oorzaak.__cause__ is not None or oorzaak.__context__ is not None:
                oorzaak = oorzaak.__cause__ or oorzaak.__context__
            if not isinstance(oorzaak, (OSError, requests.RequestException)):
                raise oorzaak
            print(f"  LET OP: detailpagina niet opgehaald ({type(exc).__name__}) {url}")
            return "", "", False
        soup = BeautifulSoup(r.text, "lxml")
        found: dict[str, str] = {}
        for item in soup.select("div.accordion-item"):
            label_el = item.select_one("h3.accordion-header button span")
            body_el = item.select_one("div.accordion-body")
            if not label_el or not body_el:
                continue
            found[label_el.get_text(strip=True)] = body_el.get_text(" ", strip=True)
        return (found.get("Reason for shortage", ""),
                found.get("Member States affected", ""), True)

    # ── hoofdroutine ───────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        session = requests.Session()
        content = self._get(self.XLSX_URL, session)
        self._check_freshness(content)

        header = self._find_header_row(content, self.SHEET_NAME)
        df = pd.read_excel(BytesIO(content), sheet_name=self.SHEET_NAME, header=header)
        print(f"  Excel: {len(df)} datarijen, {len(df.columns)} kolommen "
              f"(kopregel index {header})")

        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise RuntimeError(f"Verwachte kolommen ontbreken in de export: {missing}")

        # Statussen altijd loggen: zo zie je meteen als EMA een waarde toevoegt.
        counts = df["Supply shortage status"].astype(str).str.strip().value_counts()
        print("  Statusverdeling in de bron:")
        for value, n in counts.items():
            print(f"    {value}: {n}")

        known = set(self.STATUS_MAP) | self.STATUS_EXCLUDE
        unknown = sorted(set(counts.index) - known)
        if unknown:
            # HARD FALEN. Een onbekende status stilletjes doorlaten of stilletjes
            # weggooien is allebei fout: we weten niet of het een tekort is.
            raise RuntimeError(
                f"Onbekende waarde(n) in 'Supply shortage status': {unknown}. "
                "Bepaal bewust of dit tekorten zijn en pas STATUS_MAP/STATUS_EXCLUDE aan."
            )

        status_raw = df["Supply shortage status"].astype(str).str.strip()
        excluded = int(status_raw.isin(self.STATUS_EXCLUDE).sum())
        df = df[status_raw.isin(self.STATUS_MAP)].copy()
        print(f"  Filter: {excluded} rij(en) uitgesloten ({', '.join(sorted(self.STATUS_EXCLUDE))}), "
              f"{len(df)} tekortrijen over")

        atc_index = self._load_atc_index(session)

        scraped_at = datetime.now().isoformat()
        records = []
        reasons_found = 0
        detail_failed = 0
        for _, row in df.iterrows():
            source_url = self._text(row["Shortage URL"])
            if source_url:
                reason, member_states, fetched = self._fetch_detail(source_url, session)
            else:
                reason, member_states, fetched = "", "", False
            if not fetched:
                detail_failed += 1
            if reason:
                reasons_found += 1

            medicine_name = self._text(row["Medicine affected"])
            atc_code, mah = atc_index.get(self._norm_name(medicine_name), ("", ""))

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "active_substance": self._text(
                    row["International non-proprietary name (INN) or common name"]),
                "strength": self._text(row["Strengths affected"]),
                # De bron kent geen verpakkingsgrootte en geen nationaal productnummer.
                # (Het medicijnenbestand heeft wel een "EMA product number", maar dat is
                # een toelatingsnummer en geen productnummer zoals de andere landen het
                # vullen -- die door elkaar halen zou de kolom onbruikbaar maken.)
                "package_size": "",
                "product_no": "",
                "atc_code": atc_code,
                "marketing_auth_holder": mah,
                "dosage_form": self._text(row["Pharmaceutical forms affected"]),

                # DATUMVAL 1: slechts een handvol dossiers heeft een echte startdatum.
                # NOOIT vullen met "First published date" (dat is de publicatiedatum van
                # de melding, niet het begin van het tekort) en NOOIT met de scrapedatum.
                # Leeg is hier het eerlijke antwoord.
                "shortage_start": self._parse_date(row["Start of shortage date"]),
                "first_published": self._parse_date(row["First published date"]),

                # DATUMVAL 2: "Expected resolution date" is in deze export volledig leeg,
                # en "Expected resolution" is VRIJE TEKST ("2027", "End of 2027",
                # "The expected duration of the shortage is unknown."). Dat is geen datum,
                # dus het gaat naar een apart tekstveld. estimated_end blijft daardoor in
                # de praktijk altijd leeg -- dat is correct, niet kapot.
                "estimated_end": self._parse_date(row["Expected resolution date"]),
                "estimated_end_text": self._text(row["Expected resolution"]),

                "last_updated": self._parse_date(row["Last updated date"]),
                "status": self.STATUS_MAP[str(row["Supply shortage status"]).strip()],
                # 'Availability of alternatives' is Yes/No/Unknown: een beschikbaarheids-
                # vraag, geen oorzaak. Daarom apart en niet als reason.
                "alternatives_available": self._text(row["Availability of alternatives"]),
                "reason": reason,
                "member_states": member_states,
                "source_url": source_url,
                "scraped_at": scraped_at,
            })

        result = pd.DataFrame(records)
        if not result.empty:
            lopend = result[result["status"] == "shortage"]
            ongoing = len(lopend)
            with_start = int((lopend["shortage_start"] != "").sum())
            print(f"  {len(result)} rijen | {result['medicine_name'].nunique()} unieke producten "
                  f"| {result['source_url'].nunique()} unieke dossier-URL's")
            print(f"  Lopend: {ongoing} (waarvan {with_start} met echte startdatum; "
                  f"de rest houdt shortage_start bewust leeg)")
            print(f"  Reden opgehaald van detailpagina: {reasons_found}/{len(result)} "
                  f"({len(result) - detail_failed} pagina's gelezen, {detail_failed} mislukt)")
            if detail_failed:
                print(f"  WAARSCHUWING: {detail_failed} detailpagina('s) niet opgehaald; "
                      "reden/lidstaten zijn daar LEEG omdat het ophalen faalde, niet omdat "
                      "de bron ze niet heeft")
            print(f"  ATC gevuld: {int(result['atc_code'].astype(bool).sum())}/{len(result)} "
                  f"| vergunninghouder: {int(result['marketing_auth_holder'].astype(bool).sum())}/{len(result)}")

            # BEWUST GEEN hard falen op mislukte detailpagina's. De 80 tekortrijen komen
            # uit de Excel en kloppen ook zonder verrijking; EMA laat routinematig zo'n
            # 40-50% van de detailverzoeken op 429 lopen, dus een drempel zou vooral
            # goede runs weggooien. Wat telt is dat het zichtbaar is: hierboven staat per
            # run hoeveel pagina's zijn gelezen en hoeveel er misten, zodat een lege
            # reden-kolom nooit meer als "de bron heeft geen redenen" kan worden gelezen.
        print(f"  Total: {len(result)} shortage records scraped")
        return result
