"""Scraper voor Maleisie (NPRA) - tekorten en marktterugtrekkingen uit een publiek Google Sheet."""

import re
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

from scrapers.base_scraper import BaseScraper


class MyNpraScraper(BaseScraper):
    """Scraper voor NPRA (Maleisie) medicijntekorten/-stopzettingen.

    De NPRA publiceert haar tekortenlijst niet als pagina maar als Google Sheet.
    """

    SHEET_ID = "1wH8oW7PMUULnIvn2AnEmYliHQ_CJdn3VLUpYvhDVsyo"

    # WAAROM niet /export?format=csv: het Drive-export-endpoint eist sinds enige tijd
    # Drive-LEESRECHTEN op het bestand. Dit sheet is niet gedeeld als "iedereen met de link",
    # maar wel "gepubliceerd op het web". Gevolg: /export geeft 401 terwijl exact hetzelfde
    # sheet publiek leesbaar is. De bron is dus NIET dood en niet afgeschermd - het is puur
    # een verkeerd endpoint. Trap hier niet opnieuw in door de bron als verloren af te schrijven.
    #
    # WAAROM /pub en niet /gviz/tq (validatie 22-09): gviz leidt PER KOLOM een type af en geeft
    # null terug voor elke cel die niet in dat type past. Beide datumkolommen worden zo als date
    # getypeerd, waarna gviz 33 gevulde broncellen stilzwijgend leegmaakt: 11 startdatums
    # ("Ongoing", "December 2026", "19/08/2026 (Deregistration ongoing)") en 22 einddatums
    # ("30/04/2027 (Planned deregistration)", "2027/2028", ...). Het aantal RIJEN en de
    # MAL-nummers blijven daarbij exact gelijk, dus een controle op recordaantal ziet dit NIET.
    # In de bron heeft 115/115 een startdatum; via gviz leek dat er 104. Niet terugzetten.
    CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/pub?output=csv"

    # Fallback op gviz. Zelfde 115 records en dezelfde MAL-nummers, maar met bovenstaand
    # dataverlies in de datumkolommen - bewust alleen als noodgreep, niet als gelijkwaardig.
    # /pub geeft een paar volledig lege staartregels; die vallen weg in _echte_records().
    # LET OP: de vorm /spreadsheets/d/e/<SHEET_ID>/pub?output=csv geeft 404. Die verwacht een
    # apart publicatie-id, niet het sheet-id; niet "repareren" door hem terug te zetten.
    FALLBACK_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv"

    def __init__(self):
        super().__init__(
            country_code="MY",
            country_name="Malaysia",
            source_name="NPRA",
            base_url="https://www.npra.gov.my",
        )

    # ── Hulpfuncties ────────────────────────────────────────────────────────────

    def _txt(self, val) -> str:
        """Tekstveld opschonen. Meerdere velden (o.a. Active Ingredient bij combinatie-
        preparaten) bevatten harde regeleindes uit de sheet-cel. Witruimte platslaan houdt
        alle tokens intact maar maakt de CSV en de latere stof/PRK-matching bruikbaar."""
        if pd.isna(val):
            return ""
        s = re.sub(r"\s+", " ", str(val)).strip()
        return "" if s.lower() == "nan" else s

    def _parse_date(self, val) -> str | None:
        """Datum naar ISO. Geeft bewust None als er niets te parsen valt.

        WAAROM geen fallback op de scrapedatum: 8 records hebben geen bruikbare startdatum (de
        bron zegt daar "Ongoing") en 70 geen einddatum. Een scrapedatum invullen maakt van
        "onbekend" een onware feitelijke bewering over wanneer een tekort begon - daar is dit
        project bij AT/DK eerder op stukgelopen. Leeg is hier het juiste antwoord.

        LET OP bij het herijken van die aantallen: ze horen bij het /pub-endpoint. Via gviz leken
        het er 11 en 84, puur doordat dat endpoint gevulde tekstcellen in datumkolommen op null
        zet (zie CSV_URL). "Ontbrekende" datums zijn dus eerst een endpoint-vraag, pas daarna
        een bron-vraag.
        """
        if pd.isna(val) or not val:
            return None
        # Witruimte platslaan: de bron zet de toelichting vaak achter een harde regeleinde
        # ("30/04/2027 \n(Planned deregistration)").
        val = re.sub(r"\s+", " ", str(val)).strip()
        if not val:
            return None

        def _probeer(s: str) -> str | None:
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%B %Y", "%b %Y"):
                try:
                    return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None

        gevonden = _probeer(val)
        if gevonden:
            return gevonden
        # Geannoteerde datums: de datum staat vooraan, de uitleg erachter. Alleen de KOP
        # meenemen; de toelichting zelf is geen datum. Bewust streng: "2027/2028",
        # "Q1 2027" en "Ongoing" matchen niet en blijven dus leeg in plaats van dat we
        # er een precieze datum bij verzinnen.
        kop = re.match(r"^(\d{1,2}[/-]\d{1,2}[/-]\d{4}|[A-Za-z]+ \d{4})\b", val)
        return _probeer(kop.group(1)) if kop else None

    def _parse_atc(self, val) -> str:
        """ATC-code opschonen zonder hem te verzinnen.

        Drie soorten ruis in deze bron, elk met een andere juiste behandeling:
        - "NIL" is de letterlijke invulling van de bron voor "niet opgegeven" -> leeg maken,
          anders komt de tekst NIL als ATC-code de pijplijn in.
        - "H01B A02" bevat een tikfout-spatie. build_data.extract_atc5() splitst op witruimte
          en houdt dan "H01B" over, waardoor het record van de kaart valt. Spaties weghalen
          geeft H01BA02 (desmopressine) en dat klopt met de stofnaam in dezelfde regel.
        - "N07BA"/"V03AX" zijn ATC4: de bron is hier echt minder specifiek. Ongewijzigd
          doorgeven; build_data heeft ATC5 nodig en laat ze terecht liggen. Zelf een vijfde
          niveau raden zou data verzinnen.
        """
        s = self._txt(val)
        if not s or s.upper() in ("NIL", "N/A", "-"):
            return ""
        compact = s.replace(" ", "").upper()
        if re.fullmatch(r"[A-Z]\d{2}[A-Z]{2}\d{2}", compact):
            return compact
        return s

    def _normaliseer_status(self, status: str, disruption: str) -> str:
        """Bron-status + soort verstoring -> projectconventie (zie build_data.derive_status).

        DIT IS EEN KAARTDEFINITIE, geen technisch detail. Twee eerdere fouten komen hier samen:

        1) De resolved-ruis van CA: de bron bevat 47 AFGEHANDELDE meldingen. Die mogen niet als
           lopend tekort op de kaart. Ze worden niet weggegooid maar als "resolved" doorgegeven,
           zodat build_data ze correct classificeert en ze bruikbaar blijven voor historie.
        2) De Estland-valkuil: 74 van de 115 regels zijn Discontinuation (marktterugtrekking),
           geen leveringsprobleem. Stilzwijgend meetellen als tekort blaast het cijfer op -
           precies wat bij EE met de filterwaarde "beide" gebeurde. Ze krijgen daarom een eigen
           status ("discontinued" / "to be discontinued") in plaats van te worden meegeteld.

        Netto op de kaart: 29 actief + 1 upcoming = 30 echte open tekorten, 38 discontinued,
        47 resolved. We filteren bewust NIETS weg in de scraper: de kaartdefinitie hoort in
        build_data thuis, zodat Jesper/Nicky de grens kunnen verleggen zonder te herscrapen.
        """
        s = status.strip().lower()
        d = disruption.strip().lower()

        if s == "resolved":
            return "resolved"
        if d == "discontinuation":
            # "to be discontinued" staat al in build_data.DISCONTINUED_STATUSES en dekt de
            # aangekondigde terugtrekking exact.
            return "to be discontinued" if s == "anticipated" else "discontinued"
        if s == "anticipated":
            return "anticipated"
        return "shortage"

    def _haal_csv(self) -> str:
        """CSV ophalen, met de publicatie-export als terugvaloptie."""
        laatste_fout = None
        for url in (self.CSV_URL, self.FALLBACK_URL):
            try:
                resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
                resp.raise_for_status()
                # Google geeft bij een rechtenprobleem een HTML-foutpagina met status 200 terug;
                # daarom op inhoud controleren en niet alleen op de statuscode vertrouwen.
                if resp.text.lstrip().startswith("<"):
                    raise ValueError("HTML in plaats van CSV ontvangen")
                return resp.text
            except Exception as e:
                laatste_fout = e
                print(f"  Endpoint mislukt ({url.split('/')[-1]}): {str(e)[:80]}")
        raise RuntimeError(f"Geen bruikbare CSV van NPRA-sheet: {laatste_fout}")

    def _echte_records(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Structuurruis uit het sheet filteren.

        Het sheet bevat twee soorten niet-records die er als data uitzien:
        - 3 groepskopregels (Status gevuld, verder leeg; het AANTAL staat in de kolom
          Type of Disruption, bv. Current/40).
        - 1 voettekstregel "Data published as of ..." waarbij de TIMESTAMP in de kolom
          Product Name staat.

        WAAROM niet alleen dropna(subset=["Product Name"]): dat vangt de groepskoppen wel maar
        de voettekst niet, want die heeft een gevulde Product Name. Resultaat was 116 rijen met
        een nepgeneesmiddel dat letterlijk een datum als naam had. De eis dat OOK Status gevuld
        is, sluit beide soorten ruis uit en laat precies de 115 echte records over.
        """
        return raw[raw["Status"].notna() & raw["Product Name"].notna()].copy()

    def _lees_bron_timestamp(self, raw: pd.DataFrame) -> str | None:
        """Timestamp uit de voettekstregel "Data published as of <timestamp>" halen.

        PAS OP - dit is GEEN publicatiedatum (gemeten 22-09). De cel is een levende klok in het
        sheet zelf: bij herhaald ophalen liep hij mee met de actuele tijd in Maleisie (om 20:21:02
        MYT stond er 20:21:02), terwijl de 115 records ongewijzigd bleven. Hij staat dus altijd op
        vandaag, ook als NPRA het sheet maandenlang niet bijwerkt.

        Gevolg voor de interpretatie: deze waarde zegt NIETS over hoe vers de data is en mag nooit
        als bewijs van versheid worden aangehaald. Hij is functioneel gelijk aan de scrapedatum;
        we zetten hem op last_updated om dezelfde reden als CZ/SUKL in build_data r.466 (een
        actuele meldlijst mag niet door de >1-jaar-inactiefregel verborgen worden), maar de keerzijde
        is dat die regel voor MY daardoor nooit meer kan aanslaan. Gecontroleerd: vandaag maakt dat
        geen verschil (alle 29 lopende tekorten hebben een startdatum binnen het jaar, dus met en
        zonder deze waarde is de uitkomst identiek). Als MY ooit stilvalt, merkt de kaart dat niet
        automatisch - dat moet dan handmatig opvallen.
        """
        voet = raw[raw["Status"].isna() & raw["Product Name"].notna()]
        for waarde in voet["Product Name"]:
            datum = self._parse_date(self._txt(waarde).split(" ")[0])
            if datum:
                return datum
        return None

    # ── Hoofdroutine ────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        raw = pd.read_csv(StringIO(self._haal_csv()), dtype=str)
        print(f"  Gedownload: {len(raw)} ruwe regels")

        bron_datum = self._lees_bron_timestamp(raw)
        print(f"  Bron gepubliceerd op: {bron_datum or 'onbekend'}")

        echt = self._echte_records(raw)
        print(f"  Na wegfilteren groepskoppen + voettekst: {len(echt)} records")

        scraped_at = datetime.now().isoformat()
        records = []
        for _, row in echt.iterrows():
            bron_status = self._txt(row.get("Status"))
            disruptie = self._txt(row.get("Type of Disruption"))
            houder = self._txt(row.get("Product Registration Holder (PRH)"))
            mal = self._txt(row.get("Product Registration (MAL)"))

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": self._txt(row.get("Product Name")),
                "active_substance": self._txt(row.get("Active Ingredient")),
                "strength": self._txt(row.get("Strength")),
                "package_size": "",  # bron heeft geen verpakkingsgrootte
                "dosage_form": self._txt(row.get("Dosage Form")),
                "atc_code": self._parse_atc(row.get("ATC Product Code")),
                # marketing_auth_holder en product_no zijn de contractnamen die build_data
                # verwacht. company_name/registration_number blijven staan omdat build_data
                # daar alleen als FALLBACK op terugvalt - liever niet van een fallback afhangen.
                "marketing_auth_holder": houder,
                "product_no": mal,
                "company_name": houder,
                "registration_number": mal,
                "manufacturer": self._txt(row.get("Product Manufacturer")),
                "status": self._normaliseer_status(bron_status, disruptie),
                # Ruwe bronwaarden bewaren zodat de kaartdefinitie herzien kan worden
                # zonder opnieuw te scrapen.
                "source_status": bron_status,
                "disruption_type": disruptie,
                "reason": self._txt(row.get("Reason for Discontinuation / Shortage")),
                "shortage_start": self._parse_date(row.get("Supply Impact Start Date")),
                "estimated_end": self._parse_date(row.get("Supply Impact End Date")),
                "last_updated": bron_datum,
                "mitigation_prh": self._txt(row.get("Mitigation Plan by PRH for shortage status")),
                "mitigation_npra": self._txt(row.get("Mitigation Plan by NPRA")),
                "alternatives": self._txt(row.get("Alternative Registered Products Available")),
                "scraped_at": scraped_at,
            })

        df = pd.DataFrame(records)

        # Zelfcontrole tegen het totaal dat de bron ZELF noemt: de groepskopregels bevatten per
        # status het aantal (bv. Current/40). Wijkt dat af, dan is er data gemist of dubbel
        # opgehaald en moet je dat uitzoeken in plaats van het getal te geloven.
        koppen = raw[raw["Status"].notna() & raw["Product Name"].isna()]
        verwacht = sum(int(v) for v in koppen["Type of Disruption"].dropna()
                       if str(v).strip().isdigit())
        if verwacht and verwacht != len(df):
            print(f"  LET OP: bron noemt zelf {verwacht} records, wij hebben er {len(df)}")
        else:
            print(f"  Controle OK: bron noemt zelf {verwacht} records")

        open_tekorten = df[df["status"].isin(["shortage", "anticipated"])]
        print(f"  Totaal: {len(df)} records "
              f"({len(open_tekorten)} open tekort, "
              f"{len(df[df['status'].str.contains('discontinued')])} discontinued, "
              f"{len(df[df['status'] == 'resolved'])} resolved)")
        print(f"  Unieke registratienummers: {df['product_no'].nunique()}")
        return df
