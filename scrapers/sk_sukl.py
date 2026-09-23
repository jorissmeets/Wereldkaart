"""Slowakije — ŠÚKL (Štátny ústav pre kontrolu liečiv).

De bron is GEEN lijst van lopende tekorten maar een MELDINGENLOGBOEK: elke regel is
één mededeling van een vergunninghouder over één verpakking (ŠÚKL-kód). Vier soorten:

    R  prerušenie      onderbreking van de levering        -> opent een tekort
    O  obnovenie       hervatting van de levering          -> sluit een tekort
    Z  zrušenie        definitieve stopzetting             -> uit de handel
    U  uvedenie        eerste introductie op de markt      -> sluit een tekort

Wie de R-regels simpelweg uitleest (wat deze scraper deed) zet het hele archief sinds
2019 als "actief" op de kaart: 9.667 rijen waarvan er maar ~900 echt lopen. De 6.802
O-meldingen zijn precies het tegendeel: bewijs dat het tekort voorbij is. Daarom draait
alles hier om de toestandsmachine in _open_meldingen().

De tekorten-CSV bevat maar zes kolommen en géén stof of ATC. Die stonden vroeger op
portal.sukl.sk/LiekDetail/, maar dat endpoint geeft sinds ~2026 een 404 voor ELK
product — de oude per-product-lookup leverde dus stilzwijgend lege velden op. Stof en
ATC komen nu uit het publieke ŠÚKL-geneesmiddelenregister (zoznam_liekov), koppelbaar
op exact dezelfde ŠÚKL-kód. Eén download in plaats van 6.119 losse requests.

Geen browser nodig, geen paginering (de CSV is één complete export), geen login.
"""
from __future__ import annotations

import io
import json
import re
import zipfile
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper


class SkSuklScraper(BaseScraper):
    """ŠÚKL-leveringsmeldingen, verrijkt met stof/ATC uit het geneesmiddelenregister."""

    # Complete export van alle meldingen sinds 2019 — geen paginering, geen filter.
    CSV_URL = "https://portal.sukl.sk/PreruseniePublic/?act=PrerusenieOznList&export=csv"

    # Maandelijkse momentopname van het register van geregistreerde geneesmiddelen.
    ZOZNAM_URL = "https://www.sukl.sk/verejne/Zoznam_liekov/zoznam_liekov_aktualny.zip"

    # Archiefsnapshots voor producten die intussen zijn uitgeschreven en dus NIET meer in
    # het actuele register staan. Het archief bundelt per jaar; daarbinnen zit per maand
    # nog een zip. Twee snapshots volstaan aantoonbaar: actueel alleen dekt 65,4% van de
    # gemelde producten, +2025-01 -> 79,3%, +2019-01 -> 99,0%. Meer snapshots toevoegen
    # levert nog hooguit een procent op en kost per stuk tientallen MB's download.
    # Volgorde = voorrang: actueel gaat vóór archief, nieuwer archief vóór ouder.
    ARCHIEF_SNAPSHOTS = (
        ("https://www.sukl.sk/verejne/Archiv/Lieky2025.zip", "zoznam_liekov_2025_01.zip", "2025_01"),
        ("https://www.sukl.sk/verejne/Archiv/Lieky2019.zip", "zoznam_liekov_2019_01.zip", "2019_01"),
    )

    # De lookup wordt lokaal gecachet: ~91 MB downloaden voor een herdraai van één land is
    # onnodig. Cache ligt naast de scrapers en valt onder *.csv in .gitignore.
    CACHE_DIR = Path(__file__).resolve().parent / "_cache"

    # Alleen kolommen die we echt gebruiken, anders is de cache onnodig groot.
    LOOKUP_KOLOMMEN = ("ATC kód", "atc_nazov_sk", "lie_sila", "lie_balenie", "form_kod")

    # De zes kolommen die de tekorten-CSV levert; meer zitten er niet in (geen stof, geen ATC).
    CSV_KOLOMMEN = ("Podanie", "Držiteľ", "Kód", "Liek", "Účinnosť", "Predmet")

    # Status per melding. 'supply_discontinuation' staat al in DISCONTINUED_STATUSES van
    # landkaart/build_data.py en 'anticipated' in UPCOMING_STATUSES; niet hernoemen zonder
    # daar mee te kijken.
    ST_TEKORT = "shortage"
    ST_AANGEKONDIGD = "anticipated"
    ST_UIT_DE_HANDEL = "supply_discontinuation"

    # Vergunninghouder staat in de CSV als "Zentiva k.s. (ZNT-3)": naam + ŠÚKL-firmacode.
    # Alle 426 houders hebben dat achtervoegsel; de code is registratie-administratie en
    # hoort niet in een bedrijfsnaam op de kaart.
    _FIRMACODE = re.compile(r"\s*\([A-Z0-9ÁČĎÉÍĽĹŇÓŔŠŤÚÝŽ&./ -]{2,14}\)\s*$")

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    }

    def __init__(self):
        super().__init__(
            country_code="SK",
            country_name="Slovakia",
            source_name="SUKL",
            base_url="https://portal.sukl.sk",
        )

    # ── downloaden ─────────────────────────────────────────────────────────────

    def _get(self, url: str, timeout: int = 120) -> bytes:
        resp = requests.get(url, headers=self.HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    # ── lookup-tabel: ŠÚKL-kód -> stof/ATC/sterkte/verpakking/vorm ──────────────

    @staticmethod
    def _lees_register(blob: bytes, bestand: str, engine: str) -> pd.DataFrame:
        """Lees zoznam_liekov uit een (geneste) zip.

        dtype=str is hier niet cosmetisch: de ŠÚKL-kód is een STRING. Er zijn 1.882
        codes met een leidende nul ("03542") en 7.532 met een letter ("2668E", "9369B").
        Laat pandas er getallen van maken en de join op de tekorten-CSV valt stil voor
        precies die codes, zonder foutmelding.
        """
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            with zf.open(bestand) as fh:
                return pd.read_excel(io.BytesIO(fh.read()), dtype=str, engine=engine)

    def _snapshot_naar_dict(self, df: pd.DataFrame) -> dict[str, dict]:
        """Registersnapshot -> {ŠÚKL-kód: {kolom: waarde}}."""
        if "ŠÚKL kód" not in df.columns:
            raise RuntimeError("kolom 'ŠÚKL kód' ontbreekt — registerformaat gewijzigd")
        # De archiefsnapshot van 2019 heeft 15 kolommen i.p.v. 20: lie_sila, lie_balenie
        # en form_kod bestonden toen nog niet. Ontbrekende kolommen blijven leeg — liever
        # een leeg veld dan een verzonnen sterkte.
        aanwezig = [k for k in self.LOOKUP_KOLOMMEN if k in df.columns]
        uit: dict[str, dict] = {}
        for rij in df[["ŠÚKL kód", *aanwezig]].itertuples(index=False):
            kod = self._tekst(rij[0])
            if kod:
                uit.setdefault(kod, {k: self._tekst(v) for k, v in zip(aanwezig, rij[1:])})
        return uit

    def _cache_pad(self, naam: str) -> Path:
        return self.CACHE_DIR / f"sk_zoznam_{naam}.csv"

    def _uit_cache(self, naam: str) -> dict[str, dict] | None:
        pad = self._cache_pad(naam)
        if not pad.exists():
            return None
        try:
            df = pd.read_csv(pad, dtype=str, keep_default_na=False, encoding="utf-8")
            return self._snapshot_naar_dict(df)
        except Exception as exc:                      # kapotte cache mag nooit fataal zijn
            print(f"  cache {naam} onbruikbaar ({exc}) — opnieuw ophalen")
            return None

    def _naar_cache(self, naam: str, tabel: dict[str, dict]) -> None:
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        rijen = [{"ŠÚKL kód": k, **v} for k, v in tabel.items()]
        pd.DataFrame(rijen).to_csv(self._cache_pad(naam), index=False, encoding="utf-8")

    def _actueel_versiestempel(self) -> str:
        """Last-Modified van de actuele register-zip, als cachesleutel.

        Het register verschijnt maandelijks opnieuw. Een vaste TTL zou of te vaak 91 MB
        binnenhalen of te lang met een verouderde lijst werken; de Last-Modified-header
        zegt precies wanneer ŠÚKL hem echt vernieuwd heeft.
        """
        try:
            resp = requests.head(self.ZOZNAM_URL, headers=self.HEADERS, timeout=30)
            return resp.headers.get("last-modified", "")
        except Exception:
            return ""

    def _bouw_lookup(self) -> dict[str, dict]:
        """Eén tabel ŠÚKL-kód -> stof/ATC, actueel aangevuld met twee archiefsnapshots."""
        lookup: dict[str, dict] = {}

        # 1. Actueel register. Cache alleen geldig zolang ŠÚKL dezelfde versie publiceert.
        stempel = self._actueel_versiestempel()
        stempel_pad = self.CACHE_DIR / "sk_zoznam_actueel.stamp"
        gecachet = ""
        if stempel_pad.exists():
            try:
                gecachet = json.loads(stempel_pad.read_text()).get("last_modified", "")
            except Exception:
                gecachet = ""
        actueel = self._uit_cache("actueel") if (stempel and stempel == gecachet) else None
        if actueel is None:
            print(f"  register ophalen (versie {stempel or 'onbekend'})...")
            blob = self._get(self.ZOZNAM_URL)
            actueel = self._snapshot_naar_dict(
                self._lees_register(blob, "zoznam_liekov.xlsx", "openpyxl"))
            self._naar_cache("actueel", actueel)
            self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
            stempel_pad.write_text(json.dumps({"last_modified": stempel}))
        print(f"  register actueel: {len(actueel)} producten")
        lookup.update(actueel)

        # 2. Archief voor uitgeschreven producten. Deze snapshots zijn historisch en
        #    veranderen nooit meer, dus de cache verloopt niet. setdefault: actueel wint.
        for jaar_url, binnen_zip, naam in self.ARCHIEF_SNAPSHOTS:
            snapshot = self._uit_cache(naam)
            if snapshot is None:
                print(f"  archief {naam} ophalen...")
                jaar_blob = self._get(jaar_url, timeout=180)
                # Geneste zip: Lieky<jaar>.zip bevat per maand een zip met daarin de .xls.
                with zipfile.ZipFile(io.BytesIO(jaar_blob)) as zf:
                    maand_blob = zf.read(binnen_zip)
                # Archief is .xls (BIFF), niet .xlsx -> xlrd i.p.v. openpyxl.
                snapshot = self._snapshot_naar_dict(
                    self._lees_register(maand_blob, "zoznam_liekov.xls", "xlrd"))
                self._naar_cache(naam, snapshot)
            nieuw = 0
            for kod, waarden in snapshot.items():
                if kod not in lookup:
                    lookup[kod] = waarden
                    nieuw += 1
            print(f"  archief {naam}: {len(snapshot)} producten, {nieuw} nieuw -> {len(lookup)} totaal")

        return lookup

    # ── meldingen -> toestand ──────────────────────────────────────────────────

    @staticmethod
    def _tekst(waarde) -> str:
        if waarde is None or (isinstance(waarde, float) and pd.isna(waarde)):
            return ""
        s = str(waarde).strip()
        return "" if s.lower() == "nan" else s

    def _open_meldingen(self, df: pd.DataFrame) -> dict[str, dict]:
        """Bepaal per ŠÚKL-kód de toestand NA alle meldingen.

        Per product worden de meldingen chronologisch op Účinnosť (ingangsdatum) afgelopen.
        R en Z zijn twee losse sporen — een onderbreking en een stopzetting zijn niet
        hetzelfde en de kaart moet ze kunnen scheiden. O en U sluiten beide sporen.

        Valkuil 1: sorteer op Účinnosť, NIET op Podanie. Die twee lopen geregeld uiteen
        (Canephron 86819: R met ingang 30-06 maar ingediend op 21-08, Z met ingang 01-08
        maar ingediend op 10-06). De ingangsdatum bepaalt wat er feitelijk geldt.

        Valkuil 1b: bij een GELIJKE Účinnosť beslist Podanie, niet de bronvolgorde. De
        export van ŠÚKL staat op Podanie AFLOPEND, dus bronvolgnummer als tiebreak laat
        de OUDSTE melding als laatste winnen — precies verkeerd om. 55 producten hebben
        een opener en een sluiter op dezelfde ingangsdatum; met de bronvolgorde kregen er
        44 de verkeerde uitkomst (o.a. Suboxone 36928/36930: R met ingang 31-12-2025
        ingediend 08-09-2025, Z met dezelfde ingang ingediend 24-05-2026 — die kwam als
        lopend tekort op de kaart terwijl de houder de levering definitief had gestaakt).

        Valkuil 2: staat er na een openstaande R nog een Z, dan is het product definitief
        uit de handel en géén lopend tekort meer (Olicard 40 mg: R vanaf 01-06-2026,
        Z vanaf 05-08-2026). Daarom wint de LAATSTE openstaande melding. Dat scheelt 202
        producten die anders als lopend tekort op de kaart zouden komen terwijl de houder
        de levering al definitief heeft gestaakt.

        Valkuil 3: toekomstige Účinnosť-waarden (tot 2028) zijn vooraf aangekondigde
        onderbrekingen. Die tellen niet als "loopt nu"; ze krijgen status 'anticipated'.
        Een toekomstige O sluit hier wel al af — een hervatting die over twee dagen ingaat
        is beter als afgelopen te tonen dan als lopend tekort.
        """
        # Bewust op kolomnaam en niet via itertuples-attributen: de kolomnamen bevatten
        # diakritieken en zodra ŠÚKL er iets aan verandert geeft itertuples stilletjes
        # hernoemde velden (_1, _2) terug en zou de scraper nul rijen opleveren zonder
        # één foutmelding. Liever hier hard stuklopen.
        ontbreekt = [k for k in self.CSV_KOLOMMEN if k not in df.columns]
        if ontbreekt:
            raise RuntimeError(f"tekorten-CSV mist kolommen {ontbreekt}; "
                               f"gekregen: {list(df.columns)}")

        gebeurtenissen: dict[str, list[tuple]] = defaultdict(list)
        kolommen = [df[k] for k in ("Kód", "Účinnosť", "Predmet", "Podanie", "Liek", "Držiteľ")]
        for kod, ucinnost, predmet, podanie, liek, drzitel in zip(*kolommen):
            kod = self._tekst(kod)
            if not kod:
                continue
            gebeurtenissen[kod].append((
                self._tekst(ucinnost),                 # ingangsdatum
                self._tekst(podanie),                  # tiebreak: indieningstijdstip
                self._tekst(predmet).upper(),
                self._tekst(podanie)[:10],             # meldingsdatum
                self._tekst(liek),
                self._tekst(drzitel),
            ))

        vandaag = date.today().isoformat()
        open_toestand: dict[str, dict] = {}
        for kod, reeks in gebeurtenissen.items():
            reeks.sort(key=lambda g: (g[0], g[1]))
            spoor: dict[str, dict | None] = {"R": None, "Z": None}
            rang = {"R": -1, "Z": -1}
            for pos, (ucinnost, _, predmet, podanie, liek, drzitel) in enumerate(reeks):
                if predmet in ("R", "Z"):
                    spoor[predmet] = {"start": ucinnost, "podanie": podanie,
                                      "liek": liek, "drzitel": drzitel, "predmet": predmet}
                    rang[predmet] = pos
                elif predmet in ("O", "U"):
                    spoor["R"] = spoor["Z"] = None
                    rang["R"] = rang["Z"] = -1
            # Laatste openstaande melding wint (zie valkuil 2).
            kandidaat = max((s for s in (spoor["R"], spoor["Z"]) if s),
                            key=lambda s: rang[s["predmet"]], default=None)
            if not kandidaat:
                continue
            if kandidaat["predmet"] == "Z":
                # Ook een Z met toekomstige ingangsdatum (28 stuks) blijft hier
                # 'supply_discontinuation': een aangekondigde stopzetting is geen tekort
                # dat vanzelf overgaat, en build_data leest de status vóór de datum.
                kandidaat["status"] = self.ST_UIT_DE_HANDEL
            elif kandidaat["start"] and kandidaat["start"] > vandaag:
                kandidaat["status"] = self.ST_AANGEKONDIGD
            else:
                kandidaat["status"] = self.ST_TEKORT
            open_toestand[kod] = kandidaat
        return open_toestand

    # ── controle: ingangsdatum die niet bij de indieningsdatum past ─────────────

    # Grens tussen "ver vooruit aangekondigd" en "vrijwel zeker een jaartal-tikfout".
    # Gemeten over alle 7.720 R-meldingen in de export van 22-09-2026: mediaan 0 dagen,
    # p95 48, p99 76. Tussen 304 en 365 dagen zit een gat en daarboven staan er nog maar
    # vijf. 365 is dus geen rond getal dat we mooi vinden, maar de onderkant van dat gat.
    MAX_VOORLOOP_DAGEN = 365

    def _meld_ongeloofwaardige_ingangsdatums(self, rauw: pd.DataFrame,
                                             toestand: dict[str, dict]) -> None:
        """Maak zichtbaar welke OPENSTAANDE meldingen een onwaarschijnlijke ingangsdatum hebben.

        WAT DIT VANGT. De toestandsmachine sorteert op Účinnosť (ingangsdatum). Een melding
        met een ingangsdatum ver in de toekomst belandt daardoor ALTIJD achteraan in de reeks
        van een product en overleeft dus elke hervatting die later is INGEDIEND. Zolang die
        datum klopt is dat precies goed (09530 Ramipril: R ingediend 30-08-2026 met ingang
        01-10-2026, 32 dagen; 80644 Javlor: 272 dagen — echte aankondigingen). Staat er een
        jaartal-tikfout in, dan blijft het product voor onbepaalde tijd als tekort staan:

          0498E Atorvastatín/Ezetimib Teva — R ingediend 04-02-2025, ingang 20-02-2028
              (1.111 dagen, de grootste voorloop in de hele export; alle andere meldingen van
              dit product hebben 0-64 dagen). Laatste woord van de houder is een HERVATTING
              op 05-11-2025, maar het product staat als 'anticipated' vanaf 2028 op de kaart.
          4989C Gamunex — R ingediend 10-12-2025, ingang 10-12-2026 (exact één jaar; alle
              14 andere meldingen van dit product hebben ingang == indieningsdatum). Hervat
              gemeld op 24-07-2026.
          2376D Latanoprost/timolol Olikla — R ingediend 07-05-2024, ingang 07-05-2025 (exact
              één jaar). Hervat gemeld op 28-05-2024; staat nu als LOPEND tekort.
          95236 Sertralin Actavis 100 mg — R ingediend 22-12-2023, ingang 27-12-2024. Hervat
              gemeld op 29-04-2024; staat nu als LOPEND tekort.
          32720 Xyzal 50x5 mg — R ingediend 01-06-2021, ingang 01-09-2022. Twee LATER
              ingediende Z-meldingen zetten het product per 2022 definitief uit de handel;
              staat nu als lopend tekort sinds 2022.

        WAAROM ALLEEN MELDEN EN NIET REPAREREN. Er is geen structureel verschil tussen deze
        vijf en de terechte aankondigingen: in beide gevallen volgt er een later ingediende O
        met een eerdere ingangsdatum. Het enige onderscheid is of de datum plausibel is, en
        dat is een oordeel, geen bronfeit — ŠÚKL publiceert geen correctie. Zo'n regel
        stilzwijgend weggooien of de datum "herstellen" zou data verzinnen. Dit hoort op tafel
        bij Jesper/Nicky als kaartdefinitie, niet in een stille scraperregel. Het gaat om
        5 van 2.726 producten (3 daarvan binnen de 813 lopende tekorten).
        """
        laatste: dict[str, tuple[str, str]] = {}
        for kod, podanie, predmet in zip(rauw["Kód"], rauw["Podanie"], rauw["Predmet"]):
            kod = self._tekst(kod)
            podanie = self._tekst(podanie)
            if not kod or not podanie:
                continue
            if kod not in laatste or podanie > laatste[kod][0]:
                laatste[kod] = (podanie, self._tekst(predmet).upper())

        verdacht = []
        for kod, waarde in toestand.items():
            start, podanie = waarde.get("start", ""), waarde.get("podanie", "")
            try:
                voorloop = (date.fromisoformat(start) - date.fromisoformat(podanie[:10])).days
            except ValueError:
                continue
            if voorloop >= self.MAX_VOORLOOP_DAGEN:
                verdacht.append((voorloop, kod, waarde, laatste.get(kod, ("", ""))))

        if not verdacht:
            return
        print(f"  LET OP: {len(verdacht)} openstaande melding(en) met een ingangsdatum "
              f">= {self.MAX_VOORLOOP_DAGEN} dagen na de indiening — mogelijk een jaartal-"
              f"tikfout in de bron. Niet gecorrigeerd, wel om te controleren:")
        for voorloop, kod, waarde, (laatste_podanie, laatste_predmet) in sorted(verdacht,
                                                                               reverse=True):
            # Alleen "weersproken" als het LAATSTE woord van de houder een ANDERE melding is
            # dan de openstaande zelf. Anders is de aankondiging gewoon het jongste bericht
            # (55407 MUSTOPHORAN: Z ingediend 14-03-2024 met ingang 01-06-2025 — ver vooruit,
            # maar niets spreekt het tegen).
            anders = laatste_podanie[:10] != waarde["podanie"][:10]
            weersproken = (" — later ingediend weersproken door "
                           f"{laatste_predmet} op {laatste_podanie[:10]}"
                           if anders and laatste_predmet in ("O", "U", "Z") else "")
            print(f"    {kod} {waarde['predmet']} ingang {waarde['start']} "
                  f"(ingediend {waarde['podanie']}, +{voorloop} dagen) "
                  f"-> status {waarde['status']}{weersproken}: {waarde['liek'][:55]}")

    # ── scrape ─────────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # dtype=str: zie _lees_register — een int-cast sloopt codes als "03542"/"2668E".
        rauw = pd.read_csv(io.BytesIO(self._get(self.CSV_URL)), sep=";",
                           dtype=str, encoding="utf-8-sig")
        print(f"  CSV: {len(rauw)} meldingen over {rauw['Kód'].nunique()} producten")
        print("  verdeling: " + ", ".join(
            f"{k}={v}" for k, v in sorted(rauw["Predmet"].value_counts().items())))

        # Verssheidscontrole: de export is een logboek zonder publicatiedatum, dus de
        # jongste meldingsdatum is het enige signaal dat we niet naar een bevroren
        # kopie kijken (de GR-valkuil: maandenoude data die er actueel uitziet).
        jongste = self._tekst(rauw["Podanie"].max())[:10]
        print(f"  jongste melding: {jongste}")
        if jongste and (date.today() - datetime.strptime(jongste, "%Y-%m-%d").date()).days > 30:
            print(f"  LET OP: jongste melding is {jongste} — bron mogelijk bevroren")

        toestand = self._open_meldingen(rauw)
        per_status: dict[str, int] = defaultdict(int)
        for waarde in toestand.values():
            per_status[waarde["status"]] += 1
        print(f"  na toestandsmachine: {len(toestand)} producten met een openstaande melding "
              + ", ".join(f"{k}={v}" for k, v in sorted(per_status.items())))

        self._meld_ongeloofwaardige_ingangsdatums(rauw, toestand)

        lookup = self._bouw_lookup()

        records = []
        met_atc = met_stof = 0
        nu = datetime.now().isoformat()
        for kod, waarde in toestand.items():
            extra = lookup.get(kod, {})
            atc = self._tekst(extra.get("ATC kód")).upper()
            atc_naam = self._tekst(extra.get("atc_nazov_sk"))

            # atc_nazov_sk is de naam die bij de ATC-code hoort. Alleen op niveau 5 (7
            # tekens, bv. C09CA04) is dat een werkzame stof. Op niveau 3/4 is het een
            # groepsnaam — "Antiseptiká", "Rozpúšťadlá a riedidlá" — en die als werkzame
            # stof wegschrijven vervuilt elke stofmatch stroomafwaarts. Dan liever leeg.
            stof = atc_naam if len(atc) == 7 else ""
            if atc:
                met_atc += 1
            if stof:
                met_stof += 1

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": waarde["liek"],
                "active_substance": stof,
                "strength": self._tekst(extra.get("lie_sila")),
                "package_size": self._tekst(extra.get("lie_balenie")),
                "product_no": kod,
                "atc_code": atc,
                "marketing_auth_holder": self._FIRMACODE.sub("", waarde["drzitel"]).strip(),
                "dosage_form": self._tekst(extra.get("form_kod")),
                # Allebei echte bronvelden. NOOIT de scrapedatum invullen: een lege datum
                # is eerlijk, een scrapedatum als tekortstart is een leugen (fout AT/DK).
                "shortage_start": waarde["start"],
                "notification_date": waarde["podanie"],
                # De bron kent geen verwachte einddatum; die blijft dus leeg.
                "estimated_end": "",
                # Meldingsdatum van de nog openstaande melding. Hierdoor kan build_data
                # zien hoe vers een melding is; een R uit 2019 die nooit is afgemeld hoort
                # niet als vers tekort te tellen. Bewust NIET op de scrapedatum zetten —
                # dit is een logboek, geen actuele-tekortenlijst zoals CZ.
                "last_updated": waarde["podanie"],
                "status": waarde["status"],
                "notification_type": waarde["predmet"],
                "scraped_at": nu,
            })

        df = pd.DataFrame(records)
        n = max(len(df), 1)
        print(f"  verrijking: ATC {met_atc}/{len(df)} ({met_atc / n * 100:.1f}%), "
              f"werkzame stof {met_stof}/{len(df)} ({met_stof / n * 100:.1f}%)")
        print(f"  Total: {len(df)} shortage records scraped "
              f"({df['product_no'].nunique() if len(df) else 0} unieke producten)")
        return df
