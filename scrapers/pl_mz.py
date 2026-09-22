"""Scraper voor Polen — Ministerstwo Zdrowia, wykaz leków zagrożonych brakiem dostępności.

WAT DIT IS (lees dit voordat je de cijfers gebruikt)
----------------------------------------------------
Dit is GEEN tekortmelding met start- en einddatum zoals BE/FR/DE, en ook geen register
van handelsvergunningen (dat is keyword 6 op dezelfde site — bewust NIET gebruikt, dat is
precies de LT/TR/EE-fout waarbij een register als tekortenlijst werd ingelezen).

Het is de tweemaandelijkse ministeriële wykaz onder art. 37av ust. 14 Prawo farmaceutyczne:
producten die met onbeschikbaarheid worden BEDREIGD, en waarvoor export/intracommunautaire
verkoop daardoor meldings- en verbodsplichtig wordt. Preventief dus, niet curatief.
Daarom status="at_risk_of_shortage".

LET OP voor het dashboard: build_data.py kent "at_risk_of_shortage" niet in RESOLVED/
DISCONTINUED/UPCOMING_STATUSES, dus derive_status() maakt er "active" van. Zonder een
expliciete keuze van Jesper/Nicky belanden ~400 preventieve PL-records ongemerkt tussen de
echte tekorten. De ruwe status blijft als `sr` in data.json staan, dus filteren/apart tonen
kan zonder deze scraper aan te passen. Die beslissing hoort vóór livegang gemaakt te zijn.

BRON
----
Index : https://dziennikmz.mz.gov.pl/api/keywords/55  (JSON, publiek, geen auth/headers nodig)
PDF   : https://dziennikmz.mz.gov.pl/GetActPdf.ashx?year=..&book=0&position=..

De index-pagina zelf is een SPA; de vorige versie van deze scraper strandde daarop met
INDEX_API = None. Er is geen browser nodig: kale requests zonder User-Agent, Referer of
Origin krijgen zowel de JSON als de PDF gewoon binnen.

WAAROM DE DATUMVELDEN LEEG BLIJVEN
----------------------------------
De bron kent GEEN datum per product. De enige datums zijn lijst-breed: de publicatiedatum
van het obwieszczenie en de peildatum ("na dzień ..."). De publicatiedatum als shortage_start
invullen zou onwaar zijn — het overgrote deel van de GTIN's staat al maanden tot jaren op
opeenvolgende wykazy — en het is exact de AT/DK-bug (publicatie-/scrapedatum als startdatum)
waar dit project eerder op is stukgelopen. Dus: shortage_start en estimated_end LEEG, de
publicatiedatum in last_updated, en de peildatum + het Dz.-Urz.-nummer in `reason`.
Een echte startdatum is wél af te leiden (zie ONAFGEMAAKT hieronder), maar dat is een
tweede stap die pas zin heeft als het dashboard er om vraagt.

Om dezelfde reden schrijft deze scraper NIET de kolom `published_date`: build_data.py
gebruikt die als terugval voor shortage_start, wat de publicatiedatum alsnog als tekortstart
op de kaart zou zetten.

ONAFGEMAAKT (bewust, niet vergeten)
-----------------------------------
shortage_start per GTIN = "first seen" over de 85 historische obwieszczenia. Valkuil bij die
uitbreiding: 2019 drukt EAN-13 zonder voorloopnul af (5702157142200) en 2026 GTIN-14 mét
(05702157142200); vergelijk daarom op de laatste 13 cijfers, anders lijkt de overlap 0.
De kolomkop verschilt ook ("Kod EAN lub inny kod odpowiadający kodowi EAN" vs "Kod GTIN"),
vandaar dat de kolommapping hieronder op de kopregel gebeurt en niet op een vaste index.

ATC-code en vergunninghouder staan NIET in deze bron (nul treffers in de hele PDF).
Ze blijven leeg; niet bijverrijken zonder expliciete afspraak.

AANTAL: 401, NIET 399
---------------------
De bijlage van poz. 63/2026 telt 294 producten (Lp 1-294) en 401 verpakkingen. Wie de
verpakkingen telt met een regex op veertien aaneengesloten cijfers komt op 399 uit: twee
GTIN's staan in de tekstlaag mét spatie afgedrukt (05909990 008483 op p.3 en
05909991 205966 op p.4). Die twee zijn dus geen extra rijen maar herstelde rijen — vandaar
dat _gtin() eerst alle niet-cijfers verwijdert. Corrigeer 401 niet "terug" naar 399.
"""

from __future__ import annotations

import io
import re
from datetime import date, datetime
from urllib.parse import urljoin

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper

# Poolse maandnamen in de genitief, zoals ze in "na dzień 9 września 2026 r." staan.
_MAANDEN = {
    "stycznia": 1, "lutego": 2, "marca": 3, "kwietnia": 4, "maja": 5, "czerwca": 6,
    "lipca": 7, "sierpnia": 8, "września": 9, "października": 10,
    "listopada": 11, "grudnia": 12,
}
_PEILDATUM_RE = re.compile(r"na dzień\s+(\d{1,2})\s+([a-ząćęłńóśźż]+)\s+(\d{4})\s*r\.", re.I)


class PlMzScraper(BaseScraper):
    """Polen — Ministerstwo Zdrowia, lijst van producten bedreigd met onbeschikbaarheid."""

    # Keyword 55 = "Lista leków zagrożonych niedostepnoscią". Keyword 6 is het register en
    # is géén tekortenbron — niet omwisselen.
    INDEX_API = "https://dziennikmz.mz.gov.pl/api/keywords/55"
    INDEX_PAGE = "https://dziennikmz.mz.gov.pl/keywords/55"

    # De wykaz verschijnt ongeveer elke twee maanden. Wordt de nieuwste publicatie ouder dan
    # dit, dan is óf de bron gestopt óf we kijken naar de verkeerde ingang. Stil oude data
    # blijven leveren is hier de gevaarlijkste uitkomst (de GR-valkuil: vijf maanden oude
    # lijst die er vers uitzag), dus dat moet luid in de log staan.
    MAX_LEEFTIJD_DAGEN = 130

    def __init__(self) -> None:
        super().__init__(
            country_code="PL",
            country_name="Poland",
            source_name="MZ",
            base_url="https://dziennikmz.mz.gov.pl",
        )

    # ─── Index ophalen ──────────────────────────────────────────────────────

    def _list_publications(self) -> list[dict]:
        """Alle obwieszczenia uit de JSON-index, nieuwste eerst.

        Twee dingen die eerder fout gingen en niet zichtbaar faalden:
        - de response is een OBJECT, niet een lijst: de akten zitten onder "LegalActs";
        - de veldnamen zijn PascalCase (PublicationDate/PdfUrl), niet camelCase. Een
          camelCase-gok levert overal None op en dus een lege, "succesvolle" run.
        """
        resp = requests.get(self.INDEX_API, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        acts = data.get("LegalActs") if isinstance(data, dict) else None
        if not acts:
            raise RuntimeError(
                f"Geen 'LegalActs' in de JSON-index ({self.INDEX_API}); responsevorm gewijzigd?"
            )

        publicaties = []
        for item in acts:
            pdf_rel = item.get("PdfUrl")
            if not pdf_rel:
                continue
            publicaties.append({
                "publication_date": (item.get("PublicationDate") or "")[:10],
                # PdfUrl is relatief ("GetActPdf.ashx?..."); de basis is de site-root en
                # NIET /api/, anders krijg je een 404-HTML-pagina terug die pdfplumber
                # pas veel later laat struikelen.
                "pdf_url": urljoin(self.base_url + "/", pdf_rel),
                "title": (item.get("Title") or "").strip(),
                "year": item.get("Year"),
                "position": item.get("Position"),
            })

        # De API levert de akten ONGESORTEERD (2019 staat vooraan). Zonder deze sortering
        # scrape je zeven jaar oude data zonder dat er iets misgaat.
        publicaties.sort(key=lambda p: p["publication_date"], reverse=True)
        return publicaties

    # ─── PDF ophalen ────────────────────────────────────────────────────────

    def _download_pdf(self, url: str) -> bytes:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        if not resp.content.startswith(b"%PDF"):
            raise RuntimeError(f"Geen PDF terug van {url} (ct={resp.headers.get('content-type')})")
        return resp.content

    # ─── PDF parsen ─────────────────────────────────────────────────────────
    #
    # De bijlage is een echte lijnen-tabel, maar met twee addertjes:
    #
    # 1. De cellen zijn niet consequent getekend. De meeste rijen bestaan uit gevulde
    #    cel-rechthoeken; sommige rijen (bv. Absenor, Lp 5-6) hebben alleen dunne
    #    randlijntjes. Wie alleen naar de gevulde rechthoeken kijkt, mist die rijen —
    #    stil, en zonder foutmelding.
    # 2. pdfplumber.extract_tables() ziet per pagina een ANDER aantal kolommen (9 tot 19),
    #    doordat elke celrand uit twee tot drie evenwijdige lijntjes bestaat. Positioneel
    #    indexeren op die uitkomst geeft verschoven kolommen (Moc/Postać/Wielkość door
    #    elkaar) en dubbele Lp-nummers.
    #
    # Daarom bouwen we het raster zelf: kolomgrenzen uit de geclusterde verticale lijnen,
    # rijgrenzen per kolom uit alles wat horizontaal over die kolom heen loopt. De kolommen
    # "Wielkość opakowania" en "Kod GTIN" zijn binnen één Lp-rij verder onderverdeeld —
    # dat zijn de losse verpakkingen, en dat is precies de granulariteit die we leveren.

    _KOPMAP = [
        ("lp", ("lp.", "lp")),
        ("medicine_name", ("nazwa produktu",)),
        ("active_substance", ("nazwa międzynarodowa", "nazwa miedzynarodowa")),
        ("dosage_form", ("postać", "postac")),
        ("strength", ("moc",)),
        ("package_size", ("wielkość opakowania", "wielkosc opakowania")),
        ("product_no", ("kod gtin", "kod ean")),
    ]

    @staticmethod
    def _clusters(waarden, tol: float) -> list[list[float]]:
        groepen: list[list[float]] = []
        for v in sorted(waarden):
            if groepen and v - groepen[-1][-1] <= tol:
                groepen[-1].append(v)
            else:
                groepen.append([v])
        return groepen

    @staticmethod
    def _mediaan(groep: list[float]) -> float:
        g = sorted(groep)
        return g[len(g) // 2]

    @staticmethod
    def _tekst(woorden, x0: float, x1: float, top: float, bottom: float) -> str:
        """Alle woorden waarvan het middelpunt binnen deze cel valt, in leesvolgorde."""
        sel = [w for w in woorden
               if x0 <= (w["x0"] + w["x1"]) / 2 <= x1
               and top <= (w["top"] + w["bottom"]) / 2 <= bottom]
        sel.sort(key=lambda w: w["top"])
        regels: list[tuple[float, list]] = []
        for w in sel:
            if regels and w["top"] - regels[-1][0] <= 3:
                regels[-1][1].append(w)
            else:
                regels.append((w["top"], [w]))
        return " ".join(
            " ".join(x["text"] for x in sorted(groep, key=lambda w: w["x0"]))
            for _, groep in regels
        ).strip()

    def _parse_pdf(self, pdf_bytes: bytes) -> tuple[list[dict], str | None]:
        """Lees de bijlage uit tot rijen op VERPAKKINGSniveau (één regel per GTIN).

        Retourneert (rijen, peildatum_iso).
        """
        import pdfplumber  # pas hier importeren: alleen deze scraper heeft het nodig

        rijen: list[dict] = []
        lp_gezien: list[int] = []
        peildatum = None
        kolomnamen: list[str] | None = None
        kolomgrenzen: list[float] | None = None

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for paginanr, pagina in enumerate(pdf.pages, start=1):
                tekst = pagina.extract_text() or ""
                if peildatum is None:
                    peildatum = self._peildatum(tekst)

                v_x = [e["x0"] for e in pagina.edges if e["orientation"] == "v"]
                if not v_x:
                    continue  # titelpagina: geen tabel

                # Elke celrand bestaat uit 2-3 lijntjes binnen ~3,5 pt; met tol=8 vallen die
                # samen tot één grens. Zeven kolommen -> acht grenzen.
                grenzen = [self._mediaan(g) for g in self._clusters(v_x, tol=8)]
                if len(grenzen) != 8:
                    raise RuntimeError(
                        f"Pagina {paginanr}: {len(grenzen)} kolomgrenzen i.p.v. 8 "
                        f"({[round(g, 1) for g in grenzen]}); tabelindeling gewijzigd."
                    )
                kolomgrenzen = grenzen

                def rijgrenzen(kolom: int) -> list[float]:
                    """y-posities van alles wat horizontaal ÓVER kolom `kolom` heen loopt.

                    De inzet-rechthoekjes (~3,3 pt smaller aan elke kant) horen bij de
                    tekstregels, niet bij de celindeling; die vallen af doordat we eisen dat
                    een rechthoek de hele kolombreedte overspant.
                    """
                    links, rechts = grenzen[kolom], grenzen[kolom + 1]
                    ys: list[float] = []
                    for r in pagina.rects:
                        if r["x0"] <= links + 2 and r["x1"] >= rechts - 2:
                            if r["bottom"] - r["top"] <= 1.6:
                                ys.append((r["top"] + r["bottom"]) / 2)   # dun randlijntje
                            else:
                                ys.append(r["top"])                       # gevulde cel
                                ys.append(r["bottom"])
                    return [self._mediaan(g) for g in self._clusters(ys, tol=1.6)]

                def cellen(kolom: int) -> list[tuple[float, float]]:
                    g = rijgrenzen(kolom)
                    return [(g[i], g[i + 1]) for i in range(len(g) - 1) if g[i + 1] - g[i] > 3]

                woorden = pagina.extract_words()
                rij_cellen = cellen(0)
                pak_cellen = cellen(5)
                gtin_cellen = cellen(6)

                for top, bottom in rij_cellen:
                    # "110." komt als twee woorden ('11' + '0.') uit extract_words omdat er
                    # ruimte tussen de cijfers staat; daarom eerst alle witruimte weg.
                    lp_ruw = re.sub(r"\s+", "", self._tekst(woorden, grenzen[0], grenzen[1], top, bottom))

                    if lp_ruw.lower().startswith("lp"):
                        kolomnamen = self._lees_kopregel(woorden, grenzen, top, bottom, paginanr)
                        continue
                    if not re.fullmatch(r"\d+\.?", lp_ruw):
                        continue  # paginakop ("Dziennik Urzędowy ... Poz. 63") of losse regel
                    if kolomnamen is None:
                        raise RuntimeError("Productrij gevonden vóór de kopregel; kopregel niet herkend.")

                    lp = int(lp_ruw.rstrip("."))
                    lp_gezien.append(lp)

                    veld = {naam: self._tekst(woorden, grenzen[i], grenzen[i + 1], top, bottom)
                            for i, naam in enumerate(kolomnamen)}

                    binnen = lambda c: top - 1 <= (c[0] + c[1]) / 2 <= bottom + 1  # noqa: E731
                    subpak = [c for c in pak_cellen if binnen(c)] or [(top, bottom)]
                    subgtin = [c for c in gtin_cellen if binnen(c)] or [(top, bottom)]

                    # Eén verpakkingscel kan meerdere GTIN-subcellen overspannen (bv. als de
                    # verpakkingsgrootte "-" is). Koppel daarom op verticale overlap en niet
                    # op volgorde: anders schuift bij zo'n samengevoegde cel alles één op.
                    for gt, gb in subgtin:
                        beste = max(subpak, key=lambda p: min(p[1], gb) - max(p[0], gt))
                        rijen.append({
                            "lp": lp,
                            "page": paginanr,
                            "medicine_name": self._schoon(veld.get("medicine_name", "")),
                            "active_substance": self._schoon(veld.get("active_substance", "")),
                            "dosage_form": self._schoon(veld.get("dosage_form", "")),
                            "strength": self._schoon(veld.get("strength", "")),
                            "package_size": self._schoon(
                                self._tekst(woorden, grenzen[5], grenzen[6], beste[0], beste[1])),
                            "product_no": self._gtin(
                                self._tekst(woorden, grenzen[6], grenzen[7], gt, gb)),
                        })

            # Onafhankelijke hertelling langs een ANDER pad: tel de GTIN-regels rechtstreeks
            # uit de woordenstroom. Wijkt dat af van het aantal opgebouwde rijen, dan is er
            # een subcel weggevallen of dubbel geteld — dan liever stuk dan stilletjes 95%.
            if kolomgrenzen is None:
                raise RuntimeError("Geen tabelpagina's in de PDF gevonden")
            verwacht = self._tel_gtin_regels(pdf, kolomgrenzen)

        self._valideer(rijen, lp_gezien, verwacht)
        return rijen, peildatum

    def _lees_kopregel(self, woorden, grenzen, top, bottom, paginanr) -> list[str]:
        """Koppel de zeven kolommen aan veldnamen op basis van de KOPTEKST.

        Bewust niet op vaste index: oudere obwieszczenia noemen de laatste kolom
        "Kod EAN lub inny kod odpowiadający kodowi EAN" i.p.v. "Kod GTIN", en een
        stille kolomverschuiving is hier eerder opgetreden dan een harde fout.
        """
        namen: list[str] = []
        for i in range(7):
            kop = self._tekst(woorden, grenzen[i], grenzen[i + 1], top, bottom).lower()
            kop = " ".join(kop.split())
            treffer = next((veld for veld, sleutels in self._KOPMAP
                            if any(s in kop for s in sleutels)), None)
            if treffer is None:
                raise RuntimeError(f"Pagina {paginanr}: onbekende kolomkop {kop!r}")
            namen.append(treffer)
        ontbreekt = {veld for veld, _ in self._KOPMAP} - set(namen)
        if ontbreekt or len(set(namen)) != 7:
            raise RuntimeError(f"Kopregel niet eenduidig: {namen} (mist {ontbreekt})")
        return namen

    @staticmethod
    def _tel_gtin_regels(pdf, grenzen) -> int:
        """Tel de GTIN-regels los van de celindeling: elke verpakking heeft er precies één."""
        totaal = 0
        for pagina in pdf.pages:
            sel = [w for w in pagina.extract_words()
                   if grenzen[6] <= (w["x0"] + w["x1"]) / 2 <= grenzen[7] and w["top"] > 60]
            sel.sort(key=lambda w: w["top"])
            regels: list[tuple[float, list[str]]] = []
            for w in sel:
                if regels and w["top"] - regels[-1][0] <= 3:
                    regels[-1][1].append(w["text"])
                else:
                    regels.append((w["top"], [w["text"]]))
            # De GTIN staat soms met een spatie afgedrukt ("05909990 008483"); plakken.
            totaal += sum(1 for _, delen in regels if re.fullmatch(r"\d{13,14}", "".join(delen)))
        return totaal

    @staticmethod
    def _schoon(waarde: str) -> str:
        """'-' is de manier van de bron om 'niet van toepassing' te zeggen -> leeg veld."""
        w = " ".join(waarde.split())
        return "" if w in ("-", "–", "—") else w

    @staticmethod
    def _gtin(waarde: str) -> str:
        """Normaliseer naar GTIN-14. EAN-13 uit oudere lijsten krijgt een voorloopnul."""
        cijfers = re.sub(r"\D", "", waarde)
        if len(cijfers) == 13:
            return "0" + cijfers
        if len(cijfers) == 14:
            return cijfers
        raise RuntimeError(f"Onverwachte GTIN {waarde!r} ({len(cijfers)} cijfers)")

    @staticmethod
    def _peildatum(tekst: str) -> str | None:
        """'na dzień 9 września 2026 r.' -> '2026-09-09'."""
        m = _PEILDATUM_RE.search(" ".join(tekst.split()))
        if not m:
            return None
        maand = _MAANDEN.get(m.group(2).lower())
        if not maand:
            return None
        return f"{int(m.group(3)):04d}-{maand:02d}-{int(m.group(1)):02d}"

    @staticmethod
    def _valideer(rijen: list[dict], lp_gezien: list[int], verwacht_gtin: int) -> None:
        """Alles-of-niets. Een half ingelezen bijlage is schadelijker dan een foutmelding."""
        if not rijen:
            raise RuntimeError("Geen productrijen uit de bijlage gehaald")

        uniek = set(lp_gezien)
        if len(uniek) != len(lp_gezien):
            dubbel = sorted({l for l in uniek if lp_gezien.count(l) > 1})
            raise RuntimeError(f"Dubbele Lp-nummers: {dubbel[:20]}")
        if max(uniek) != len(uniek):
            ontbreekt = sorted(set(range(1, max(uniek) + 1)) - uniek)
            raise RuntimeError(
                f"Lp loopt tot {max(uniek)} maar er zijn {len(uniek)} rijen; ontbreekt: {ontbreekt[:20]}"
            )

        if len(rijen) != verwacht_gtin:
            raise RuntimeError(
                f"{len(rijen)} verpakkingsrijen opgebouwd, maar {verwacht_gtin} GTIN-regels "
                f"in de PDF geteld — er is een subcel weggevallen of dubbel geteld."
            )

        codes = [r["product_no"] for r in rijen]
        if len(set(codes)) != len(codes):
            dubbel = sorted({c for c in codes if codes.count(c) > 1})
            print(f"  LET OP: {len(codes) - len(set(codes))} dubbele GTIN's in de bijlage: {dubbel[:5]}")

    # ─── Main ───────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        publicaties = self._list_publications()
        print(f"  {len(publicaties)} obwieszczenia in de index")

        nieuwste = publicaties[0]
        pub = nieuwste["publication_date"]
        print(f"  nieuwste publicatie: {pub} — poz. {nieuwste['position']}/{nieuwste['year']}")

        leeftijd = (date.today() - date.fromisoformat(pub)).days if pub else None
        if leeftijd is not None and leeftijd > self.MAX_LEEFTIJD_DAGEN:
            print(f"  WAARSCHUWING: nieuwste wykaz is {leeftijd} dagen oud (cadans ~2 maanden). "
                  f"Bron mogelijk gestopt of verplaatst — data is dan verschaald.")

        pdf_bytes = self._download_pdf(nieuwste["pdf_url"])
        rijen, peildatum = self._parse_pdf(pdf_bytes)

        if peildatum:
            print(f"  peildatum bijlage: {peildatum}")
        else:
            print("  LET OP: peildatum niet uit de bijlagekop te lezen")

        bron = f"Dz. Urz. Min. Zdrow. z {nieuwste['year']} r. poz. {nieuwste['position']}"
        reden = ("Wykaz produktów zagrożonych brakiem dostępności (art. 37av Prawo farmaceutyczne)"
                 + (f"; stan na {peildatum}" if peildatum else "")
                 + f"; {bron}")
        nu = datetime.now().isoformat()

        records = [{
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": r["medicine_name"],
            "active_substance": r["active_substance"],
            "dosage_form": r["dosage_form"],
            "strength": r["strength"],
            "package_size": r["package_size"],
            "product_no": r["product_no"],
            "atc_code": "",              # staat niet in de bron
            "marketing_auth_holder": "",  # staat niet in de bron
            "shortage_start": "",         # bron kent geen datum per product — zie module-docstring
            "estimated_end": "",
            "status": "at_risk_of_shortage",
            "reason": reden,
            "last_updated": pub,          # publicatiedatum van het obwieszczenie
            "reference_date": peildatum or "",
            "source_url": nieuwste["pdf_url"],
            "list_position": f"{nieuwste['position']}/{nieuwste['year']}",
            "lp": r["lp"],
            "scraped_at": nu,
        } for r in rijen]

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} verpakkingsrijen / {df['lp'].nunique()} producten "
              f"/ {df['product_no'].nunique()} unieke GTIN's")
        print("  NB: preventieve wykaz (bedreigd met onbeschikbaarheid), geen gemeld tekort — "
              "status=at_risk_of_shortage")
        return df
