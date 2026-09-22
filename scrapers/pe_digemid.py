"""Peru / DIGEMID - beschikbaarheid van geneesmiddelen in de publieke zorg (SISMED).

WAAROM DEZE SCRAPER ER ZO UITZIET
---------------------------------
De vorige versie leverde 17 "tekorten" die geen geneesmiddelen waren maar NIEUWSKOPPEN
(COMUNICADO N 013-2026, ALERTA DIGEMID Nr 109-2026, Resolucion Ministerial ...), met de
publicatiedatum van het nieuwsbericht als shortage_start. Oorzaak: alle main.asp-URLs zijn
404 en DIGEMID's WordPress negeert `?q=...` volledig, dus `?q=desabastecimiento` levert
gewoon de HOMEPAGE. Die homepage haalde de trefwoordcontrole omdat er toevallig een
nieuwsbericht met het woord "desabastecimiento" op stond, waarna de div-fallback de
nieuwsblokken als tekorten wegschreef. Les: een trefwoordcontrole op paginatekst is GEEN
bewijs dat je op een tekortenpagina staat. Deze scraper controleert daarom op STRUCTUUR
(bestaat het tabblad DESABASTECIDO? staat er een kolom codigo_med? klopt mesano?) en nooit
meer op losse woorden.

WAT DEZE BRON WEL IS - EN WAAROM PE NIET VANZELF OP DE KAART KOMT
-----------------------------------------------------------------
De werkende bron is een DERDE soort bron, naast "tekortmeldingenregister" (wat de meeste
landen leveren) en "registratieregister" (waarop LT/TR/EE eerder sneuvelden):

    "Disponibilidad de Productos Farmaceuticos por Disas/Diresas/Geresas (analisis a 12
     meses) - incluye Almacenes Especializados y Farmacias Institucionales"

Dat is SISMED-logistiekdata van de publieke sector: stockstatus per ZORGINRICHTING x
PRODUCT. Het zijn echte voorraadtekorten, maar:
  - op inrichtingsniveau, niet landelijk;
  - zonder melder, zonder startdatum, zonder verwachte einddatum;
  - alleen publieke sector (MINSA), dus niet de private apotheken/klinieken.

Dat is een ander soort signaal dan de andere 30+ landen leveren. Net als bij de
registerkwestie (validatie Jesper 25-08) hoort daar een expliciete JA of NEE op te komen
van Jesper/Nicky VOORDAT PE op de kaart verschijnt. Daarom staat er een bewuste rem op:

    zonder de omgevingsvariabele PE_SISMED_AKKOORD=1 geeft deze scraper een LEGE
    DataFrame terug en legt hij in de log uit waarom.

Dat is geen storing, dat is het besluit dat nog niet genomen is. Zet de vlag pas aan als
de projectbeslissing genomen IS; dan levert de scraper de volledige landelijke aggregatie.

LET OP voor wie PE live zet: landkaart/build_data.py leest ALLE output/*_shortage_*.csv en
kent geen PE-uitzondering. Zodra hier een gevulde CSV staat en de ATC-verrijking eroverheen
is gegaan, staat PE dus op de kaart. Er is geen tweede rem.

Om Jesper/Nicky te laten beoordelen WAT ze ja of nee zeggen tegen, staat het resultaat van
de augustus-editie klaar als output/PE_DIGEMID_TERBEOORDELING_sismed_2026-08.csv. Die
bestandsnaam bevat bewust geen "_shortage_", zodat build_data.py hem niet oppikt.

De opgehaalde regio's worden gecached in cache/pe_digemid/<JJJJ-MM>/<REGIO>.json. Dat mag,
want de bron is een MAANDpublicatie: binnen een maand verandert het bestand niet. De cache
is er om twee redenen: een volledige koude run duurt ~170s en past dus niet in de 150s
SIGALRM van _rerun_targeted.py, en een afgebroken run hoeft niet overnieuw. Nieuwe maand =
nieuwe cachemap, dus verouderen kan niet.

TECHNIEK: WAAROM HTTP RANGE EN NIET GEWOON DOWNLOADEN
-----------------------------------------------------
Per maand staan er 33 XLSX-werkboeken (een per DISA/DIRESA/GERESA) van samen ~1,2 GB;
Lima Metropolitana alleen al is 137 MB. _rerun_targeted.py hanteert 150s SIGALRM per
scraper, dus alles downloaden kan niet, en een steekproef van regio's mag niet: dan levert
PE stilletjes een fractie, precies het Denemarken-scenario.

XLSX is een ZIP en de server ondersteunt Range (206 bevestigd). We lezen daarom alleen de
onderdelen die we nodig hebben: het staartstuk (EOCD + central directory), workbook.xml,
workbook.xml.rels, sharedStrings.xml en het ENE werkblad DESABASTECIDO. Dat is ~9,8 MB van
Lima's 137 MB (7%) en ~1,2 MB van Tumbes' 14,7 MB (8%) - samen ~80-100 MB in plaats van
1,2 GB.

Het werkblad wordt opgezocht via workbook.xml + rels op NAAM. Nooit op volgorde en nooit op
regionaam: in Lima heet het regiotabblad met typefout "LIMA_METRPOLITANA_2026_08", en de
sheetId-attributen liegen (DESABASTECIDO heeft sheetId="2" maar rId3 -> sheet3.xml, terwijl
sheet2.xml het 12 MB grote regiotabblad is). Wie op volgorde of sheetId gokt, trekt het
verkeerde - en vooral veel te grote - werkblad binnen.
"""

import io
import os
import re
import time
import unicodedata
import zipfile
import calendar
import json
import xml.etree.ElementTree as ET
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# Kolomletter vooraan een celverwijzing ("AB12" -> "AB").
_COLLETTER = re.compile(r"[A-Z]+")


def _plat(s) -> str:
    """Accenten en hoofdletters weg, zodat 'PRODUCTO BIOLOGICO' en 'PRODUCTO BIOLÓGICO'
    dezelfde sleutel worden. De bron is niet consequent in accenten en trailing spaties."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c)).upper().strip()


class _RangeReader(io.RawIOBase):
    """Bestandsachtig object dat via HTTP Range leest, zodat zipfile in een XLSX van
    137 MB kan bladeren zonder hem te downloaden.

    Twee dingen waar een volgende lezer over struikelt:
      * De eerste aanvraag is een SUFFIX-range ("bytes=-98304"). Zo hebben we in een klap
        het ZIP-staartstuk EN de bestandsgrootte (uit Content-Range), en is een aparte
        HEAD-aanvraag niet nodig. Elke bespaarde aanvraag telt: een koude aanvraag kost
        ~2s round-trip, ongeacht hoeveel bytes je vraagt.
      * Alles wat binnenkomt blijft in self.cache staan. zipfile leest in kleine hapjes
        (headers, dan data); zonder cache zou elk hapje een eigen HTTP-aanvraag worden en
        loopt het aantal aanvragen - en dus de 2s round-trip - volledig uit de hand.
    """

    def __init__(self, url, session, headers, tail=96 * 1024, throttle=0.6, deadline=None):
        self.url = url
        self.s = session
        self.h = headers
        self.pos = 0
        self.throttle = throttle
        self.deadline = deadline
        self.nreq = 0
        self.nbytes = 0
        self._last = 0.0
        self.cache: dict[tuple[int, int], bytes] = {}
        self.size = 0
        self._suffix(tail)

    # -- throttle -----------------------------------------------------------
    def _wait(self):
        """Aanvragen uit elkaar houden. Een burst parallelle aanvragen levert ~1 minuut
        Cloudflare-403 op; 20 sequentiele aanvragen met 0,25s ertussen bleven wel 206.
        0,6s is de gekozen marge tussen 'veilig' en 'past binnen de 150s time-out'."""
        dt = time.time() - self._last
        if dt < self.throttle:
            time.sleep(self.throttle - dt)
        self._last = time.time()

    def _get(self, rangehdr: str) -> requests.Response:
        if self.deadline and time.time() > self.deadline:
            raise TimeoutError("tijdbudget op")
        self._wait()
        r = self.s.get(self.url, headers=dict(self.h, Range=rangehdr), timeout=120)
        if r.status_code == 403:
            # Cloudflare heeft ons eruit gegooid. Niet stilletjes doorgaan met minder
            # regio's; dat is precies de stille onderlevering die we willen voorkomen.
            raise RuntimeError("HTTP 403 (Cloudflare) - te snel gevraagd")
        if r.status_code != 206:
            raise RuntimeError(f"verwachtte 206 Partial Content, kreeg {r.status_code}")
        self.nreq += 1
        self.nbytes += len(r.content)
        return r

    def _suffix(self, n: int):
        r = self._get(f"bytes=-{n}")
        cr = r.headers.get("Content-Range", "")          # bytes 14596064-14692455/14692456
        m = re.match(r"bytes (\d+)-(\d+)/(\d+)", cr)
        if not m:
            raise RuntimeError(f"onbruikbare Content-Range: {cr!r}")
        start, _, total = (int(x) for x in m.groups())
        self.size = total
        self.cache[(start, self.size - 1)] = r.content

    def prefetch(self, start: int, end: int):
        """Haal een heel ZIP-onderdeel in EEN aanvraag op. Aanroepen voordat zipfile het
        onderdeel gaat lezen; anders knipt zipfile het in tientallen losse aanvragen."""
        start = max(0, start)
        end = min(end, self.size - 1)
        if start > end or self._lookup(start, end - start + 1) is not None:
            return
        self.cache[(start, end)] = self._get(f"bytes={start}-{end}").content

    def _lookup(self, start: int, n: int):
        for (a, b), buf in self.cache.items():
            if a <= start and start + n - 1 <= b:
                return buf[start - a: start - a + n]
        return None

    # -- file protocol ------------------------------------------------------
    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else (self.pos + off if whence == 1 else self.size + off)
        return self.pos

    def tell(self):
        return self.pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self.pos
        n = min(n, self.size - self.pos)
        if n <= 0:
            return b""
        got = self._lookup(self.pos, n)
        if got is None:
            self.prefetch(self.pos, self.pos + max(n, 262144) - 1)
            got = self._lookup(self.pos, n)
        self.pos += n
        return got

    def readinto(self, b):
        d = self.read(len(b))
        b[:len(d)] = d
        return len(d)


class PeDigemidScraper(BaseScraper):
    """DIGEMID (Peru): maandelijkse SISMED-beschikbaarheidsrapportage per DISA/DIRESA/GERESA.

    Levert per uniek geneesmiddel (codigo_med) een landelijke regel met als ernstmaat het
    aantal getroffen zorginrichtingen en regio's. Dat aantal IS de waarde van deze bron:
    "desabastecido in 412 van de 8.000 inrichtingen" zegt iets, een kale ja/nee niet.
    """

    BASE = "https://www.digemid.minsa.gob.pe"
    INDEX = (
        "https://www.digemid.minsa.gob.pe/webDigemid/publicaciones/"
        "disponibilidad-de-productos-farmaceuticos/"
    )

    # Volledige, realistische Chrome-UA. Een korte of ontbrekende UA levert 403 bij
    # Cloudflare. Een Referer is hier NIET nodig (wel bij CH; daar was dat juist de fix).
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    MAANDEN = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,  # Peruaanse variant
        "octubre": 10, "noviembre": 11, "diciembre": 12,
    }

    # WELKE RIJEN ZIJN EEN GENEESMIDDELTEKORT (de Estland-valkuil)
    # ------------------------------------------------------------
    # Het tabblad DESABASTECIDO bevat alles wat een inrichting inkoopt. In Tumbes:
    #   tipomed I (insumo): INSUMOS 611, REACTIVOS Y MATERIAL DE LABORATORIO 250,
    #                       POR DEFINIR 231, INSTRUMENTAL MEDICO 85, ...  -> 1.258 rijen
    #   tipomed M         : MEDICAMENTO 503, PRODUCTO BIOLOGICO 170, PRODUCTO GALENICO 22,
    #                       PRODUCTO DIETETICO 23, ANTISEPTICO Y DESINFECTANTE 5 -> 723
    # Filteren op tipomed=='M' alleen is dus NIET genoeg: dan komen voedingssupplementen
    # (PRODUCTO DIETETICO) en ontsmettingsmiddelen er als "geneesmiddeltekort" bij. We
    # eisen daarom tipomed=='M' EN een subtype uit onderstaande lijst. Zonder filter
    # belanden halskragen, verband en labreagentia op de kaart.
    SUBTYPES_JA = {
        "MEDICAMENTO", "MEDICAMENTOS", "MEDICAMENTOS QUIMICOS",
        "PRODUCTO BIOLOGICO", "PRODUCTO GALENICO", "GALENICOS",
    }

    # Parentheses die de FARMACEUTISCHE VORM beschrijven horen niet in de stofnaam:
    # "OMEPRAZOL (TABLETA DE LIBERACION MODIFICADA)" -> stof is OMEPRAZOL.
    # Parentheses die bij de stof horen blijven staan:
    # "HIDROCORTISONA (COMO ACETATO)", "ALCOHOL ETILICO (ETANOL)", "... (PENTAVALENTE)".
    # Onderscheid op het EERSTE woord binnen de haakjes; deze basislijst wordt tijdens de
    # run aangevuld met alle formaf-waarden die we in de bron zien.
    VORMWOORDEN = {
        "TABLETA", "TABLETAS", "CAPSULA", "CAPSULAS", "SUSPENSION", "SOLUCION",
        "JARABE", "CREMA", "UNGUENTO", "GEL", "POLVO", "GRANULADO", "EMULSION",
        "AEROSOL", "INYECTABLE", "GOTAS", "OVULO", "OVULOS", "SUPOSITORIO",
        "SUPOSITORIOS", "PARCHE", "COLIRIO", "PASTA", "LOCION", "SPRAY", "ESPUMA",
        "IMPLANTE", "COMPRIMIDO", "GRAGEA", "ELIXIR", "LINIMENTO", "JALEA",
        "CONCENTRADO", "DISPERSION", "LIOFILIZADO",
    }

    def __init__(self):
        super().__init__(
            country_code="PE",
            country_name="Peru",
            source_name="DIGEMID",
            base_url="https://www.digemid.minsa.gob.pe",
        )
        self.s = requests.Session()
        self._formafs: set[str] = set()

    # ==================================================================
    # Stap 1: nieuwste editie dynamisch opzoeken
    # ==================================================================

    def _nieuwste_editie(self) -> tuple[str, int, int]:
        """Geeft (url van de editiepagina, jaar, maand) van de NIEUWSTE publicatie.

        Nooit de scrapedatum gebruiken om een pad te raden (/2026/09/ bestaat nog niet als
        september nog niet gepubliceerd is) en nooit een vast pad hardcoderen: dan haalt de
        scraper over drie maanden nog steeds augustus op zonder dat iemand het merkt. Dat
        is de Griekenland-valkuil: een link volgen van een pagina die al maanden stilstaat.
        """
        r = self.s.get(self.INDEX, headers=self.HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        pat = re.compile(
            r"/disponibilidad-de-productos-farmaceuticos-([a-zñáéíóúü]+)-(\d{4})/?$",
            re.I,
        )
        edities: list[tuple[int, int, str]] = []
        for a in soup.find_all("a", href=True):
            m = pat.search(a["href"].lower())
            if not m:
                continue
            maand = self.MAANDEN.get(m.group(1))
            if maand:
                edities.append((int(m.group(2)), maand, urljoin(self.INDEX, a["href"])))
        if not edities:
            raise RuntimeError("geen editielinks gevonden op de publicaties-index")

        jaar, maand, url = max(edities)
        print(f"    nieuwste editie: {jaar}-{maand:02d}  ({len(edities)} edities op pagina 1)")
        return url, jaar, maand

    def _regiobestanden(self, editie_url: str, jaar: int, maand: int) -> list[tuple[str, str]]:
        """Alle XLSX-links van de editiepagina als [(REGIO, url), ...]."""
        r = self.s.get(editie_url, headers=self.HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        uit: dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".xlsx"):
                continue
            naam = href.rsplit("/", 1)[-1]
            m = re.match(r"(.+)_(\d{4})_(\d{2})\.xlsx$", naam, re.I)
            if not m:
                continue
            # Kruiscontrole: het jaar/maand IN het bestandspad moet de editie zijn waarvan
            # we denken dat we hem ophalen. Wijkt dat af, dan linkt de editiepagina naar
            # oude bestanden en zouden we ongemerkt verouderde data leveren.
            if (int(m.group(2)), int(m.group(3))) != (jaar, maand):
                print(f"    LET OP: {naam} hoort niet bij {jaar}-{maand:02d}, overgeslagen")
                continue
            uit[m.group(1).upper()] = urljoin(editie_url, href)
        return sorted(uit.items())

    # ==================================================================
    # Stap 2: XLSX-onderdelen via Range lezen
    # ==================================================================

    def _member(self, zf: zipfile.ZipFile, rr: _RangeReader, naam: str) -> bytes:
        i = zf.getinfo(naam)
        rr.prefetch(i.header_offset, i.header_offset + i.compress_size + 4096)
        return zf.read(naam)

    def _tabbladen(self, zf: zipfile.ZipFile, rr: _RangeReader) -> dict[str, str]:
        """{TABBLADNAAM: 'xl/worksheets/sheetN.xml'} via workbook.xml + rels.

        Via de relatie-id, niet via sheetId en niet via volgorde - zie de uitleg in de
        moduledocstring over Lima."""
        wb = ET.fromstring(self._member(zf, rr, "xl/workbook.xml"))
        rels = ET.fromstring(self._member(zf, rr, "xl/_rels/workbook.xml.rels"))
        rmap = {rel.get("Id"): rel.get("Target") for rel in rels}
        uit = {}
        for sh in wb.iter(NS + "sheet"):
            rid = next((v for k, v in sh.attrib.items() if k.endswith("}id")), None)
            doel = (rmap.get(rid) or "").lstrip("/")
            if not doel:
                continue
            if not doel.startswith("xl/"):
                doel = "xl/" + doel
            uit[_plat(sh.get("name"))] = doel
        return uit

    def _sharedstrings(self, zf: zipfile.ZipFile, rr: _RangeReader) -> list[str]:
        if "xl/sharedStrings.xml" not in zf.namelist():
            return []
        i = zf.getinfo("xl/sharedStrings.xml")
        rr.prefetch(i.header_offset, i.header_offset + i.compress_size + 4096)
        uit = []
        with zf.open("xl/sharedStrings.xml") as fh:
            for _, el in ET.iterparse(fh, events=("end",)):
                if el.tag == NS + "si":
                    uit.append("".join(t.text or "" for t in el.iter(NS + "t")))
                    el.clear()
        return uit

    def _rijen(self, zf, rr, pad: str, shared: list[str]):
        """Streamt een werkblad als {kolomletter: tekst}. Streamen, niet inlezen: het
        uitgepakte DESABASTECIDO-blad van Lima is tientallen MB's aan XML."""
        i = zf.getinfo(pad)
        rr.prefetch(i.header_offset, i.header_offset + i.compress_size + 4096)
        with zf.open(pad) as fh:
            for _, el in ET.iterparse(fh, events=("end",)):
                if el.tag != NS + "row":
                    continue
                cellen = {}
                for c in el:
                    m = _COLLETTER.match(c.get("r") or "")
                    if not m:
                        continue
                    t = c.get("t")
                    if t == "inlineStr":
                        isel = c.find(NS + "is")
                        txt = "".join(x.text or "" for x in isel.iter(NS + "t")) if isel is not None else ""
                    else:
                        v = c.find(NS + "v")
                        if v is None:
                            continue
                        if t == "s":
                            idx = int(v.text)
                            txt = shared[idx] if 0 <= idx < len(shared) else ""
                        else:
                            txt = v.text or ""
                    cellen[m.group(0)] = txt
                yield cellen
                el.clear()

    # ==================================================================
    # Stap 3: een regio verwerken
    # ==================================================================

    def _regio(self, regio: str, url: str, deadline: float | None) -> dict:
        """Leest een regiowerkboek en geeft de aggregatie per codigo_med terug.

        Teruggegeven: {'per_med': {codigo_med: {...}}, 'rijen': n, 'resumen': n, 'mesano': str}
        """
        rr = _RangeReader(url, self.s, self.HEADERS, deadline=deadline)
        zf = zipfile.ZipFile(rr)
        tab = self._tabbladen(zf, rr)
        if "DESABASTECIDO" not in tab:
            raise RuntimeError(f"{regio}: geen tabblad DESABASTECIDO (wel: {sorted(tab)})")
        shared = self._sharedstrings(zf, rr)

        # --- controlegetal uit RESUMEN -------------------------------------
        # RESUMEN telt per inrichting hoeveel producten desabastecido zijn. De som daarvan
        # hoort ongeveer gelijk te zijn aan het aantal rijen op het DESABASTECIDO-tabblad.
        # Loopt dat ver uiteen, dan lezen we het verkeerde blad of missen we rijen.
        resumen = 0
        if "RESUMEN" in tab:
            kop = None
            for cellen in self._rijen(zf, rr, tab["RESUMEN"], shared):
                waarden = {k: _plat(v) for k, v in cellen.items()}
                if kop is None:
                    if "DESABASTECIDO" in waarden.values() and "TOTAL" in waarden.values():
                        kop = {v: k for k, v in waarden.items()}
                    continue
                try:
                    resumen += int(float(cellen.get(kop["DESABASTECIDO"], "") or 0))
                except (TypeError, ValueError):
                    pass

        # --- DESABASTECIDO --------------------------------------------------
        kop = None
        rijen = 0
        mesano = ""
        gezien: set[tuple[str, str]] = set()   # (codigo_pre, codigo_med)
        per_med: dict[str, dict] = {}

        for cellen in self._rijen(zf, rr, tab["DESABASTECIDO"], shared):
            if kop is None:
                laag = {_plat(v).lower(): k for k, v in cellen.items()}
                if "codigo_med" in laag and "nombre_med" in laag and "tipomed" in laag:
                    kop = laag
                continue

            def g(veld: str) -> str:
                return (cellen.get(kop.get(veld, "\x00"), "") or "").strip()

            codigo_med = g("codigo_med")
            if not codigo_med:
                continue
            rijen += 1
            if not mesano:
                mesano = g("mesano")

            if g("tipomed").upper() != "M":
                continue
            if _plat(g("nomsubtipo")) not in self.SUBTYPES_JA:
                continue

            codigo_pre = g("codigo_pre")
            sleutel = (codigo_pre, codigo_med)
            if sleutel in gezien:
                continue          # dezelfde inrichting x product mag maar een keer tellen
            gezien.add(sleutel)

            rec = per_med.get(codigo_med)
            if rec is None:
                formaf = g("formaf")
                if formaf:
                    self._formafs.add(_plat(formaf))
                rec = per_med[codigo_med] = {
                    "nombre_med": re.sub(r"\s+", " ", g("nombre_med")).strip(),
                    "formaf": formaf,
                    "petitorio": g("petitorio"),
                    "n_pre": 0,
                }
            rec["n_pre"] += 1

        return {"per_med": per_med, "rijen": rijen, "resumen": resumen, "mesano": mesano}

    # ==================================================================
    # Stap 4: nombre_med uit elkaar halen
    # ==================================================================

    def _splits_naam(self, nombre: str, formaf: str) -> tuple[str, str]:
        """(active_substance, strength) uit nombre_med.

        nombre_med is INN-stijl en volgt het patroon
            <STOF>[ (vorm)] <sterkte>[ <verpakking>] <VORM>
        bijvoorbeeld
            "TETRACICLINA CLORHIDRATO (UNGUENTO OFTALMICO) 1 g/100 g (1 %) 6 g UNGUENTO"
            "HIDROCORTISONA (COMO ACETATO) 1 g/100 g (1 %) 20 g CREMA"
            "ETAMBUTOL + ISONIAZIDA + PIRAZINAMIDA + RIFAMPICINA 275 mg + 75 mg + ... TABLETA"

        De sterkte en de verpakkingsgrootte zijn in de bron NIET betrouwbaar te scheiden
        (soms "20 mg  TABLETA" zonder verpakking, soms "1 g/100 g (1 %) 6 g UNGUENTO" met).
        Daarom blijft alles wat sterkte-achtig is in `strength` staan en blijft
        `package_size` LEEG. Liever een veld dat iets te veel bevat dan een verzonnen
        splitsing die er precies uitziet.
        """
        s = re.sub(r"\s+", " ", nombre or "").strip()
        if not s:
            return "", ""

        # 1. Achterliggende vormaanduiding eraf (accentloos vergeleken: nombre_med schrijft
        #    "UNGÜENTO" terwijl formaf "UNGUENTO" zegt).
        vormen = self._formafs | self.VORMWOORDEN
        plat = _plat(s)
        for kand in sorted(vormen, key=len, reverse=True):
            if kand and plat.endswith(" " + kand):
                s = s[: len(s) - len(kand)].rstrip()
                break

        # 2. Stof = alles tot het eerste woord dat met een CIJFER begint, buiten haakjes.
        #    "buiten haakjes" is nodig voor "(ANTIGENO PITMAN MOORE CEPA 3218-VERO)";
        #    "begint met een cijfer" is nodig zodat "VITAMINA B12" niet bij de B wordt
        #    afgeknipt.
        diepte = 0
        knip = len(s)
        woordstart = True
        for i, ch in enumerate(s):
            if ch == "(":
                diepte += 1
            elif ch == ")":
                diepte = max(0, diepte - 1)
            if diepte == 0 and woordstart and ch.isdigit():
                knip = i
                break
            woordstart = ch == " "

        stof = s[:knip].strip(" ,")
        sterkte = s[knip:].strip(" ,")

        # 3. Vorm-haakjes uit de stofnaam halen, stof-haakjes laten staan.
        def _schoon(m: re.Match) -> str:
            binnen = m.group(1).strip()
            eerste = _plat(binnen.split()[0]) if binnen.split() else ""
            return "" if eerste in vormen else m.group(0)

        stof = re.sub(r"\(([^()]*)\)", _schoon, stof)
        stof = re.sub(r"\s+", " ", stof).strip(" ,-")
        return stof, sterkte

    # ==================================================================
    # Stap 5: scrape
    # ==================================================================

    KOLOMMEN = [
        "country_code", "country_name", "source", "medicine_name", "active_substance",
        "strength", "package_size", "product_no", "atc_code", "marketing_auth_holder",
        "shortage_start", "estimated_end", "status", "dosage_form", "reason",
        "last_updated", "affected_establishments", "affected_regions",
        "source_url", "scraped_at",
    ]

    def _cache_dir(self, jaar: int, maand: int) -> Path:
        p = Path(__file__).resolve().parent.parent / "cache" / "pe_digemid" / f"{jaar}-{maand:02d}"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        leeg = pd.DataFrame(columns=self.KOLOMMEN)

        # ---- projectbesluit ------------------------------------------------
        if os.environ.get("PE_SISMED_AKKOORD", "").strip() not in ("1", "ja", "true", "yes"):
            print("  PE staat BEWUST uit. De werkende DIGEMID-bron is geen tekortmeldingen-")
            print("  register maar SISMED-logistiekdata (voorraadstatus per zorginrichting,")
            print("  publieke sector, zonder melder/startdatum/einddatum). Dat is een ander")
            print("  soort signaal dan de andere landen leveren en vraagt eerst een expliciet")
            print("  ja of nee van Jesper/Nicky, net als bij de registerkwestie (25-08).")
            print("  Besluit genomen? Zet PE_SISMED_AKKOORD=1 en draai opnieuw.")
            print("  Lege DataFrame teruggegeven - bewust, niet door een storing.")
            return leeg

        budget = float(os.environ.get("PE_SISMED_BUDGET", "132"))
        deadline = time.time() + budget

        editie_url, jaar, maand = self._nieuwste_editie()
        bestanden = self._regiobestanden(editie_url, jaar, maand)
        print(f"    {len(bestanden)} regiobestanden gevonden voor {jaar}-{maand:02d}")
        if not bestanden:
            print("  GEEN regiobestanden - editiepagina veranderd? Lege DataFrame.")
            return leeg

        cache = self._cache_dir(jaar, maand)
        totaal: dict[str, dict] = {}
        controle: list[tuple[str, int, int]] = []
        mislukt: list[str] = []
        opgehaald = 0

        for regio, url in bestanden:
            cf = cache / f"{regio}.json"
            data = None
            if cf.exists():
                try:
                    data = json.loads(cf.read_text(encoding="utf-8"))
                except Exception:
                    data = None
            if data is None:
                if time.time() > deadline:
                    mislukt.append(f"{regio} (tijdbudget op)")
                    continue
                try:
                    data = self._regio(regio, url, deadline)
                    cf.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                except Exception as e:
                    mislukt.append(f"{regio} ({str(e)[:60]})")
                    continue
                opgehaald += 1

            # Griekenland-controle, maar dan op de data zelf: de maand die IN het werkboek
            # staat moet de editie zijn. mesano is de waarheid, de scrapedatum nooit.
            verwacht = f"{jaar}{maand:02d}"
            if data.get("mesano") and data["mesano"] != verwacht:
                print(f"    LET OP {regio}: mesano={data['mesano']} maar editie={verwacht}")

            controle.append((regio, data["rijen"], data["resumen"]))
            for cod, rec in data["per_med"].items():
                t = totaal.get(cod)
                if t is None:
                    t = totaal[cod] = dict(rec)
                    t["regios"] = 0
                else:
                    t["n_pre"] += rec["n_pre"]
                    if not t.get("formaf"):
                        t["formaf"] = rec.get("formaf", "")
                t["regios"] += 1
                if rec.get("formaf"):
                    self._formafs.add(_plat(rec["formaf"]))

        # ---- ingebouwde controle -------------------------------------------
        r_tot = sum(c[1] for c in controle)
        s_tot = sum(c[2] for c in controle)
        afw = (100.0 * (r_tot - s_tot) / s_tot) if s_tot else 0.0
        print(f"    regio's: {len(controle)}/{len(bestanden)} verwerkt "
              f"({opgehaald} vers opgehaald, {len(controle) - opgehaald} uit cache)")
        print(f"    DESABASTECIDO-rijen: {r_tot}  vs  RESUMEN-som: {s_tot}  ({afw:+.1f}%)")
        for regio, rj, rs in controle:
            if rs and abs(100.0 * (rj - rs) / rs) > 10:
                print(f"      {regio}: {rj} rijen vs {rs} in RESUMEN "
                      f"({100.0*(rj-rs)/rs:+.1f}%) - nakijken")
        if mislukt:
            print(f"    NIET opgehaald ({len(mislukt)}): {', '.join(mislukt[:8])}"
                  + (" ..." if len(mislukt) > 8 else ""))

        # Alles of niets. Een deelverzameling zou een landelijk beeld suggereren dat er niet
        # is - precies wat er bij Denemarken maanden onopgemerkt gebeurde. De regio's die wel
        # lukten staan in de cache, dus een tweede run pakt alleen de rest op.
        if len(controle) < len(bestanden):
            print(f"  ONVOLLEDIG: {len(controle)} van {len(bestanden)} regio's. Geen landelijk")
            print("  beeld mogelijk, dus LEGE DataFrame. Draai opnieuw: wat al gelukt is staat")
            print("  in cache/pe_digemid/ en wordt niet opnieuw opgehaald.")
            return leeg

        # ---- landelijke regels ---------------------------------------------
        laatste_dag = date(jaar, maand, calendar.monthrange(jaar, maand)[1]).isoformat()
        nu = datetime.now().isoformat()
        rijen = []
        for cod, rec in sorted(totaal.items(), key=lambda kv: -kv[1]["n_pre"]):
            stof, sterkte = self._splits_naam(rec["nombre_med"], rec.get("formaf", ""))
            rijen.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": rec["nombre_med"],
                "active_substance": stof,
                "strength": sterkte,
                # De bron scheidt sterkte en verpakking niet betrouwbaar; niet gokken.
                "package_size": "",
                # codigo_med is de SISMED-catalogus code, GEEN handelsvergunningnummer.
                # Niet verwarren met registro sanitario.
                "product_no": cod,
                # ATC zit niet in de bron; die komt via de bestaande o4-mini-route (>=90%).
                "atc_code": "",
                # Vergunninghouder, startdatum, verwachte einddatum en reden staan NIET in
                # deze bron. Leeg laten. fecha_venc uit de bron is de houdbaarheid van de
                # partij en is dus GEEN estimated_end - dat veld nooit daarmee vullen.
                "marketing_auth_holder": "",
                "shortage_start": "",
                "estimated_end": "",
                "status": "shortage",
                "dosage_form": rec.get("formaf", ""),
                "reason": "",
                # De rapportage gaat over een hele maand; de laatste dag daarvan is het
                # enige verdedigbare peilmoment. Nooit de scrapedatum.
                "last_updated": laatste_dag,
                # De eigenlijke waarde van deze bron: hoe breed het tekort is.
                "affected_establishments": rec["n_pre"],
                "affected_regions": rec["regios"],
                "source_url": editie_url,
                "scraped_at": nu,
            })

        df = pd.DataFrame(rijen, columns=self.KOLOMMEN)
        print(f"  Totaal: {len(df)} unieke geneesmiddelen (codigo_med) over "
              f"{len(controle)} regio's, rapportagemaand {jaar}-{maand:02d}")
        if not df.empty:
            print(f"    getroffen inrichtingen: {int(df['affected_establishments'].sum())} "
                  f"product-inrichtingcombinaties; breedste: "
                  f"{df.iloc[0]['medicine_name'][:60]} "
                  f"({df.iloc[0]['affected_establishments']} inrichtingen)")
        return df
