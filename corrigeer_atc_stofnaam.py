#!/usr/bin/env python3
"""Corrigeer ATC-codes die niet bij de stofnaam in de melding passen.

WAT HET PROBLEEM IS
De meeste buitenlandse bronnen leveren geen ATC-code; die kennen wij zelf toe met
enrich_atc_llm.py. Dat gaat meestal goed, maar niet altijd, en een foute ATC is erger dan
een ontbrekende: hij legt een buitenlands tekort onder het VERKEERDE molecuul, waardoor een
Nederlands geneesmiddel geraakt lijkt terwijl er niets aan de hand is. Aangetroffen in de
data van 23-09-2026:

    ivabradine        stond op C01EB16 (ibuprofen, neonataal)
    oxycodon          stond op N02AA03 (hydromorfon)
    ceftriaxon        stond op J01DD08 (cefixim) en J01DD01 (cefotaxim)
    bupivacaine       stond op N01BB02 (lidocaine)
    sotalol           stond op C07AA05 (propranolol)

HOE DIT WERKT
Voor elke regel met een ATC en een stofnaam in latijns schrift:
 1. vergelijk de stofnaam met de OFFICIELE stofnaam achter de toegekende code (atc2name.json),
    via het skelet -- dat normaliseert zouten, taalvarianten en spelling, zodat
    "QUETIAPIN FUMARAT" en "QUETIAPINE" gelijk zijn;
 2. wijkt hij af, zoek de stofnaam dan op in de deterministische skeletindex;
 3. corrigeer ALLEEN als aan alle vier de eisen is voldaan.

DE VIER EISEN, en waarom ze er zijn
 a. de skeletindex geeft precies EEN code voor deze stof (geen gok bij ambiguiteit);
 b. die code zit in DEZELFDE ATC4-groep als de huidige. Dit is de belangrijkste rem. Zonder
    deze eis stelt de index 95 keer voor om adrenaline van C01CA24 (systemisch) naar A01AD01
    (tandheelkundig) te verplaatsen -- dezelfde stof, totaal andere toepassing. De index kent
    stofnamen, geen toedieningscontext;
 c. de nieuwe code staat in atc2name.json, zodat we WETEN welke stof het is;
 d. de officiele naam van de nieuwe code lijkt voor >= 0,85 op de stofnaam uit de melding,
    en beter dan de oude. Dit hield bijvoorbeeld "turoctocog alfa" -> FACTOR VIII tegen:
    inhoudelijk juist, maar de namen lijken niet op elkaar, dus buiten bereik van deze
    deterministische stap.

Wat hier niet doorheen komt is geen goedgekeurde code, alleen een onbesliste. De afgewezen
gevallen worden geteld en samengevat, zodat zichtbaar blijft hoeveel er blijft liggen.

    uv run --python 3.13 --with python-dotenv --with openai --with pandas \
        python corrigeer_atc_stofnaam.py [--schrijf] [--data]

Zonder --schrijf toont hij alleen wat hij zou doen.

WAAROM OOK --data
De verse bronbestanden in output/ zijn niet de hele kaart: _merge_live.py vult aan met
records uit de vorige live-data voor moleculen die de verse scrape niet dekt. Van de
ivabradine-regels die op de ibuprofencode stonden kwamen er 26 van de 36 langs die weg
binnen. Alleen de CSV's corrigeren laat die dus staan. Met --data draait dezelfde toets over
data.json, NA de merge, en worden de indices herberekend.

Plaats in de pijplijn: na _merge_live.py, voor build_atc4_dekking.py.

UITBREIDING 01-10-2026, na de hercontrole van Jespers validatie
De vier eisen hielden te veel tegen. Van de ~230 zichtbare meldingen onder een verkeerd molecuul
(eletriptan op almotriptan, galantamine op tacrine, pravastatine op fluvastatine, bosentan op
veratrum) kwam er geen door, om vier redenen:
 1. zouten in Romaanse en Slavische talen bleven in het skelet staan (ELETRIPTAN BROMIDRATO,
    PRAVASTATINA SODICA, bosentan hidrat), zodat de index niets vond;
 2. atc2name.json kent alleen codes die in Nederland op de markt zijn (1.941); tacrine,
    bekanamycine en veratrum hebben er geen naam, dus viel er niets te vergelijken;
 3. een skelet dat IN het andere voorkwam gold als gelijk: sufentanil "was" fentanyl;
 4. Kroatisch schrijft vericigvat en riocigvat.
Nu: een voorbewerking (kern) haalt die zouten en schrijfwijzen weg voor het skelet; de
officiele WHO-naam van ELKE code (atc_who.json, alleen lokaal en in de prive-pijplijnrepo
wegens het WHOCC-auteursrecht) vult atc2name aan; gelijk is gelijk of een voorvoegsel met
hooguit drie letters verschil, nooit een stuk uit het midden.
De ATC4-rem blijft de eerste keus. Buiten de groep wordt alleen gecorrigeerd als de huidige
code aantoonbaar een ANDERE stof is (bosentan op veratrum) en er voor de stof precies een code
in de dichtstbijzijnde groep bestaat, of maar een code in het geheel. Wat niet eenduidig is,
blijft liggen. Categoriecodes ("kunsttranen", "spiraal met progestageen", "multi-enzymen"),
vaccins, allergenen, diagnostica, infuusoplossingen en zouten van metalen (zinkacetaat is niet
zinksulfaat) worden niet aangeraakt.
Met --schrijf (zonder --data) wordt ook de LLM-cache rechtgezet, anders legt de verrijking
volgende week dezelfde foute code terug.
"""
import collections
import csv
import difflib
import glob
import json
import os
import re
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import PRK as _PRK
sys.path.insert(0, _PRK)
import enrich_atc_llm as E  # noqa: E402

from lcg_paden import BASE
ATC2NAME = os.path.join(BASE, "atc2name.json")
WHO_NAMEN = os.path.join(BASE, "atc_who.json")
DREMPEL = 0.85
GELIJK = 0.88          # vanaf hier "is" een naam de stof

# Zouten, hydraten en esters die het skelet van enrich_atc_llm nog laat staan, als stam
# (begin van het woord). Zonder deze laag vond de index ELETRIPTAN BROMIDRATO niet.
EXTRA_ZOUT = tuple("""HYDROCHLOR HIDROCLOR CLORHIDR CLORIDR CHLORHYDR HYDROKLOR HIDROKLOR DIHIDROKLOR
CHLORID CLORUR KLORID BROMID BROMUR BROMHIDR BROMIDR HYDROBROM HIDROBROM BROMHYDR NATRI NATRIJ
SODI SODN KALIJ POTAS MAGNES MESIL MESYL BESIL BESYL MALEA FUMARA HEMIFUMAR TARTRA HEMITARTAR
BITARTR CITRA DICITR SULFA SULPHA SOLFAT FOSFA PHOSPHA ACETA ACETONID SUCCINA SUKCINA OXALA
OKSALA LACTA LAKTA GLUCONA GLUKONA DIGLUCON DIGLUKON NITRA ARGININ ERBUMIN TOSILA TOSYLA ESILA
ESYLA EMBONA PAMOA PROPIONA DIPROPION VALERA BUTIRA BUTYRA DECANOA ENANTA ENANTHA UNDECANOA
CIPIONA CYPIONA HICLA HYCLA IDROGENOMALA HIDROGENOMAL MONOHIDRA MONOHYDRA DIHIDRA DIHYDRA
TRIHIDRA TRIHYDRA SESQUIHYDR HEMIHIDRA HEMIHYDRA HEMIHEPTAHYDR PENTAHIDR PENTAHYDR HIDRAT HYDRAT
ANHIDR ANHYDR WASSERFREI DISODI DINATRI TROMETAMOL MEGLUMIN OLAMIN AXETIL PIVOXIL""".split())
LOSSE_WOORDEN = set("""INN PH EUR USP BP JP DCI DCF ACIDUM ACIDO ACIDE ACID ACIDI SALE SAL SALT BASE BASIS
FOR INJECTION INJECTABLE INYECTABLE SOLUCION SOLUTION POLVO POWDER TABLET TABLETS CAPSULE CAPSULES
ORAL EMULSION DE DI DEL DELLA LA LE DES""".split())
METAAL = set("""ZINC ZINK ZINCI ZINCUM CALCIUM CALCI KALSIUM CALCICO CALCICA POTASSIUM KALIUM KALII
SODIUM NATRIUM MAGNESIUM IRON FERRUM FERROUS FERRIC JERN RAUTA COPPER CUPRUM KOPER LITHIUM LITIUM
SELENIUM IODINE JOD IOD FLUORIDE ALUMINIUM ALUMINUM BISMUTH SILVER ARGENTUM STRONTIUM BARIUM
CALCIO CALCII CALCIQUE KALCIJ KALCIUM SODIO SODIQUE POTASIO POTASSIO POTASSIQUE MAGNESIO MAGNESII MAGNEZIJ
FERRO FERROSO FERRICO HIERRO FER FERREUX ZINCO CINC LITIO COBRE RAME ARGENTO YODO IODO""".split())
# Codes waar een stofnaam niet de beslissende informatie is (of geen stof): hier niet aankomen.
BUITEN = ("Q", "J07", "V01", "V04", "V06", "V07", "V09", "V10", "B05", "A10A")
# Landen waarvan de bron zelf de ATC levert. Die code is leidend: in Belgie heet de stof van
# Elvanse "Dexamfetamine" terwijl FAMHP terecht N06BA12 (lisdexamfetamine) geeft. Een stofnaam
# mag een broncode niet overschrijven. IT en ES horen hier sinds 01-10 bij (AIFA-kolom, CIMA).
BRON_ATC = {"AT", "BE", "CH", "CZ", "DE", "DK", "EE", "ES", "EU", "FI", "GR", "HU", "IS", "IT",
            "LT", "MY", "NO", "SE", "SI", "SK", "TR"}
CATEGORIE = re.compile(r"COMBINAT|VARIOUS|\bOTHERS?\b|\bAND\b|\bWITH\b|PREPARATION|\bAGENTS?\b|\bETC\b|TEARS|"
                       r"POLLEN|MULTIENZYM|VACCIN|ALLERGEN|EXTRACT|DIAGNOST|\bPLAIN\b|DERIVATIVE|ANTISER|"
                       r"IMMUNOGLOBULIN|ELECTROLYT|SOLUTION|\bIUD\b|OVERIG|DIVERSE|PREPARATEN|\bMET\b|\bEN\b")

# Alleen namen in latijns schrift; katakana/cyrillisch levert geen bruikbaar skelet op.
LATIJN = re.compile(r"^[\x20-\x7EÀ-ɏ\s\-/,.()]+$")


def kern(s: str, referentie: bool = False) -> str:
    """Stofnaam zonder zouten, hydraten, esters, vormwoorden en losse lidwoorden, in hoofdletters.

    Een stof die alleen uit een metaal en een zout bestaat (zinkacetaat, kaliumchloride) houdt
    niets over: daar IS het zout de stof, en dan beslist deze stap niet.
    """
    t = str(s or "")
    if referentie:
        # "insulin (human)" en "insulin (beef)" zijn voor deze vergelijking allebei insuline;
        # de kwalificatie mag geen voorvoegseltreffer maken met de ene en niet met de andere.
        t = re.sub(r"\([^)]*\)", " ", t)
    t = unicodedata.normalize("NFKD", t.upper())
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"\bGV(?=[AEIOU])", "GU", t)                 # Kroatisch: vericigvat -> vericiguat
    t = re.sub(r"GV(?=[AEIOU])", "GU", t)
    t = re.sub(r"\bKV(?=[AEIOU])", "QU", t)                 # kvetiapin -> quetiapin
    woorden = re.findall(r"[A-Z]+", t)
    if woorden and all(w in METAAL or w.startswith(EXTRA_ZOUT) or w in LOSSE_WOORDEN for w in woorden):
        return ""
    over = [w for w in woorden if len(w) >= 3 and w not in LOSSE_WOORDEN and w not in METAAL
            and not w.startswith(EXTRA_ZOUT)]
    return " ".join(over)


ROMEINS = re.compile(r"^(I{1,3}|IV|V|VI{1,3}|IX|X|XI{1,3})$")


def kenmerk(s: str, referentie: bool = False) -> frozenset:
    """Losse letters en Romeinse cijfers die een stof onderscheiden: polymyxine B of E, factor VIII
    of IX, vitamine A of D. kern() laat ze weg (te kort), en het skelet strijkt een eind-E glad --
    zonder deze toets werd POLIMIXINA E (colistine) als polymyxine B 'gecorrigeerd'.
    Een losse letter telt alleen als hij achteraan staat; midden in de naam is het een voegwoord
    ("amoxicillina e acido clavulanico")."""
    t = str(s or "")
    if referentie:
        t = re.sub(r"\([^)]*\)", " ", t)
    t = unicodedata.normalize("NFKD", t.upper())
    t = "".join(c for c in t if not unicodedata.combining(c))
    woorden = [w for w in re.findall(r"[A-Z]+", t)
               if not (w in LOSSE_WOORDEN or w in METAAL or w.startswith(EXTRA_ZOUT))]
    uit = {w for w in woorden[1:] if len(w) > 1 and ROMEINS.match(w)}
    if len(woorden) >= 2 and len(woorden[-1]) == 1:
        uit.add(woorden[-1])
    return frozenset(uit)


def sk(s: str, referentie: bool = False) -> str:
    """Skelet na de extra voorbewerking."""
    k = kern(s, referentie)
    return E.skelet(k) if k else ""


def gelijkenis(a: str, b: str) -> float:
    """Hoe sterk wijst naam b dezelfde stof aan als a?

    Gelijk skelet = 1. Een voorvoegsel telt alleen mee als het verschil hooguit drie letters is
    (uitgangen als -A, -UM, -INE), nooit een stuk uit het midden: sufentanil is geen fentanyl.
    Spellingvarianten gaan via difflib, maar alleen bij dezelfde eerste drie letters.
    """
    sa, sb = sk(a), sk(b, referentie=True)
    if not sa or not sb:
        return 0.0
    ka, kb = kenmerk(a), kenmerk(b, referentie=True)
    if ka and kb and ka != kb:
        return 0.0
    if sa == sb:
        return 1.0
    kort, lang = sorted((sa, sb), key=len)
    if len(kort) >= 5 and lang.startswith(kort) and len(lang) - len(kort) <= 3:
        return 0.95
    # difflib alleen voor een spellingvariant van een letter: penicilline en penicillamine
    # scheelden twee letters en haalden zo 0,90.
    if sa[:3] != sb[:3] or abs(len(sa) - len(sb)) > 1:
        return 0.0
    return difflib.SequenceMatcher(None, sa, sb).ratio()


def is_combinatie(stof: str, naam: str) -> bool:
    """Combinatiepreparaten laten we met rust: meerdere stoffen, een code."""
    s, n = stof.upper(), naam.upper()
    return ("," in s or "/" in s or ";" in s or " EN " in s or " + " in s
            or "COMBINAT" in n or " MET " in n)


class Referentie:
    """ATC -> namen (atc2name.json, Nederlands, plus de WHO-naam) en het omgekeerde, per skelet."""

    def __init__(self, atc2: dict, who: dict):
        self.atc2, self.who = atc2, who
        self.per_begin = collections.defaultdict(list)          # eerste 4 skeletletters -> (skelet, code)
        for code in set(atc2) | set(who):
            if code.startswith(BUITEN):
                continue
            for naam in self.namen(code):
                if self.categorie(naam):
                    continue
                k = sk(naam, referentie=True)
                if k:
                    self.per_begin[k[:4]].append((k, code, naam))

    def namen(self, code: str) -> list:
        return [n for n in (self.atc2.get(code), self.who.get(code)) if n]

    @staticmethod
    def categorie(naam: str) -> bool:
        return bool(CATEGORIE.search(naam.upper()))

    def kandidaten(self, stof: str, minimaal: float = GELIJK) -> set:
        k = sk(stof)
        if not k:
            return set()
        return {code for kk, code, naam in self.per_begin.get(k[:4], []) if gelijkenis(stof, naam) >= minimaal}


def beoordeel(atc: str, stof: str, ref: "Referentie"):
    """Geef (nieuwe_code, info) terug, of (None, reden) als er niets te corrigeren valt."""
    if not atc or not stof or not LATIJN.match(stof) or atc.startswith(BUITEN):
        return None, None
    namen = ref.namen(atc)
    if not namen:
        return None, "geen referentienaam voor de huidige code"
    if is_combinatie(stof, " ".join(namen)) or any(ref.categorie(n) for n in namen):
        return None, None
    if not sk(stof) or not any(sk(n, referentie=True) for n in namen):
        return None, None                                          # alleen zout of metaal
    r_oud = max(gelijkenis(stof, n) for n in namen)
    if r_oud >= GELIJK:
        return None, None                                          # past al
    kand = {k for k in ref.kandidaten(stof) if k != atc}
    if not kand:
        return None, "stof niet eenduidig in de referentie"
    # Eerst dezelfde ATC4-groep (toedieningscontext), dan ATC3, ATC2, hoofdgroep; bij twee
    # kandidaten op hetzelfde niveau beslissen we niet.
    for n in (5, 4, 3):
        groep = {k for k in kand if k[:n] == atc[:n]}
        if len(groep) == 1:
            nieuw = groep.pop()
            return nieuw, (atc, nieuw, namen[-1][:22], ref.namen(nieuw)[-1][:22])
        if len(groep) > 1:
            return None, "meerdere codes in dezelfde groep"
    # Verder weg dan ATC2 alleen bij een volledig gelijke naam (skelet gelijk of voorvoegsel),
    # en dan alleen als er in de hoofdgroep, of in het geheel, precies een code is.
    # Buiten de ATC2-groep weten we niet welke toepassing bedoeld is (retinol: vitamine, acne,
    # neus, oog); dan alleen als de stof in de hele index maar EEN code heeft.
    streng = {k for k in ref.kandidaten(stof, 0.95) if k != atc}
    if len(streng) == 1:
        nieuw = next(iter(streng))
        return nieuw, (atc, nieuw, namen[-1][:22], ref.namen(nieuw)[-1][:22])
    if len(streng) > 1:
        return None, "meerdere codes, geen in dezelfde groep"
    return None, "alleen een vage treffer buiten de groep"


def lees_prk2atc() -> dict:
    """PRK-code -> ATC uit de G-standaard, om te toetsen of een PRK nog klopt."""
    pad = os.path.join(BASE, "gstandaard_actueel.csv")
    kaart: dict = {}
    if not os.path.exists(pad):
        print("  LET OP: gstandaard_actueel.csv ontbreekt; PRK-consistentie niet getoetst")
        return kaart
    with open(pad, encoding="ISO-8859-1", newline="") as f:
        for r in csv.DictReader(f, delimiter=";"):
            p = (r.get("PRK code") or "").strip()
            a = (r.get("ATC code") or "").strip().upper()
            if p and a:
                kaart.setdefault(p, a)
    return kaart


def corrigeer_datajson(ref: Referentie, schrijf: bool) -> None:
    pad = os.path.join(BASE, "data.json")
    d = json.load(open(pad, encoding="utf-8"))
    prk2atc = lees_prk2atc()
    paren = collections.Counter()
    afgewezen = collections.Counter()
    prk_status = collections.Counter()
    for rec in d["records"]:
        oud = (rec.get("atc") or "").strip().upper()
        if rec.get("cc") in BRON_ATC:
            continue
        nieuw, info = beoordeel(oud, (rec.get("sub") or "").strip(), ref)
        if nieuw is None:
            if info:
                afgewezen[info] += 1
            continue
        paren[info] += 1
        rec["atc"] = nieuw

        # De PRK is gekoppeld TOEN de ATC nog fout was. Hoort hij bij de oude code, dan wijst
        # hij het verkeerde Nederlandse product aan -- schadelijker dan geen koppeling, want
        # het dashboard toont dat product dan als geraakt. In dat geval halen we hem weg en
        # laten we de volgende PRK-ronde het opnieuw proberen, nu met de juiste ATC.
        prk = rec.get("prk")
        if not prk:
            prk_status["geen PRK"] += 1
            continue
        bij = prk2atc.get(str(prk))
        if bij == nieuw:
            prk_status["PRK klopte al bij de nieuwe code"] += 1
        elif bij == oud:
            prk_status["PRK hoorde bij de FOUTE code -- verwijderd"] += 1
            rec.pop("prk", None)
            rec.pop("prk_naam", None)
        else:
            prk_status["PRK bij geen van beide -- verwijderd"] += 1
            rec.pop("prk", None)
            rec.pop("prk_naam", None)

    n = sum(paren.values())
    print(f"\n--- data.json ---\nCORRECTIES: {n} records, {len(paren)} unieke code-paren")
    for k, v in prk_status.most_common():
        print(f"    {k:44} {v:4d}")
    for (oud, nw, on, nn), c in sorted(paren.items(), key=lambda kv: -kv[1]):
        print(f"  {oud} -> {nw}  {on:24} => {nn:24}  x{c}")
    print("  niet gecorrigeerd:", dict(afgewezen.most_common(4)))
    if not schrijf or not n:
        return
    # Indices herberekenen: de ATC-lijst en de telling per ATC zijn nu veranderd.
    recs = d["records"]
    acc: dict = {}
    for r in recs:
        acc.setdefault(r["atc"], set()).add(r["cc"])
    d["total_atc"] = len(sorted({r["atc"] for r in recs}))
    d["atc_country_count"] = {k: len(v) for k, v in acc.items()}
    json.dump(d, open(pad, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print(f"  geschreven; total_atc nu {d['total_atc']}")


def lees_who() -> dict:
    if os.path.exists(WHO_NAMEN):
        return json.load(open(WHO_NAMEN, encoding="utf-8"))
    print(f"  LET OP: {os.path.basename(WHO_NAMEN)} ontbreekt; alleen atc2name.json als referentie "
          f"(codes die niet in Nederland op de markt zijn, worden dan niet beoordeeld)")
    return {}


def corrigeer_cache(ref: Referentie, schrijf: bool) -> None:
    """Zet de LLM-cache recht met dezelfde beoordeling, zodat een gecorrigeerde code niet terugkomt."""
    pad = getattr(E, "CACHE", os.path.join(BASE, "atc_llm_cache.csv"))
    if not os.path.exists(pad):
        return
    rijen = list(csv.DictReader(open(pad, encoding="utf-8")))
    paren = collections.Counter()
    for r in rijen:
        if not r.get("key", "").startswith("S:") or not r.get("atc"):
            continue
        nieuw, info = beoordeel(r["atc"].strip().upper(), r["key"][2:], ref)
        if nieuw:
            paren[info] += 1
            r["atc"] = nieuw
    print(f"\n--- LLM-cache ---\nCORRECTIES: {sum(paren.values())} stofnamen")
    for (oud, nw, on, nn), c in sorted(paren.items(), key=lambda kv: -kv[1])[:40]:
        print(f"  {oud} -> {nw}  {on:24} => {nn:24}")
    if schrijf and paren:
        with open(pad, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f); w.writerow(["key", "atc"])
            for r in sorted(rijen, key=lambda r: r["key"]):
                w.writerow([r["key"], r["atc"]])


def main() -> None:
    schrijf = "--schrijf" in sys.argv
    alleen_data = "--data" in sys.argv
    atc2 = json.load(open(ATC2NAME, encoding="utf-8"))
    ref = Referentie(atc2, lees_who())
    print(f"referentie: {len(atc2)} ATC-namen uit atc2name, {len(ref.who)} WHO-namen\n")

    if alleen_data:
        corrigeer_datajson(ref, schrijf)
        print("\nGESCHREVEN" if schrijf else "\nPROEFDRAAI -- niets gewijzigd. Draai met --schrijf.")
        return

    afgewezen = collections.Counter()
    paren = collections.Counter()
    per_bestand = collections.Counter()
    totaal_bekeken = 0

    # Alleen de bestanden die de kaart voeden. longitudinal.csv is een historisch ijkpunt (mei)
    # en bevat ook rijen uit landen met een bron-ATC; die horen hier niet te veranderen.
    for pad in sorted(glob.glob(os.path.join(BASE, "output", "*_shortage_*.csv"))):
        with open(pad, encoding="utf-8", newline="") as f:
            rijen = list(csv.DictReader(f))
        if not rijen or "atc_code" not in rijen[0] or "active_substance" not in rijen[0]:
            continue
        if os.path.basename(pad)[:2] in BRON_ATC:
            continue

        veranderd = 0
        for rij in rijen:
            atc = (rij.get("atc_code") or "").strip().upper()
            stof = (rij.get("active_substance") or "").strip()
            if not atc or not stof:
                continue
            totaal_bekeken += 1
            nieuw, info = beoordeel(atc, stof, ref)
            if nieuw is None:
                if info:
                    afgewezen[info] += 1
                continue
            paren[info] += 1
            rij["atc_code"] = nieuw
            veranderd += 1

        if veranderd:
            per_bestand[os.path.basename(pad)] = veranderd
            if schrijf:
                with open(pad, "w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=list(rijen[0].keys()))
                    w.writeheader()
                    w.writerows(rijen)

    print(f"regels met ATC en stofnaam bekeken: {totaal_bekeken}")
    print(f"CORRECTIES: {sum(paren.values())} regels, {len(paren)} unieke code-paren\n")
    for (oud, nieuw, on, nn), n in sorted(paren.items(), key=lambda kv: -kv[1]):
        print(f"  {oud} -> {nieuw}  {on:24} => {nn:24}  x{n}")
    print("\nper bestand:")
    for b, n in per_bestand.most_common():
        print(f"  {b:40} {n:4d}")
    print("\nniet gecorrigeerd (blijft liggen):")
    for k, n in afgewezen.most_common():
        print(f"  {k:42} {n:5d}")
    corrigeer_cache(ref, schrijf)
    print("\nGESCHREVEN" if schrijf else "\nPROEFDRAAI -- niets gewijzigd. Draai met --schrijf.")


if __name__ == "__main__":
    main()
