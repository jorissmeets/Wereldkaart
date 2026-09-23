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
"""
import collections
import csv
import difflib
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import PRK as _PRK
sys.path.insert(0, _PRK)
import enrich_atc_llm as E  # noqa: E402

from lcg_paden import BASE
ATC2NAME = os.path.join(BASE, "atc2name.json")
DREMPEL = 0.85

# Alleen namen in latijns schrift; katakana/cyrillisch levert geen bruikbaar skelet op.
LATIJN = re.compile(r"^[\x20-\x7EÀ-ɏ\s\-/,.()]+$")


def gelijkenis(a: str, b: str) -> float:
    sa, sb = E.skelet(a), E.skelet(b)
    if not sa or not sb:
        return 0.0
    if sa == sb or sa in sb or sb in sa:
        return 1.0
    return difflib.SequenceMatcher(None, sa, sb).ratio()


def is_combinatie(stof: str, naam: str) -> bool:
    """Combinatiepreparaten laten we met rust: meerdere stoffen, een code."""
    s, n = stof.upper(), naam.upper()
    return ("," in s or "/" in s or ";" in s or " EN " in s
            or "COMBINAT" in n or " MET " in n)


def beoordeel(atc: str, stof: str, atc2: dict, index: dict):
    """Geef (nieuwe_code, reden) terug. nieuwe_code is None als er niets te corrigeren valt."""
    if not atc or not stof or not LATIJN.match(stof):
        return None, None
    huidig = atc2.get(atc)
    if not huidig:
        return None, None
    if is_combinatie(stof, huidig):
        return None, None
    r_oud = gelijkenis(stof, huidig)
    if r_oud >= 0.80:
        return None, None
    kandidaten = index.get(E.skelet(stof))
    kandidaten = [k for k in (list(kandidaten) if isinstance(kandidaten, (set, list, tuple))
                              else [kandidaten]) if k]
    if len(kandidaten) != 1:
        return None, "skeletindex niet eenduidig"
    nieuw = kandidaten[0]
    if nieuw == atc:
        return None, "index bevestigt huidige code"
    if nieuw[:5] != atc[:5]:
        return None, "andere ATC4-groep (toedieningscontext)"
    nieuw_naam = atc2.get(nieuw)
    if not nieuw_naam:
        return None, "nieuwe code niet in referentie"
    if gelijkenis(stof, nieuw_naam) < DREMPEL or gelijkenis(stof, nieuw_naam) <= r_oud:
        return None, "nieuwe naam past onvoldoende beter"
    return nieuw, (atc, nieuw, huidig[:22], nieuw_naam[:22])


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


def corrigeer_datajson(atc2: dict, index: dict, schrijf: bool) -> None:
    pad = os.path.join(BASE, "data.json")
    d = json.load(open(pad, encoding="utf-8"))
    prk2atc = lees_prk2atc()
    paren = collections.Counter()
    afgewezen = collections.Counter()
    prk_status = collections.Counter()
    for rec in d["records"]:
        oud = (rec.get("atc") or "").strip().upper()
        nieuw, info = beoordeel(oud, (rec.get("sub") or "").strip(), atc2, index)
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


def main() -> None:
    schrijf = "--schrijf" in sys.argv
    alleen_data = "--data" in sys.argv
    atc2 = json.load(open(ATC2NAME, encoding="utf-8"))
    if hasattr(E, "_bouw_skelet_index") and not getattr(E, "SKELET", None):
        E._bouw_skelet_index()
    index = getattr(E, "SKELET", {}) or {}
    print(f"referentie: {len(atc2)} ATC-namen, {len(index)} skelet-ingangen\n")

    if alleen_data:
        corrigeer_datajson(atc2, index, schrijf)
        print("\nGESCHREVEN" if schrijf else "\nPROEFDRAAI -- niets gewijzigd. Draai met --schrijf.")
        return

    afgewezen = collections.Counter()
    paren = collections.Counter()
    per_bestand = collections.Counter()
    totaal_bekeken = 0

    for pad in sorted(glob.glob(os.path.join(BASE, "output", "*.csv"))):
        with open(pad, encoding="utf-8", newline="") as f:
            rijen = list(csv.DictReader(f))
        if not rijen or "atc_code" not in rijen[0] or "active_substance" not in rijen[0]:
            continue

        veranderd = 0
        for rij in rijen:
            atc = (rij.get("atc_code") or "").strip().upper()
            stof = (rij.get("active_substance") or "").strip()
            if not atc or not stof or not LATIJN.match(stof):
                continue
            huidig = atc2.get(atc)
            if not huidig:
                continue
            totaal_bekeken += 1
            if is_combinatie(stof, huidig):
                continue
            r_oud = gelijkenis(stof, huidig)
            if r_oud >= 0.80:
                continue                                   # past al, niets te doen

            kandidaten = index.get(E.skelet(stof))
            kandidaten = [k for k in (list(kandidaten) if isinstance(kandidaten, (set, list, tuple))
                                      else [kandidaten]) if k]
            if len(kandidaten) != 1:
                afgewezen["skeletindex niet eenduidig"] += 1
                continue
            nieuw = kandidaten[0]
            if nieuw == atc:
                afgewezen["index bevestigt huidige code"] += 1
                continue
            if nieuw[:5] != atc[:5]:
                afgewezen["andere ATC4-groep (toedieningscontext)"] += 1
                continue
            nieuw_naam = atc2.get(nieuw)
            if not nieuw_naam:
                afgewezen["nieuwe code niet in referentie"] += 1
                continue
            r_nieuw = gelijkenis(stof, nieuw_naam)
            if r_nieuw < DREMPEL or r_nieuw <= r_oud:
                afgewezen["nieuwe naam past onvoldoende beter"] += 1
                continue

            paren[(atc, nieuw, huidig[:22], nieuw_naam[:22])] += 1
            rij["atc_code"] = nieuw
            veranderd += 1

        if veranderd:
            per_bestand[os.path.basename(pad)] = veranderd
            if schrijf:
                with open(pad, "w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=list(rijen[0].keys()))
                    w.writeheader()
                    w.writerows(rijen)

    print(f"regels met ATC en latijnse stofnaam bekeken: {totaal_bekeken}")
    print(f"CORRECTIES: {sum(paren.values())} regels, {len(paren)} unieke code-paren\n")
    for (oud, nieuw, on, nn), n in sorted(paren.items(), key=lambda kv: -kv[1]):
        print(f"  {oud} -> {nieuw}  {on:24} => {nn:24}  x{n}")
    print("\nper bestand:")
    for b, n in per_bestand.most_common():
        print(f"  {b:40} {n:4d}")
    print("\nniet gecorrigeerd (blijft liggen):")
    for k, n in afgewezen.most_common():
        print(f"  {k:42} {n:5d}")
    print("\nGESCHREVEN" if schrijf else "\nPROEFDRAAI -- niets gewijzigd. Draai met --schrijf.")


if __name__ == "__main__":
    main()
