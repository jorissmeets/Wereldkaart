#!/usr/bin/env python3
"""Haal PRK-koppelingen weg die bij een andere stof horen dan de melding zelf.

WAAROM DIT BESTAAT
Een PRK is een Nederlands voorschrijfbaar product met een vaste ATC in de G-standaard.
Een buitenlandse melding heeft zelf ook een ATC. Horen die twee niet bij elkaar, dan is de
koppeling fout -- welke route hem ook heeft gelegd. Op 30-09 bleek dat bij 723 meldingen de
PRK bij een compleet andere stof hoorde (ander ATC4):

    AT  albendazol         -> PRK 27278  ibuprofen tablet 400 mg
    AT  insuline glargine  -> PRK 124559 clopidogrel tablet 75 mg
    IS  levodopa/carbidopa -> PRK 87165  oseltamivir capsule 30 mg
    IS  alprazolam         -> PRK 76910  bevacizumab infusie

plus 112 met de goede therapeutische groep maar het verkeerde molecuul. IJsland alleen al
504. Dat is precies wat de PRK-laag moet voorkomen: een Nederlandse apotheker ziet op het
Overzicht "clopidogrel is in Oostenrijk in tekort", terwijl het om insuline gaat.

De oorzaak zit in de koppelaar (hij groepeert op naam/sterkte/vorm/verpakking maar niet op
ATC, en heeft een eigen cache naast prk_koppeltabel.csv). Die wordt apart uitgezocht. Deze
toets hangt daar bewust NIET van af: hoe de fout er ook in komt, hij komt er hier uit.

WAT HET DOET
  --data         data.json: PRK weghalen waar de ATC niet klopt. De melding zelf blijft.
  --koppeltabel  prk_koppeltabel.csv: dezelfde foute koppelingen eruit, zodat de volgende
                 run ze opnieuw aan de koppelaar voorlegt in plaats van ze uit de cache te
                 halen. Zonder deze stap komen ze elke week terug.
  --schrijf      zonder deze vlag alleen tellen en voorbeelden tonen.

DE REGEL
  melding met ATC5 (7 tekens): de PRK moet in de G-standaard exact die ATC5 hebben.
  melding met alleen ATC4 (5 tekens): de PRK moet in die ATC4-groep vallen.
  PRK die niet in de G-standaard staat (sinds de koppeling uit de handel): blijft staan.
  Dat is geen fout, alleen historisch, en de pooldekking telt hem al niet mee.
  Sinds 01-10 ook de STERKTE (sterkte.py): noemen meldingsnaam en PRK-naam elk precies een
  sterkte, van dezelfde soort, en verschillen die meer dan een factor 1,25, dan wijst de PRK een
  ander Nederlands product aan (Sloveense Ecansya 150 mg hing aan de 500 mg-PRK, Duitse
  metoprolol 50 mg aan 200 mg). Op 01-10 265 meldingen.

    uv run --python 3.13 python toets_prk_atc.py --data --koppeltabel --schrijf
"""
import argparse
import collections
import csv
import io
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE, GSTD  # noqa: E402
from vorm import vorm_botst  # noqa: E402
from sterkte import sterkte_botst  # noqa: E402

DATA = os.path.join(BASE, "data.json")
KOPPEL = os.path.join(BASE, "prk_koppeltabel.csv")
CONFLICT = os.path.join(BASE, "logs", "prk_atc_conflict.csv")


PRK_STOF = {}          # PRK -> Nederlandse stofnaam uit de G-standaard


def lees_gstandaard():
    prk_atc = collections.defaultdict(set)
    with io.open(GSTD, encoding="ISO-8859-1", newline="") as f:
        for r in csv.DictReader(f, delimiter=";"):
            prk = (r.get("PRK code") or "").strip()
            atc = (r.get("ATC code") or "").strip().upper()
            if prk and atc:
                prk_atc[prk].add(atc)
                PRK_STOF.setdefault(prk, (r.get("ATC omschrijving Nederlands") or "").upper())
    if len(prk_atc) < 1000:
        # Een lege of afgekapte G-standaard zou hier ALLE koppelingen goedkeuren (niets
        # bekend = niets fout). Dan liever hard stoppen.
        sys.exit(f"G-standaard lijkt onvolledig ({len(prk_atc)} PRK in {GSTD}); toets afgebroken")
    return prk_atc


def _letters(t):
    """Hoofdletters zonder accenten, en klank gelijkgetrokken over de talen heen.

    Zonder dat laatste zag de toets Ierse "Morphine sulfate" niet als dezelfde stof als de
    Nederlandse PRK "MORFINE" (eerste zes letters MORPHI tegen MORFIN) en gooide hij een
    goede koppeling weg. Zelfde val: phenobarbital/fenobarbital, amphotericin/amfotericine,
    ephedrine/efedrine, cyclophosphamide/cyclofosfamide.
    """
    import unicodedata
    t = unicodedata.normalize("NFKD", str(t or "")).encode("ascii", "ignore").decode().upper()
    for a, b in (("PH", "F"), ("TH", "T"), ("Y", "I"), ("K", "C")):
        t = t.replace(a, b)
    return re.sub(r"[^A-Z]", " ", t)


def naam_noemt_stof(melding, prk):
    """True als de naam van de melding de stof van de PRK noemt (eerste 6 letters).

    Dan is bij een ATC-verschil meestal niet de PRK fout maar de ATC van de MELDING: op
    30-09 stond Spaanse pravastatine onder C10AA04 (fluvastatine), Celebrex onder L01XX33,
    cefazoline onder cefalexine -- telkens met de GOEDE PRK. Die weghalen gooit de goede
    koppeling weg. Niet alle gevallen zijn zo: Loniten-tabletten hingen aan minoxidil-
    haarlotion. Een regel die beide goed doet bestaat niet, dus laten we ze staan zoals ze
    waren en schrijven ze weg voor nazicht.
    """
    stof = [w for w in _letters(PRK_STOF.get(str(prk), "")).split() if len(w) >= 6][:2]
    tekst = _letters(f"{melding.get('mn') or ''} {melding.get('sub') or ''}")
    return bool(stof) and any(w[:6] in tekst for w in stof)


def past(atc, prk, prk_atc):
    """True = koppeling klopt, False = andere stof, None = PRK onbekend in de G-standaard."""
    bekend = prk_atc.get(str(prk).strip())
    if not bekend:
        return None
    atc = (atc or "").strip().upper()
    if len(atc) == 7:
        return atc in bekend
    if len(atc) == 5:
        return any(a.startswith(atc) for a in bekend)
    return None          # geen bruikbare ATC op de melding: niets over te zeggen


def toets_data(prk_atc, schrijf):
    d = json.load(open(DATA, encoding="utf-8"))
    fout, n_vorm, n_sterkte, conflict = [], 0, 0, []
    for r in d["records"]:
        if not r.get("prk"):
            continue
        if past(r.get("atc"), r["prk"], prk_atc) is False:
            if naam_noemt_stof(r, r["prk"]):
                if r.get("prk_naam") and sterkte_botst(r.get("mn") or "", r["prk_naam"]):
                    # Ook als de ATC van de melding de verdachte is: een andere sterkte
                    # maakt het hoe dan ook een ander Nederlands product (morfine 30 mg/ml
                    # aan de 10 mg/ml-ampul).
                    fout.append(r)
                    n_sterkte += 1
                    continue
                conflict.append(r)          # vermoedelijk verkeerde ATC op de melding
                continue
            fout.append(r)
        elif r.get("prk_naam") and vorm_botst(r, r["prk_naam"]):
            # Goede stof, andere toedieningsvorm: een infusie aan een tablet-PRK. Die wijst
            # een Nederlands product als geraakt aan dat het niet is.
            fout.append(r)
            n_vorm += 1
        elif r.get("prk_naam") and sterkte_botst(r.get("mn") or "", r["prk_naam"]):
            # Goede stof en vorm, andere sterkte: de PRK is een ander Nederlands product.
            fout.append(r)
            n_sterkte += 1
    per = collections.Counter(r["cc"] for r in fout)
    print(f"data.json: {len(fout) - n_vorm - n_sterkte} PRK-koppelingen bij een andere stof, "
          f"{n_vorm} bij de goede stof maar een andere toedieningsvorm, {n_sterkte} bij een andere sterkte")
    print(f"  {len(conflict)} blijven staan: de meldingsnaam noemt de stof van de PRK, dus waarschijnlijk"
          f" is de ATC van de melding fout, niet de PRK -> {CONFLICT}")
    os.makedirs(os.path.dirname(CONFLICT), exist_ok=True)
    with io.open(CONFLICT, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["land", "meldings_atc", "melding", "prk", "prk_atc", "prk_stof", "prk_naam"])
        for r in conflict:
            w.writerow([r["cc"], r.get("atc"), r.get("mn") or r.get("sub"), r["prk"],
                        "/".join(sorted(prk_atc.get(str(r["prk"]), ()))), PRK_STOF.get(str(r["prk"]), ""),
                        r.get("prk_naam")])
    if per:
        print("  per land:", ", ".join(f"{k} {v}" for k, v in per.most_common()))
    for r in fout[:6]:
        print(f"    {r['cc']} {r.get('atc')} {(r.get('sub') or r.get('mn') or '')[:30]:30s}"
              f" -> PRK {r['prk']} {'/'.join(sorted(prk_atc[str(r['prk'])]))}"
              f" {(r.get('prk_naam') or '')[:30]}")
    for r in fout:
        r["_prk"] = r["prk"]          # alleen in het geheugen, voor de koppeltabel-stap
    if schrijf:
        for r in fout:
            r["prk"] = None
            r.pop("prk_naam", None)
        schoon = [{k: v for k, v in r.items() if k != "_prk"} for r in d["records"]]
        # Ontdubbelen NA het schoonmaken. Twee meldingen die alleen in hun (foute) PRK
        # verschilden, worden identiek zodra die PRK weg is -- op 30-09 zeven stuks, o.a.
        # twee keer Entresto in IJsland aan twee verschillende foute PRK's. De bewaker
        # weigert elk duplicaat, dus zonder deze stap blokkeert de toets zijn eigen run.
        gezien, uniek = set(), []
        for r in schoon:
            k = tuple(sorted((a, str(b)) for a, b in r.items()))
            if k not in gezien:
                gezien.add(k)
                uniek.append(r)
        n_dub = len(schoon) - len(uniek)
        if fout or n_dub:
            with open(DATA, "w", encoding="utf-8") as f:
                json.dump({**d, "records": uniek}, f, ensure_ascii=False, separators=(",", ":"))
            print(f"  geschreven: {len(fout)} PRK weggehaald, de meldingen zelf blijven staan"
                  + (f"; {n_dub} daardoor identiek geworden en ontdubbeld" if n_dub else ""))
    return fout


def toets_koppeltabel(fout, schrijf):
    """Haal de sleutels weg die tot een foute koppeling leidden.

    De koppeltabel kent geen ATC; de sleutel is een productcode of een hash. We weten dus
    niet rechtstreeks welke sleutel bij welke melding hoorde. Wel: welke (land, PRK) fout
    was. Een PRK die in een land bij een andere stof staat, wordt voor dat land geschrapt
    -- op alle sleutels waar hij in dat land aan hangt. Dat is ruimer dan strikt nodig;
    de goede koppelingen daaronder worden de volgende run opnieuw gelegd. Liever een paar
    extra aanroepen van de koppelaar dan een foute koppeling die blijft staan.
    """
    if not os.path.exists(KOPPEL):
        print(f"koppeltabel niet gevonden: {KOPPEL}")
        return
    fout_paren = {(r["cc"], str(r["prk"]).strip()) for r in fout}
    rijen = list(csv.DictReader(io.open(KOPPEL, encoding="utf-8", newline="")))
    weg = [r for r in rijen if (r["cc"], (r.get("prk") or "").strip()) in fout_paren]
    print(f"koppeltabel: {len(rijen)} koppelingen | {len(weg)} te schrappen (land + foute PRK)")
    if schrijf and weg:
        blijft = [r for r in rijen if (r["cc"], (r.get("prk") or "").strip()) not in fout_paren]
        with io.open(KOPPEL, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rijen[0].keys()))
            w.writeheader()
            w.writerows(blijft)
        print(f"  geschreven: {len(weg)} weg, {len(blijft)} over")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", action="store_true")
    ap.add_argument("--koppeltabel", action="store_true")
    ap.add_argument("--schrijf", action="store_true")
    a = ap.parse_args()
    if not (a.data or a.koppeltabel):
        a.data = True
    prk_atc = lees_gstandaard()

    # Eerst de foute (land, PRK)-paren vastleggen, DAN pas schoonmaken. Andersom zijn de
    # PRK's al weg uit data.json en weet de koppeltabel-stap niet meer welke fout waren.
    fout = toets_data(prk_atc, schrijf=a.schrijf and a.data)
    paren = [{"cc": r["cc"], "prk": r["_prk"]} for r in fout]
    if a.koppeltabel:
        toets_koppeltabel(paren, a.schrijf)


if __name__ == "__main__":
    main()
