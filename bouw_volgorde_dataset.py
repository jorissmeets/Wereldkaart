#!/usr/bin/env python3
"""Zet data klaar om te onderzoeken of tekorten in een vaste VOLGORDE over landen trekken.

DE VRAAG
Raakt Duitsland stelselmatig kort voor Slovenie in tekort? Dan zou een Duitse melding als
vroegsignaal kunnen dienen. Dit script maakt de data om dat te ONDERZOEKEN. Het trekt zelf
geen conclusies, en dat is met opzet -- zie de waarschuwingen hieronder.

WAT HET OPLEVERT (in analyse/volgorde/)
  gebeurtenissen.csv   een rij per (molecuul, land): de vroegste startdatum, plus hoe
                       betrouwbaar die datum is en waar hij vandaan komt
  paren.csv            een rij per (molecuul, land A, land B) met het verschil in dagen
  landenparen.csv      samengevat per landenpaar: hoe vaak A voor B kwam, mediaan verschil
  README.md            wat je er wel en niet uit mag afleiden

DE EENHEID VAN ANALYSE
Een "gebeurtenis" is een molecuul (ATC5) in een land, met de VROEGSTE startdatum van alle
meldingen daarover. Niet per verpakking: landen rapporteren op verschillend niveau (Oostenrijk
per verpakking, Frankrijk per specialiteit) en zonder die samenvoeging telt een land met veel
verpakkingen onterecht zwaar.

DRIE DINGEN DIE DE UITKOMST KUNNEN VERZINNEN

1. DE DATUMS METEN NIET HETZELFDE. Dit is het grootste risico. Een land waar de
   vergunninghouder twee maanden VOORAF moet melden, loopt per definitie voor op een land
   waar pas bij het feitelijke stokken wordt gemeld. Dan meet je meldplicht, geen besmetting.
   Per land staat in datum_betekenis.json wat er bekend is; landen die daar niet als
   vergelijkbaar in staan krijgen in de uitvoer een vlag.

2. HET WAARNEMINGSVENSTER IS SCHEEF. We hebben geen archief van jaren, maar een momentopname
   plus een bevroren kopie van 21-09-2026. Een tekort dat in land A al liep voordat wij
   begonnen te kijken, lijkt in land B "eerder" te beginnen. Daarom staat bij elke
   gebeurtenis of de datum VOOR het begin van het venster ligt; filter die weg bij twijfel.

3. GELIJKTIJDIGHEID IS GEEN VOLGORDE. Een Europees tekort door een fabriek die uitvalt raakt
   alle landen tegelijk; dat A drie dagen eerder meldde zegt dan niets. Kijk naar de SPREIDING
   van het verschil, niet alleen naar de mediaan: een paar met mediaan 14 dagen en een
   spreiding van 200 dagen is ruis.

    uv run --python 3.13 --with pandas python bouw_volgorde_dataset.py
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE  # noqa: E402

UIT = os.path.join(BASE, "analyse", "volgorde")
DATA = os.path.join(BASE, "data.json")
BETEKENIS = os.path.join(BASE, "datum_betekenis.json")

# HET VENSTER -- de belangrijkste knop in dit script.
# De registers lopen DERTIEN JAAR uiteen: IJsland gaat terug tot 2000 en Slowakije tot 2004,
# terwijl Griekenland pas in mei 2024 begint en Tsjechie in augustus 2024. Zonder venster
# "loopt" elk land met een lang register automatisch voor op elk land met een kort register.
# De eerste naieve draai gaf dan ook 'SK voor GR, mediaan 1696 dagen' -- vier en een half
# jaar. Dat is geen waarschuwingsgolf maar linker-censurering.
#
# Binnen het venster tellen alleen gebeurtenissen die BEIDE landen hadden kunnen zien. Landen
# waarvan het register pas NA de vensterstart begint, doen niet mee aan de venster-analyse:
# hun nullen zijn niet "geen tekort" maar "wij keken nog niet".
VENSTER_START = os.environ.get("VOLGORDE_VENSTER", "2025-01-01")

# Minimaal aantal gedeelde moleculen voordat een landenpaar iets betekent. Onder de tien
# is elke mediaan een toevalstreffer.
MIN_GEDEELD = 10


def dagen(a: str, b: str) -> int:
    return (datetime.strptime(b, "%Y-%m-%d").date() - datetime.strptime(a, "%Y-%m-%d").date()).days


def main() -> None:
    os.makedirs(UIT, exist_ok=True)
    d = json.load(open(DATA, encoding="utf-8"))
    records = d["records"]

    betekenis = {}
    if os.path.exists(BETEKENIS):
        betekenis = json.load(open(BETEKENIS, encoding="utf-8"))
        print(f"datumbetekenis geladen voor {len(betekenis)} landen")
    else:
        print("LET OP: datum_betekenis.json ontbreekt -- elke datum krijgt de vlag 'onbekend'")

    # ── 1. Gebeurtenissen: vroegste start per (ATC, land) ───────────────────────
    vroegste: dict = {}
    tel: dict = defaultdict(int)
    for x in records:
        atc = (x.get("atc") or "").upper()
        cc = x.get("cc")
        ss = x.get("ss")
        if not atc or not cc or not ss:
            continue
        tel[(atc, cc)] += 1
        if (atc, cc) not in vroegste or ss < vroegste[(atc, cc)]["start"]:
            vroegste[(atc, cc)] = {"start": ss, "status": x.get("st", ""),
                                   "naam": x.get("sub") or x.get("mn") or ""}

    vandaag = date.today().isoformat()
    gebeurtenissen = []
    for (atc, cc), v in sorted(vroegste.items()):
        b = betekenis.get(cc, {})
        gebeurtenissen.append({
            "atc": atc,
            "land": cc,
            "start": v["start"],
            "status": v["status"],
            "stof": v["naam"][:60],
            "meldingen": tel[(atc, cc)],
            "datum_betekenis": b.get("betekenis", "onbekend"),
            "datum_vergelijkbaar": b.get("vergelijkbaar", ""),
            "voor_venster": "ja" if v["start"] < VENSTER_START else "nee",
            "toekomst": "ja" if v["start"] > vandaag else "nee",
        })
    schrijf(os.path.join(UIT, "gebeurtenissen.csv"), gebeurtenissen)
    print(f"gebeurtenissen.csv: {len(gebeurtenissen)} rijen")

    # ── 2. Paren: per molecuul elk landenpaar met het verschil in dagen ─────────
    per_atc = defaultdict(list)
    for g in gebeurtenissen:
        per_atc[g["atc"]].append(g)

    paren = []
    for atc, lijst in per_atc.items():
        if len(lijst) < 2:
            continue
        lijst = sorted(lijst, key=lambda g: g["start"])
        for i, a in enumerate(lijst):
            for b in lijst[i + 1:]:
                if a["land"] == b["land"]:
                    continue
                paren.append({
                    "atc": atc,
                    "stof": a["stof"],
                    "eerst": a["land"],
                    "daarna": b["land"],
                    "start_eerst": a["start"],
                    "start_daarna": b["start"],
                    "dagen_ertussen": dagen(a["start"], b["start"]),
                    "beide_vergelijkbaar": "ja" if (a["datum_vergelijkbaar"] is True
                                                    and b["datum_vergelijkbaar"] is True) else "nee",
                    "een_voor_venster": "ja" if "ja" in (a["voor_venster"], b["voor_venster"]) else "nee",
                })
    schrijf(os.path.join(UIT, "paren.csv"), paren)
    print(f"paren.csv: {len(paren)} rijen")

    # ── 3. Landenparen samengevat ──────────────────────────────────────────────
    # Per ONGEORDEND paar {A,B} tellen we hoe vaak A eerder was en hoe vaak B, en hoe groot
    # het verschil was. Zo is "A loopt voor op B" een RICHTING met een sterkte, en geen
    # gevolg van de toevallige volgorde waarin we de landen aflopen.
    bak = defaultdict(list)
    for p in paren:
        sleutel = tuple(sorted((p["eerst"], p["daarna"])))
        teken = 1 if p["eerst"] == sleutel[0] else -1
        bak[sleutel].append(teken * p["dagen_ertussen"])

    samenvatting = []
    for (a, b), verschillen in bak.items():
        n = len(verschillen)
        if n < MIN_GEDEELD:
            continue
        a_eerst = sum(1 for v in verschillen if v > 0)
        b_eerst = sum(1 for v in verschillen if v < 0)
        gelijk = sum(1 for v in verschillen if v == 0)
        med = statistics.median(verschillen)
        samenvatting.append({
            "land_a": a,
            "land_b": b,
            "gedeelde_moleculen": n,
            "a_eerst": a_eerst,
            "b_eerst": b_eerst,
            "gelijke_dag": gelijk,
            "aandeel_a_eerst": round(a_eerst / n, 3),
            "mediaan_dagen": round(med, 1),
            "spreiding_dagen": round(statistics.pstdev(verschillen), 1) if n > 1 else 0,
            "kwartiel_25": round(statistics.quantiles(verschillen, n=4)[0], 1) if n >= 4 else "",
            "kwartiel_75": round(statistics.quantiles(verschillen, n=4)[2], 1) if n >= 4 else "",
        })
    samenvatting.sort(key=lambda r: -abs(r["aandeel_a_eerst"] - 0.5))
    schrijf(os.path.join(UIT, "landenparen.csv"), samenvatting)
    print(f"landenparen.csv: {len(samenvatting)} paren (NAIEF, niet gebruiken voor conclusies)")

    # ── 4. Dekking per land ────────────────────────────────────────────────────
    # Deze tabel is het belangrijkste diagnostische stuk: wie kan wanneer uberhaupt iets
    # gezien hebben. Zonder dit leest iedereen de naieve uitkomst verkeerd.
    per_land = defaultdict(list)
    for g in gebeurtenissen:
        per_land[g["land"]].append(g["start"])
    dekking = []
    for cc, datums in sorted(per_land.items(), key=lambda kv: min(kv[1])):
        datums = sorted(datums)
        dekking.append({
            "land": cc,
            "gebeurtenissen": len(datums),
            "vroegste": datums[0],
            "mediaan": datums[len(datums) // 2],
            "laatste": datums[-1],
            "voor_2026": sum(1 for x in datums if x < "2026-01-01"),
            "in_venster": sum(1 for x in datums if VENSTER_START <= x <= vandaag),
            "doet_mee_in_venster": "ja" if datums[0] <= VENSTER_START else "nee",
            "datum_betekenis": betekenis.get(cc, {}).get("betekenis", "onbekend"),
        })
    schrijf(os.path.join(UIT, "dekking.csv"), dekking)
    meedoen = {r["land"] for r in dekking if r["doet_mee_in_venster"] == "ja"}
    print(f"dekking.csv: {len(dekking)} landen, {len(meedoen)} met een register dat voor "
          f"{VENSTER_START} begint")

    # ── 5. Landenparen BINNEN het venster ──────────────────────────────────────
    bak_v = defaultdict(list)
    for p in paren:
        if p["eerst"] not in meedoen or p["daarna"] not in meedoen:
            continue
        if p["start_eerst"] < VENSTER_START or p["start_daarna"] < VENSTER_START:
            continue
        if p["start_daarna"] > vandaag:          # aangekondigd, nog niet gebeurd
            continue
        sleutel = tuple(sorted((p["eerst"], p["daarna"])))
        teken = 1 if p["eerst"] == sleutel[0] else -1
        bak_v[sleutel].append(teken * p["dagen_ertussen"])

    venster = []
    for (a, b), verschillen in bak_v.items():
        n = len(verschillen)
        if n < MIN_GEDEELD:
            continue
        a_eerst = sum(1 for v in verschillen if v > 0)
        b_eerst = sum(1 for v in verschillen if v < 0)
        venster.append({
            "land_a": a, "land_b": b, "gedeelde_moleculen": n,
            "a_eerst": a_eerst, "b_eerst": b_eerst,
            "gelijke_dag": sum(1 for v in verschillen if v == 0),
            "aandeel_a_eerst": round(a_eerst / n, 3),
            "mediaan_dagen": round(statistics.median(verschillen), 1),
            "spreiding_dagen": round(statistics.pstdev(verschillen), 1) if n > 1 else 0,
            "kwartiel_25": round(statistics.quantiles(verschillen, n=4)[0], 1) if n >= 4 else "",
            "kwartiel_75": round(statistics.quantiles(verschillen, n=4)[2], 1) if n >= 4 else "",
        })
    venster.sort(key=lambda r: -abs(r["aandeel_a_eerst"] - 0.5))
    schrijf(os.path.join(UIT, "landenparen_venster.csv"), venster)
    print(f"landenparen_venster.csv: {len(venster)} paren binnen {VENSTER_START}..{vandaag}")

    if venster:
        print("\nsterkst eenzijdige paren BINNEN het venster (nog GEEN conclusie -- zie README):")
        for r in venster[:10]:
            richting = f"{r['land_a']} voor {r['land_b']}" if r["aandeel_a_eerst"] > 0.5 \
                else f"{r['land_b']} voor {r['land_a']}"
            aandeel = max(r["aandeel_a_eerst"], 1 - r["aandeel_a_eerst"])
            print(f"  {richting:12} {aandeel:.0%} van {r['gedeelde_moleculen']:4} moleculen, "
                  f"mediaan {abs(r['mediaan_dagen']):.0f} dagen, spreiding {r['spreiding_dagen']:.0f}")


def schrijf(pad: str, rijen: list) -> None:
    if not rijen:
        open(pad, "w").close()
        return
    with open(pad, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rijen[0].keys()))
        w.writeheader()
        w.writerows(rijen)


if __name__ == "__main__":
    main()
