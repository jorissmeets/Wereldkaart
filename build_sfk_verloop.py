#!/usr/bin/env python3
"""Maak van de volledige SFK-historie een slanke versie voor de browser.

scrape_sfk_historie.py levert sfk_historie.json: 190 weken x 3.282 PRK's, 1,7 MB. Dat is
te zwaar om naast data.json in te laden, en het meeste ervan is niet nodig: een middel dat
vandaag NIET op de monitor staat, kan vandaag ook geen oplopend signaal geven.

Dit script houdt daarom over:
  - de weekas van de laatste 104 weken (twee jaar) met de totalen per week;
  - per PRK DIE NU OP DE MONITOR STAAT het aantal artikelen per week, plus drie afgeleiden.

De drie afgeleiden zijn wat het verloop bruikbaar maakt in plaats van alleen mooi:

  aaneen        hoeveel weken dit middel ONAFGEBROKEN op de monitor staat, geteld vanaf de
                laatste week terug. Dit is het chronisch-tekortsignaal: 60 weken achtereen
                is een heel ander probleem dan drie weken.
  weken_totaal  in hoeveel van alle 190 weken het middel ooit op de monitor stond. Een hoog
                getal bij een lage 'aaneen' betekent terugkerend, niet nieuw.
  trend         het gemiddelde aantal getroffen artikelen van de laatste 4 weken tegen dat
                van de 8 weken daarvoor. Meer getroffen verpakkingen van hetzelfde middel is
                de vroegste zichtbare verslechtering: eerst valt een sterkte weg, dan de rest.

Waarom op ARTIKELAANTAL en niet op aan/afwezigheid: aanwezigheid is binair en beweegt dus
pas als het tekort helemaal voorbij is. Het aantal getroffen verpakkingen loopt daarvoor al
op. Een middel dat van 1 naar 4 artikelen gaat is aan het verslechteren terwijl de binaire
vlag onveranderd op 'ja' blijft staan.

  uv run --python 3.13 python build_sfk_verloop.py
"""
import json
import os
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
HISTORIE = os.path.join(BASE, "sfk_historie.json")
HUIDIG = os.path.join(BASE, "sfk_tekorten.json")
UIT = os.path.join(BASE, "sfk_verloop.json")

VENSTER = 104          # weken die de browser krijgt (twee jaar)
RECENT, DAARVOOR = 4, 8


def trend(reeks: list) -> str:
    """Vergelijk de laatste 4 weken met de 8 weken daarvoor."""
    if len(reeks) < RECENT + DAARVOOR:
        return "onbekend"
    recent = sum(reeks[-RECENT:]) / RECENT
    daarvoor = sum(reeks[-(RECENT + DAARVOOR):-RECENT]) / DAARVOOR
    if recent == 0:
        return "weg"
    if daarvoor == 0:
        return "nieuw"
    verhouding = recent / daarvoor
    if verhouding >= 1.25:
        return "stijgend"
    if verhouding <= 0.75:
        return "dalend"
    return "stabiel"


def aaneengesloten(reeks: list) -> int:
    """Aantal weken onafgebroken aanwezig, geteld vanaf het einde terug."""
    n = 0
    for waarde in reversed(reeks):
        if waarde <= 0:
            break
        n += 1
    return n


def main():
    if not os.path.exists(HISTORIE):
        raise SystemExit("sfk_historie.json ontbreekt -- draai eerst scrape_sfk_historie.py")

    hist = json.load(open(HISTORIE, encoding="utf-8"))
    alle_weken = hist["weken"]
    weken = alle_weken[-VENSTER:]

    # PRK's die NU op de monitor staan. Zonder dit bestand vallen we terug op alle PRK's
    # met recente activiteit, zodat het script ook los bruikbaar blijft.
    huidig = set()
    if os.path.exists(HUIDIG):
        for it in (json.load(open(HUIDIG, encoding="utf-8")) or {}).get("items", []):
            if it.get("prk"):
                huidig.add(str(it["prk"]))
    print(f"{len(huidig)} PRK's staan nu op de monitor")

    uit_prk = {}
    for sleutel, gegevens in hist["prk"].items():
        if huidig and sleutel not in huidig:
            continue
        verloop = gegevens.get("verloop", {})
        reeks = [verloop.get(w, 0) for w in weken]
        if not any(reeks):
            # Staat nu wel op de monitor maar niet in de historie: de weekdownload en de
            # HTML-pagina lopen soms een week uit de pas. Dan is er niets te tekenen.
            continue
        # 'aaneen' telt over de VOLLE historie, niet over het venster van 104 weken. Anders
        # loopt elk chronisch tekort tegen het plafond van de vensterbreedte aan en lijken
        # drie jaar en twee jaar even lang -- precies het onderscheid dat hier telt.
        volle_reeks = [verloop.get(w, 0) for w in alle_weken]
        uit_prk[sleutel] = {
            "v": reeks,
            "aaneen": aaneengesloten(volle_reeks),
            "weken_totaal": len(verloop),
            "trend": trend(reeks),
            "naam": gegevens.get("naam", ""),
            "atc": gegevens.get("atc", ""),
        }

    uit = {
        "generated": datetime.now().date().isoformat(),
        "bron": hist.get("bron", ""),
        "wat_het_meet": hist.get("wat_het_meet", ""),
        "weken": weken,
        "weken_beschikbaar": len(alle_weken),
        "totaal": [hist["artikelen_per_week"].get(w, 0) for w in weken],
        "preferent": [hist.get("preferent_per_week", {}).get(w, 0) for w in weken],
        "prk": uit_prk,
    }
    json.dump(uit, open(UIT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    kb = os.path.getsize(UIT) / 1024
    tellingen = {}
    for g in uit_prk.values():
        tellingen[g["trend"]] = tellingen.get(g["trend"], 0) + 1
    print(f"sfk_verloop.json: {len(uit_prk)} PRK's over {len(weken)} weken ({kb:.0f} KB)")
    print(f"  trend: {tellingen}")
    langst = sorted(uit_prk.items(), key=lambda kv: -kv[1]["aaneen"])[:5]
    print("  langst onafgebroken op de monitor:")
    for k, g in langst:
        print(f"    PRK {k:<8} {g['naam'][:44]:<46} {g['aaneen']} weken")


if __name__ == "__main__":
    main()
