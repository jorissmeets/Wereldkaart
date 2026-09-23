#!/usr/bin/env python3
"""Houd per verversing bij WANNEER wij een molecuul voor het eerst in een land zagen.

WAAROM DIT BESTAAT
Het LCG wil weten of tekorten in een vaste volgorde over landen trekken -- of Duitsland
bijvoorbeeld stelselmatig kort voor Slovenie in tekort raakt, zodat een Duitse melding als
vroegsignaal kan dienen.

Dat is met de GERAPPORTEERDE startdatums niet fatsoenlijk te beantwoorden. Die betekenen per
land iets anders (meldingsdatum versus feitelijke start), de registers lopen dertien jaar
uiteen in hoe ver ze teruggaan, en sommige landen bewaren oude meldingen terwijl andere
alleen de actuele stand tonen. Een naieve analyse daarop gaf 'Slowakije loopt 1.696 dagen
voor op Griekenland' -- vier en een half jaar, en puur een gevolg van hoe lang elk register
bestaat.

Wat WEL overal hetzelfde gemeten wordt, is wanneer WIJ iets voor het eerst zien. Dat is
onafhankelijk van meldplicht en registerouderdom. Dit script legt dat vast, elke run een
regel erbij. Na een paar maanden wekelijks draaien is er een panel waarmee de vraag echt te
beantwoorden is.

WAT HET BIJHOUDT (analyse/volgorde/panel.csv)
  atc, land          het molecuul in dat land
  eerst_gezien       de eerste verversing waarin wij dit zagen
  laatst_gezien      de meest recente
  keer_gezien        in hoeveel verversingen
  eerst_actief       de eerste verversing waarin het als ACTIEF tekort stond
  gerapporteerde_start  wat de bron zelf als startdatum geeft, ter vergelijking

BELANGRIJK: eerst_gezien van een molecuul dat al bij de EERSTE run in de lijst stond, zegt
niets -- dat tekort liep al. Die rijen krijgen vlag 'vanaf_begin'; sluit ze uit bij analyse.
Pas nieuwe verschijningen ná de eerste run zijn waarnemingen.

    uv run --python 3.13 python bouw_panel.py
"""
import csv
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE  # noqa: E402

PANEL = os.path.join(BASE, "analyse", "volgorde", "panel.csv")
DATA = os.path.join(BASE, "data.json")
VELDEN = ["atc", "land", "eerst_gezien", "laatst_gezien", "keer_gezien",
          "eerst_actief", "gerapporteerde_start", "stof", "vanaf_begin"]


def main() -> None:
    os.makedirs(os.path.dirname(PANEL), exist_ok=True)
    vandaag = date.today().isoformat()

    bestaand = {}
    eerste_run = True
    if os.path.exists(PANEL):
        with open(PANEL, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                bestaand[(r["atc"], r["land"])] = r
        eerste_run = False
        print(f"panel geladen: {len(bestaand)} combinaties")
    else:
        print("nog geen panel -- deze run is de nulmeting")

    d = json.load(open(DATA, encoding="utf-8"))
    nu = {}
    for x in d["records"]:
        atc = (x.get("atc") or "").upper()
        cc = x.get("cc")
        if not atc or not cc:
            continue
        k = (atc, cc)
        actief = x.get("st") == "active"
        vorig = nu.get(k)
        if vorig is None:
            nu[k] = {"actief": actief,
                     "start": x.get("ss") or "",
                     "stof": (x.get("sub") or x.get("mn") or "")[:60]}
        else:
            vorig["actief"] = vorig["actief"] or actief
            if x.get("ss") and (not vorig["start"] or x["ss"] < vorig["start"]):
                vorig["start"] = x["ss"]

    nieuw = weer_actief = 0
    for k, v in nu.items():
        r = bestaand.get(k)
        if r is None:
            bestaand[k] = {
                "atc": k[0], "land": k[1],
                "eerst_gezien": vandaag, "laatst_gezien": vandaag, "keer_gezien": "1",
                "eerst_actief": vandaag if v["actief"] else "",
                "gerapporteerde_start": v["start"], "stof": v["stof"],
                # Bij de nulmeting weten we niet of het tekort net begon of al liep.
                "vanaf_begin": "ja" if eerste_run else "nee",
            }
            nieuw += 1
        else:
            r["laatst_gezien"] = vandaag
            r["keer_gezien"] = str(int(r.get("keer_gezien") or 0) + 1)
            if v["actief"] and not r.get("eerst_actief"):
                r["eerst_actief"] = vandaag
                weer_actief += 1
            if v["start"] and not r.get("gerapporteerde_start"):
                r["gerapporteerde_start"] = v["start"]

    with open(PANEL, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=VELDEN)
        w.writeheader()
        for r in sorted(bestaand.values(), key=lambda r: (r["atc"], r["land"])):
            w.writerow({k: r.get(k, "") for k in VELDEN})

    bruikbaar = sum(1 for r in bestaand.values() if r.get("vanaf_begin") == "nee")
    print(f"panel.csv: {len(bestaand)} combinaties | {nieuw} nieuw deze run"
          + (f" | {weer_actief} voor het eerst actief" if weer_actief else ""))
    print(f"  bruikbaar voor volgorde-analyse (niet vanaf de nulmeting): {bruikbaar}")
    if eerste_run:
        print("  LET OP: dit is de nulmeting. Alles staat op 'vanaf_begin=ja' en zegt nog")
        print("  niets over volgorde. Vanaf de VOLGENDE run tellen nieuwe verschijningen mee.")


if __name__ == "__main__":
    main()
