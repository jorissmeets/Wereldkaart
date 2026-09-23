#!/usr/bin/env python3
"""Haal de VOLLEDIGE historie van de SFK Monitor leveringsproblemen op.

De scraper scrape_sfk.py leest alleen de HTML van de huidige week. De bron heeft echter
een download-endpoint met een periodekiezer: 190 weken terug tot week 1 van 2023. En dat
endpoint levert CSV in plaats van HTML, met velden die de HTML-tabel niet toont:

    zi_nummer, artikelnaam, Verpakkingstatus, Preferent_bij_<6 verzekeraars>,
    'Laagste prijs niet beschikbaar', opmerking, jaar, weeknr

De HTML-scrape verloor die preferentie per verzekeraar (207 van 666 artikelen) en de
laagste-prijs-vlag. Juist 'preferent en op de monitor' is het signaal dat er een
vergoedingsprobleem dreigt, dus dat verlies was niet onschuldig.

WAT DIT OPLEVERT: per PRK het VERLOOP over de tijd -- hoeveel van zijn artikelen stonden
in welke week op de monitor. Daarmee is te zien of een tekort oploopt, stabiel is of wegebt,
in plaats van alleen de stand van deze week.

Weken worden gecachet in sfk_historie_cache/; een tweede run haalt alleen nieuwe weken op.

  uv run --python 3.13 --with requests --with beautifulsoup4 --with pandas python scrape_sfk_historie.py
"""
import csv
import io
import json
import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import sys as _sys
_sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE, GSTD as _GSTD
PAGINA = "https://www3.sfk.nl/tekorten/?soort=totaal"
DOWNLOAD = "https://www3.sfk.nl/tekorten/download?period={p}&totaal=1"
CACHE = os.path.join(BASE, "sfk_historie_cache")
UIT = os.path.join(BASE, "sfk_historie.json")
LCG = _GSTD
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def perioden() -> list:
    """Lees de beschikbare weken uit het periodekeuzevak op de pagina zelf.

    Bewust niet hardcoded: SFK voegt elke week een periode toe en heeft gaten in de reeks
    (week 38-2024, 27-2023 en 17-2023 ontbreken). Een zelfgebouwde reeks zou dus fouten
    opleveren voor weken die niet bestaan.
    """
    r = requests.get(PAGINA, headers=UA, timeout=60)
    r.raise_for_status()
    sel = BeautifulSoup(r.text, "html.parser").find("select", {"name": "period"})
    if sel is None:
        raise RuntimeError("periodekeuzevak niet gevonden -- structuur gewijzigd?")
    return [o.get("value") for o in sel.find_all("option") if o.get("value")]


def haal_week(p: str) -> list:
    """Haal een week op, uit de cache indien aanwezig."""
    os.makedirs(CACHE, exist_ok=True)
    pad = os.path.join(CACHE, f"{p}.csv")
    if os.path.exists(pad) and os.path.getsize(pad) > 200:
        tekst = open(pad, encoding="utf-8", errors="ignore").read()
    else:
        r = requests.get(DOWNLOAD.format(p=p), headers=UA, timeout=90)
        if r.status_code != 200 or "csv" not in r.headers.get("content-type", ""):
            return []
        tekst = r.text
        open(pad, "w", encoding="utf-8").write(tekst)
        time.sleep(0.3)                      # de bron is een kleine server; rustig aan
    return list(csv.DictReader(io.StringIO(tekst), delimiter=";"))


def zi_naar_prk() -> dict:
    """ZI-nummer -> (PRK, PRK-naam, ATC) uit de G-standaard."""
    m = {}
    if not os.path.exists(LCG):
        print("  LET OP: geen G-standaard gevonden; verloop wordt op ZI-niveau gegeven")
        return m
    with open(LCG, encoding="ISO-8859-1", newline="") as f:
        for r in csv.DictReader(f, delimiter=";"):
            zi = (r.get("ZI-nummer") or "").strip().lstrip("0")
            if zi:
                m[zi] = ((r.get("PRK code") or "").strip(),
                         (r.get("PRK omschrijving") or "").strip(),
                         (r.get("ATC code") or "").strip().upper())
    return m


def main():
    ps = perioden()
    print(f"{len(ps)} weken beschikbaar: {ps[-1]} t/m {ps[0]}")
    koppel = zi_naar_prk()

    # per week: welke PRK's hadden hoeveel artikelen op de monitor
    verloop = {}          # prk -> {week: aantal}
    meta = {}             # prk -> naam/atc
    per_week = {}         # week -> aantal artikelen
    pref_week = {}        # week -> aantal met preferentie
    gemist = 0

    for i, p in enumerate(ps):
        rows = haal_week(p)
        if not rows:
            gemist += 1
            continue
        jaar, week = p.split("-")[1], p.split("-")[0]
        sleutel = f"{jaar}-W{int(week):02d}"
        per_week[sleutel] = len(rows)
        pref_week[sleutel] = sum(
            1 for r in rows
            if any(str(v).strip() == "1" for k, v in r.items() if k.startswith("Preferent_bij"))
        )
        for r in rows:
            zi = (r.get("zi_nummer") or "").strip().lstrip("0")
            prk, naam, atc = koppel.get(zi, ("", "", ""))
            sl = prk or f"ZI{zi}"
            if not sl.strip() or sl == "ZI":
                continue
            verloop.setdefault(sl, {})
            verloop[sl][sleutel] = verloop[sl].get(sleutel, 0) + 1
            if sl not in meta:
                meta[sl] = {"naam": naam or (r.get("artikelnaam") or "").strip(), "atc": atc}
        if (i + 1) % 25 == 0:
            print(f"  ... {i + 1}/{len(ps)} weken")

    weken = sorted(per_week)
    uit = {
        "generated": datetime.now().date().isoformat(),
        "bron": PAGINA,
        "wat_het_meet": ("SFK Monitor leveringsproblemen: minimaal 2 van de 4 groothandels "
                         "konden dit artikel die week niet direct leveren. Geen beschikbaarheidstoets."),
        "weken": weken,
        "artikelen_per_week": per_week,
        "preferent_per_week": pref_week,
        "prk": {k: {**meta.get(k, {}), "verloop": v} for k, v in verloop.items()},
    }
    json.dump(uit, open(UIT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    print(f"\nsfk_historie.json: {len(weken)} weken, {len(verloop)} PRK's"
          f"{f', {gemist} weken niet opgehaald' if gemist else ''}")
    if weken:
        print(f"  bereik: {weken[0]} t/m {weken[-1]}")
        print(f"  artikelen op de monitor: {per_week[weken[0]]} (oudste) -> {per_week[weken[-1]]} (nieuwste)")
        langst = sorted(verloop.items(), key=lambda kv: -len(kv[1]))[:5]
        print("  langst onafgebroken op de monitor:")
        for k, v in langst:
            print(f"    {k} {meta.get(k, {}).get('naam', '')[:40]:<42} {len(v)} weken")


if __name__ == "__main__":
    main()
