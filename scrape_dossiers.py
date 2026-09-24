#!/usr/bin/env python3
"""Haal de ACTUELE tekortendossiers van lcg.nl op.

WAAROM DIT BESTAAT
De Dossierstatuspagina toonde een lijst die HARDGECODEERD in index.html stond: zestien
dossiers met een fase en een bijwerkdatum die iemand in juli met de hand had ingetypt. Die
liepen zes weken achter, en twee ervan stonden niet eens meer als actueel op lcg.nl. De
pagina draaide dus wel mee in de verversing voor de buitenlandse meldingen, maar het
Nederlandse dossier ernaast bevroor.

DE BRON
https://lcg.nl/actuele-tekortendossiers/ geeft per dossier de titel, de escalatiefase
("Status:"), de datum laatste update, de publicatiedatum en een link naar het detail.
Het detail staat onder https://lcg.nl/medicijnentekort/<slug>/.

DE ATC-KOPPELING
Die staat niet op lcg.nl. Ze komt uit dossier_atc.json, opgebouwd uit de lijst die tot nu
toe in index.html stond. Een NIEUW dossier heeft daar nog geen ingang; dat krijgt een lege
ATC en wordt luid gemeld, zodat iemand hem er eenmalig bij zet. Stil laten liggen zou
betekenen dat het dossier wel op de pagina staat maar nooit met buitenlandse meldingen
verbonden wordt.

    uv run --python 3.13 --with requests --with beautifulsoup4 --with lxml python scrape_dossiers.py
"""
import json
import os
import re
import sys
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE  # noqa: E402

OVERZICHT = "https://lcg.nl/actuele-tekortendossiers/"
UIT = os.path.join(BASE, "dossiers.json")
ATC_KAART = os.path.join(BASE, "dossier_atc.json")
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

MAAND = {"januari": 1, "februari": 2, "maart": 3, "april": 4, "mei": 5, "juni": 6,
         "juli": 7, "augustus": 8, "september": 9, "oktober": 10, "november": 11,
         "december": 12}

# De vijf fasen zoals lcg.nl ze schrijft. Volgorde = ernst; index.html kleurt erop.
FASEN = ["Therapeutische coördinatiefase", "Logistieke coördinatiefase",
         "Bewakingsfase", "Monitoring", "Inventarisatie"]


def datum(tekst: str) -> str:
    """'21 september 2026' -> '2026-09-21'. Leeg bij iets onbekends."""
    m = re.search(r"(\d{1,2})\s+([a-zA-Zé]+)\s+(\d{4})", tekst or "")
    if not m:
        return ""
    dag, maand, jaar = m.groups()
    mnd = MAAND.get(maand.lower())
    if not mnd:
        return ""
    try:
        return date(int(jaar), mnd, int(dag)).isoformat()
    except ValueError:
        return ""


def main() -> None:
    atc_kaart = {}
    if os.path.exists(ATC_KAART):
        atc_kaart = json.load(open(ATC_KAART, encoding="utf-8"))

    r = requests.get(OVERZICHT, timeout=60, headers=UA)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # ANKEREN OP DE LINKS, niet op een blokklasse. De elf dossiers staan als broers in
    # EEN kolomblok; mikken op div[class*=kadence-column] levert dus een blok met alle elf
    # links erin en daarmee een dossier in plaats van elf. Vanaf elke link omhoog lopen tot
    # het kleinste voorouderblok dat precies dit ene dossier omvat.
    links = soup.find_all("a", href=re.compile(r"/medicijnentekort/[a-z0-9-]+/?$"))
    dossiers, zonder_atc = [], []
    gezien = set()
    for link in links:
        url = link.get("href", "")
        if not url or url in gezien:
            continue
        gezien.add(url)
        blok = link
        for _ in range(8):
            ouder = blok.parent
            if ouder is None:
                break
            aantal = len(ouder.find_all("a", href=re.compile(r"/medicijnentekort/[a-z0-9-]+/?$")))
            if aantal > 1:
                break            # nog een stap hoger zou een tweede dossier meenemen
            blok = ouder
            if "Status" in blok.get_text() and "laatste update" in blok.get_text():
                break
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        kop = blok.find(["h2", "h3", "h4"])
        titel = (kop.get_text(strip=True) if kop else link.get_text(strip=True)) \
            or slug.replace("-", " ").title()

        tekst = re.sub(r"\s+", " ", blok.get_text(" ", strip=True))
        fase = next((f for f in FASEN if f.lower() in tekst.lower()), "")
        bijgewerkt = datum(tekst.split("laatste update", 1)[-1][:60]) if "laatste update" in tekst else ""
        gepubliceerd = datum(tekst.split("Publicatiedatum", 1)[-1][:60]) if "Publicatiedatum" in tekst else ""

        bekend = atc_kaart.get(slug, {})
        atc = bekend.get("atc", "")
        if not atc:
            zonder_atc.append((slug, titel))
        dossiers.append({
            "key": bekend.get("key") or slug,
            "slug": slug,
            "title": titel,
            "atc": atc,
            "phase": fase,
            "updated": bijgewerkt,
            "published": gepubliceerd,
            "url": url,
        })

    if not dossiers:
        raise SystemExit("geen dossiers gevonden -- structuur van lcg.nl gewijzigd?")

    dossiers.sort(key=lambda d: (FASEN.index(d["phase"]) if d["phase"] in FASEN else 99,
                                 d["title"]))
    json.dump({"generated": date.today().isoformat(), "bron": OVERZICHT,
               "dossiers": dossiers},
              open(UIT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"dossiers.json: {len(dossiers)} actuele dossiers")
    tel = {}
    for d in dossiers:
        tel[d["phase"] or "(geen fase)"] = tel.get(d["phase"] or "(geen fase)", 0) + 1
    for f, n in tel.items():
        print(f"  {n:2}x {f}")
    met = sum(1 for d in dossiers if d["updated"])
    print(f"  met bijwerkdatum: {met}/{len(dossiers)}")
    if zonder_atc:
        print(f"\n  LET OP: {len(zonder_atc)} dossier(s) zonder ATC in dossier_atc.json.")
        print("  Zonder ATC is er geen koppeling met de buitenlandse meldingen:")
        for slug, titel in zonder_atc:
            print(f"    {slug}  ({titel})")


if __name__ == "__main__":
    main()
