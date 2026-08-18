#!/usr/bin/env python3
"""Scrape de SFK-tekortenlijst (totaal) en koppel aan de G-standaard.

Bron: https://www3.sfk.nl/tekorten/?soort=totaal (server-side HTML-tabel).
Per artikel: ZI-nummer, artikelnaam, verpakking-status, preferentiebeleid per
verzekeraar. Koppeling ZI-nummer -> PRK + ATC + PRK-omschrijving via LCG.csv
(deterministisch, ~95%). ATC->EMS-kleur doet de vroegsignalering-pagina zelf.

Schrijft sfk_tekorten.json voor het Vroegsignalering-dashboard.

  uv run --python 3.12 --with requests --with beautifulsoup4 --with lxml --with pandas \
    python scrape_sfk.py
"""
import requests, re, json, datetime
from bs4 import BeautifulSoup
import pandas as pd

SFK_URL = "https://www3.sfk.nl/tekorten/?soort=totaal"
LCG_CSV = "/Users/karkara/Documents/LCG/Matchen_prk/LCG.csv"
OUT = "sfk_tekorten.json"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124 Safari/537.36"}


def scrape():
    html = requests.get(SFK_URL, headers=UA, timeout=40).text
    soup = BeautifulSoup(html, "lxml")
    # De datatabel = de tabel met de meeste rijen.
    table = max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")))
    rows = table.find_all("tr")
    header = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
    # Alleen echte "Preferent bij <verzekeraar>"-kolommen -> verzekeraarnaam
    ins_cols = {i: re.sub(r"(?i)^preferent bij\s*", "", header[i]).strip()
                for i in range(len(header)) if header[i].lower().startswith("preferent bij")}
    opm_col = next((i for i, hh in enumerate(header) if "opmerking" in hh.lower()), None)
    items = []
    for r in rows[1:]:
        c = [x.get_text(" ", strip=True) for x in r.find_all(["td", "th"])]
        if len(c) < 3 or not c[1].isdigit():
            continue
        pref = [ins_cols[i] for i in ins_cols if i < len(c) and c[i].strip()]
        opm = c[opm_col].strip() if opm_col is not None and opm_col < len(c) else ""
        items.append({
            "zi": c[1],
            "naam": c[2],
            "status": c[3] if len(c) > 3 else "",
            "preferent": pref,
            "opmerking": opm,
        })
    return items


def couple(items):
    z = pd.read_csv(LCG_CSV, encoding="latin-1", delimiter=";",
                    usecols=["ZI-nummer", "PRK code", "PRK omschrijving", "ATC code"], dtype=str).fillna("")
    z["ZI-nummer"] = z["ZI-nummer"].str.strip()
    z["PRK code"] = z["PRK code"].str.replace(r"\.0$", "", regex=True).str.strip()
    zi2 = {r["ZI-nummer"]: (r["PRK code"], r["PRK omschrijving"], r["ATC code"].strip())
           for _, r in z.drop_duplicates("ZI-nummer").iterrows()}
    hit = 0
    for it in items:
        m = zi2.get(it["zi"])
        if not m:
            continue
        if m[0]:
            it["prk"], it["prk_naam"] = m[0], m[1]
        if m[2]:
            it["atc"] = m[2]
        if m[0] or m[2]:
            hit += 1
    return hit


def main():
    items = scrape()
    hit = couple(items)
    payload = {
        "generated": datetime.date.today().isoformat(),
        "source": SFK_URL,
        "count": len(items),
        "gekoppeld": hit,
        "items": items,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=0)
    n_pref = sum(1 for it in items if it["preferent"])
    print(f"SFK-tekorten: {len(items)} artikelen; gekoppeld aan PRK/ATC: {hit} "
          f"({100 * hit // max(len(items), 1)}%), preferent: {n_pref} -> {OUT}")


if __name__ == "__main__":
    main()
