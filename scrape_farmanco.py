#!/usr/bin/env python3
"""Scrape KNMP Farmanco-meldingen en filter op de Essential Medicines List (EMS-lijst).

Schrijft farmanco_eml.json voor het Vroegsignalering-dashboard (tab 3).
De pagina fetcht dit bestand; opnieuw draaien = verse data.

Draaien:
  uv run --python 3.12 --with requests --with beautifulsoup4 --with lxml --with pandas \
    python scrape_farmanco.py
"""
import requests, re, json, datetime
from functools import reduce
from bs4 import BeautifulSoup
import pandas as pd

FARMANCO_URL = "https://farmanco.knmp.nl/"
EML_CSV = "LijstenEMS/Stofnamen 2025-Tabel 1.csv"
LCG_CSV = "/Users/karkara/Documents/LCG/Matchen_prk/LCG.csv"
OUT = "farmanco_eml.json"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

SEV = {"geel": 1, "oranje": 2, "rood": 3}


def canon(s):
    """Uppercase, komma -> spatie (komma is deel van stofnaam, geen combo-scheiding), witruimte inklappen."""
    return re.sub(r"\s+", " ", str(s).upper().replace(",", " ")).strip()


def comps(s):
    """Combinatie-componenten: alleen splitsen op / en + (niet op komma)."""
    return [c for c in (canon(p) for p in re.split(r"[/+]", s)) if c]


def worse(a, b):
    return a if SEV.get(a, 0) >= SEV.get(b, 0) else b


def scrape_farmanco():
    """Farmanco rendert zijn meldingen server-side als <ul class="clear"> met sort-* li's."""
    html = requests.get(FARMANCO_URL, headers=UA, timeout=40).text
    soup = BeautifulSoup(html, "lxml")
    items = []
    for ul in soup.find_all("ul", class_="clear"):
        if not ul.find(class_="sort-active-ingredient"):
            continue

        def dv(cls):
            el = ul.find(class_=cls)
            return el.get("data-value", "").strip() if el else ""

        raw_date = dv("sort-date")  # YYYYMMDDHHMMSS
        datum = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if len(raw_date) >= 8 else ""
        stof = dv("sort-active-ingredient")
        if not stof:
            continue
        items.append({
            "stof": stof,
            "omschrijving": dv("sort-description"),
            "vorm": dv("sort-form"),
            "preferent": dv("sort-preferential").lower() == "ja",
            "datum": datum,
        })
    return items


def load_eml():
    """EML -> (eml_single: canon-naam->beoordeling, eml_combo: frozenset(componenten)->beoordeling)."""
    eml = pd.read_csv(EML_CSV, sep=";", dtype=str, engine="python").fillna("")
    eml.columns = [c.strip() for c in eml.columns]
    single, combo = {}, {}
    for _, r in eml.iterrows():
        nm = canon(r["Stof Naam"])
        if not nm:
            continue
        b = str(r["Uiteindelijke beoordeling"]).strip().lower()
        single[nm] = worse(single.get(nm, ""), b)
        key = frozenset(comps(r["Stof Naam"]))
        if key:
            combo[key] = worse(combo.get(key, ""), b)
    return single, combo


def load_lcg_bridge():
    """G-standaard stofnaam/omschrijving -> ATC5, als controle-brug voor naamvarianten."""
    try:
        lcg = pd.read_csv(LCG_CSV, usecols=["ATC code", "ATC omschrijving Nederlands", "Werkzame -/hulpstof (stam)"],
                          dtype=str, sep=None, engine="python").fillna("")
    except Exception as e:
        print(f"  (LCG-brug niet beschikbaar: {e})")
        return {}
    m = {}
    for _, r in lcg.iterrows():
        a = canon(r["ATC code"])
        if len(a) != 7:
            continue
        for col in ("ATC omschrijving Nederlands", "Werkzame -/hulpstof (stam)"):
            nm = canon(r[col])
            if nm:
                m.setdefault(nm, a)
    return m


def main():
    items = scrape_farmanco()
    eml_single, eml_combo = load_eml()
    name2atc = load_lcg_bridge()
    eml_atc5 = {}
    for nm, b in eml_single.items():
        a = name2atc.get(nm)
        if a:
            eml_atc5[a] = worse(eml_atc5.get(a, ""), b)

    def eml_beoordeling(stof):
        c = canon(stof)
        if c in eml_single:                       # 1) exacte (komma-genormaliseerde) stofnaam
            return eml_single[c]
        cs = comps(stof)
        key = frozenset(cs)
        if key and key in eml_combo:              # 2) combinatie order-onafhankelijk als EML-entry bestaat
            return eml_combo[key]
        found = []                                # 3) anders: zwaarste beoordeling over losse componenten
        for x in cs + [c]:
            if x in eml_single:
                found.append(eml_single[x])
            else:
                a = name2atc.get(x)               #    incl. ATC5-brug (controle) voor naamvarianten
                if a and a in eml_atc5:
                    found.append(eml_atc5[a])
        return reduce(worse, found) if found else None

    out, n_overig = [], 0
    for it in items:
        if "GEEN WERKZAAM" in it["stof"].upper():
            continue
        b = eml_beoordeling(it["stof"])           # None = niet op EMS -> "overig" (eml leeg)
        if b is None:
            n_overig += 1
        cands = [canon(it["stof"])] + comps(it["stof"])
        atc = next((name2atc[c] for c in cands if c in name2atc), "")
        out.append({**it, "eml": b or "", "atc": atc})

    seen, uniq = set(), []
    for m in out:
        k = (m["stof"], m["omschrijving"], m["datum"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(m)
    uniq.sort(key=lambda m: m["datum"], reverse=True)

    payload = {
        "generated": datetime.date.today().isoformat(),
        "source": FARMANCO_URL,
        "total_scraped": len(items),
        "count": len(uniq),
        "on_eml": sum(1 for m in uniq if m["eml"]),
        "overig": sum(1 for m in uniq if not m["eml"]),
        "items": uniq,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=0)

    from collections import Counter
    n_pref = sum(1 for m in uniq if m["preferent"])
    n_eml = sum(1 for m in uniq if m["eml"])
    print(f"Farmanco gescrapet: {len(items)} meldingen; behouden: {len(uniq)} "
          f"(op EMS: {n_eml}, overig: {len(uniq) - n_eml}, preferent: {n_pref}) -> {OUT}")
    print(f"  EMS-verdeling: {dict(Counter(m['eml'] or 'overig' for m in uniq))}")


if __name__ == "__main__":
    main()
