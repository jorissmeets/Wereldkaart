#!/usr/bin/env python3
"""Bouwt de EML-gekoppelde data voor het Vroegsignalering-dashboard (tab 3):

  - cbg_tav_eml.json : CBG "Tijdelijk Afwijkende Verpakking" (live gescrapet),
                       elk besluit gekoppeld aan de Essential Medicines List via
                       RVG-nummer -> ATC (G-standaard/LCG.csv) -> EML-beoordeling.
  - eml_atc5.json    : ATC5 -> EML-beoordeling (rood/oranje/geel), voor de
                       landkaart-watchlist (die per ATC koppelt).

Farmanco gaat via scrape_farmanco.py (aparte scraper).

Draaien:
  uv run --python 3.12 --with requests --with beautifulsoup4 --with lxml --with pandas \
    python build_tab3_data.py
"""
import requests, re, json, datetime
from bs4 import BeautifulSoup
import pandas as pd

CBG_URL = "https://www.cbg-meb.nl/onderwerpen/handelsvergunning-productinformatie-vereisten/hv-tijdelijk-afwijkende-verpakking"
EML_CSV = "LijstenEMS/Stofnamen 2025-Tabel 1.csv"
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from lcg_paden import BASE as _B
# Slanke extractie: alleen hier zit het Registratienummer (RVG/EU) dat de actuele
# G-standaard-export niet levert. Zie bouw_referentie.py.
LCG_CSV = _os.path.join(_B, "referentie", "gstd_rvg.csv")
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
SEV = {"geel": 1, "oranje": 2, "rood": 3}
MND = {"januari": "01", "februari": "02", "maart": "03", "april": "04", "mei": "05", "juni": "06",
       "juli": "07", "augustus": "08", "september": "09", "oktober": "10", "november": "11", "december": "12"}


def canon(s):
    return re.sub(r"\s+", " ", str(s).upper().replace(",", " ")).strip()


def digits(s):
    return re.sub(r"\D", "", str(s))


def worse(a, b):
    return a if SEV.get(a, 0) >= SEV.get(b, 0) else b


def scrape_cbg_tav():
    html = requests.get(CBG_URL, headers=UA, timeout=40).text
    soup = BeautifulSoup(html, "lxml")
    year = month = None
    out = []
    for el in soup.find_all(["h2", "h3", "h4", "p"]):
        t = el.get_text(" ", strip=True)
        if el.name == "h2":
            m = re.search(r"Overzicht besluiten (\d{4})", t)
            if m:
                year, month = m.group(1), None
        elif el.name == "h3":
            if t.strip().lower() in MND:
                month = t.strip().lower()
        elif el.name == "h4" and year and month:
            nxt = el.find_next("p")
            det = nxt.get_text(" ", strip=True) if nxt else ""
            if "Handelsvergunninghouder" not in det:
                continue
            mah = re.search(r"Handelsvergunninghouder:\s*(.*?)\s*(?:RVG-nummer|EU-nummer|Datum van goedkeuring)", det)
            num = re.search(r"(RVG-nummer|EU-nummer):\s*(.*?)\s*(?:Datum van goedkeuring|N\.B\.|$)", det)
            dat = re.search(r"datum van goedkeuring:\s*(\d{1,2})\s+([a-z]+)\s+(\d{4})", det.lower())
            datum = f"{dat.group(3)}-{MND.get(dat.group(2),'00')}-{int(dat.group(1)):02d}" if dat else ""
            out.append({
                "product": t,
                "mah": mah.group(1).strip() if mah else "",
                "reg": num.group(2).strip() if num else "",
                "reg_type": num.group(1).replace("-nummer", "") if num else "",
                "datum": datum,
            })
    # dedup
    seen, uniq = set(), []
    for e in out:
        k = (e["product"], e["reg"], e["datum"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(e)
    return uniq


def load_maps():
    eml = pd.read_csv(EML_CSV, sep=";", dtype=str, engine="python").fillna("")
    eml.columns = [c.strip() for c in eml.columns]
    eml_name = {}
    for _, r in eml.iterrows():
        nm = canon(r["Stof Naam"])
        if nm:
            eml_name[nm] = worse(eml_name.get(nm, ""), str(r["Uiteindelijke beoordeling"]).strip().lower())
    lcg = pd.read_csv(LCG_CSV, usecols=["Registratienummer", "ATC code", "ATC omschrijving Nederlands",
                                        "Werkzame -/hulpstof (stam)"], dtype=str, sep=None, engine="python").fillna("")
    name2atc, rvg2atc = {}, {}
    for _, r in lcg.iterrows():
        a = canon(r["ATC code"])
        if len(a) != 7:
            continue
        for col in ("ATC omschrijving Nederlands", "Werkzame -/hulpstof (stam)"):
            nm = canon(r[col])
            if nm:
                name2atc.setdefault(nm, a)
        reg = str(r["Registratienummer"]).strip()
        if reg:
            rvg2atc.setdefault(canon(reg), a)
            d = digits(reg)
            if d:
                rvg2atc.setdefault("D" + d, a)
    eml_atc5 = {}
    for nm, b in eml_name.items():
        a = name2atc.get(nm)
        if a:
            eml_atc5[a] = worse(eml_atc5.get(a, ""), b)
    return eml_name, name2atc, rvg2atc, eml_atc5


def main():
    today = datetime.date.today().isoformat()
    eml_name, name2atc, rvg2atc, eml_atc5 = load_maps()

    # eml_atc5.json (voor de watchlist)
    with open("eml_atc5.json", "w", encoding="utf-8") as f:
        json.dump({"generated": today, "count": len(eml_atc5), "map": eml_atc5}, f, ensure_ascii=False)
    print(f"eml_atc5.json: {len(eml_atc5)} ATC5 -> beoordeling")

    # CBG TAV, gekoppeld aan EML
    tav = scrape_cbg_tav()

    def tav_atc(e):
        for k in (canon(e["reg"]), "D" + digits(e["reg"])):
            if k in rvg2atc:
                return rvg2atc[k]
        # fallback: eerste 1-2 woorden van de productnaam als stofnaam
        toks = re.split(r"\s+", e["product"].strip())
        for c in ([canon(toks[0] + " " + toks[1])] if len(toks) > 1 else []) + [canon(toks[0])]:
            if c in name2atc:
                return name2atc[c]
        return None

    dated = [e for e in tav if e["datum"]]
    n_eml = 0
    for e in dated:
        a = tav_atc(e)
        e["atc"] = a or ""
        e["eml"] = eml_atc5.get(a, "") if a else ""
        if e["eml"]:
            n_eml += 1
    dated.sort(key=lambda e: e["datum"], reverse=True)
    with open("cbg_tav_eml.json", "w", encoding="utf-8") as f:
        json.dump({"generated": today, "source": CBG_URL, "count": len(dated),
                   "on_eml": n_eml, "items": dated}, f, ensure_ascii=False, indent=0)
    from collections import Counter
    print(f"cbg_tav_eml.json: {len(dated)} TAV-besluiten (gedateerd); op EML: {n_eml} "
          f"({dict(Counter(e['eml'] for e in dated if e['eml']))})")


if __name__ == "__main__":
    main()
