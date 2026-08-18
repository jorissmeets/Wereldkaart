#!/usr/bin/env python3
"""Verrijk de Vroegsignalering-bronnen CBG-TAV en Farmanco met een G-standaard PRK.

Deterministisch (geen AI):
  • CBG-TAV : RVG-nummer (item['reg']) -> PRK via LCG.csv 'Registratienummer'.
  • Farmanco: omschrijving -> PRK via LCG.csv 'PRK omschrijving' (genormaliseerd,
              met fallback op het deel vóór de eerste komma).

Voegt per item 'prk' + 'prk_naam' toe (alleen bij match). Schrijft de JSON's terug.
Draai dit na de tab-3-scrapers in de wekelijkse refresh.

  uv run --python 3.12 --with pandas python enrich_prk_tab3.py
"""
import json, re, pandas as pd
from pathlib import Path

LCG = "/Users/karkara/Documents/LCG/Matchen_prk/LCG.csv"
HERE = Path(__file__).resolve().parent


def norm_oms(s):
    """Uppercase, witruimte inklappen, spaties rond '/' weg (Farmanco: '5/ 80' -> '5/80')."""
    s = re.sub(r"\s*/\s*", "/", str(s).upper())
    return re.sub(r"\s+", " ", s).strip()


def build_maps():
    z = pd.read_csv(LCG, encoding="ISO-8859-1", delimiter=";",
                    usecols=["Registratienummer", "PRK code", "PRK omschrijving"], dtype=str).fillna("")
    z["PRK code"] = z["PRK code"].str.replace(r"\.0$", "", regex=True).str.strip()
    z = z[z["PRK code"] != ""]
    rvg2prk, oms2prk = {}, {}
    for _, r in z.iterrows():
        prk, oms = r["PRK code"], r["PRK omschrijving"]
        rvg = r["Registratienummer"].strip()
        if rvg and rvg not in rvg2prk:
            rvg2prk[rvg] = (prk, oms)
        k = norm_oms(oms)
        if k and k not in oms2prk:
            oms2prk[k] = (prk, oms)
    return rvg2prk, oms2prk


def enrich_cbg(rvg2prk):
    p = HERE / "cbg_tav_eml.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    hit = 0
    for it in d["items"]:
        m = rvg2prk.get(str(it.get("reg", "")).strip())
        if m:
            it["prk"], it["prk_naam"] = m
            hit += 1
        else:
            it.pop("prk", None); it.pop("prk_naam", None)
    p.write_text(json.dumps(d, ensure_ascii=False, indent=0), encoding="utf-8")
    print(f"CBG-TAV: {hit}/{len(d['items'])} met PRK ({100*hit//max(len(d['items']),1)}%)")


def enrich_farmanco(oms2prk):
    p = HERE / "farmanco_eml.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    hit = 0
    for it in d["items"]:
        oms = it.get("omschrijving", "")
        m = oms2prk.get(norm_oms(oms))
        if not m and "," in oms:                       # fallback: deel vóór de eerste komma
            m = oms2prk.get(norm_oms(oms.split(",")[0]))
        if m:
            it["prk"], it["prk_naam"] = m
            hit += 1
        else:
            it.pop("prk", None); it.pop("prk_naam", None)
    p.write_text(json.dumps(d, ensure_ascii=False, indent=0), encoding="utf-8")
    print(f"Farmanco: {hit}/{len(d['items'])} met PRK ({100*hit//max(len(d['items']),1)}%)")


def main():
    rvg2prk, oms2prk = build_maps()
    print(f"LCG-lookups: {len(rvg2prk)} RVG's, {len(oms2prk)} omschrijvingen")
    enrich_cbg(rvg2prk)
    enrich_farmanco(oms2prk)


if __name__ == "__main__":
    main()
