#!/usr/bin/env python3
"""Bouw name2atc.json (stofnaam -> ATC5) + atc_valid.json (geldige ATC5-codes)
uit de WHO-ATC-index + de G-standaard. Basis voor deterministische ATC-verrijking
van landen die wel een stofnaam maar geen ATC publiceren.

  uv run --python 3.12 --with pandas python build_name2atc.py
"""
import pandas as pd, re, json

WHO = "/Users/karkara/Documents/LCG/medicatie_matcher_ref/assets/pharma/who_atc_index.csv"
LCG = "/Users/karkara/Documents/LCG/Matchen_prk/LCG.csv"
SALT = r"\b(HYDROCHLORIDE|HYDROCHLORIDU?M|SODIUM|NATRIUM|SULFATE|SULFAS|MESILATE|MESYLATE|MALEATE|MALEAS|CITRATE|ACETATE|SUCCINATE|TARTRATE|FUMARATE|BESILATE|HEMIFUMARATE|DIHYDRATE|MONOHYDRATE|HYDRATE|POTASSIUM|CALCIUM|PHOSPHATE|CHLORIDE)\b"


def norm(s):
    s = re.split(r"[,/(]", str(s).upper())[0]
    s = re.sub(SALT, "", s)
    return re.sub(r"\s+", " ", s).strip()


def main():
    n2a, valid = {}, set()
    w = pd.read_csv(WHO, dtype=str).fillna("")
    namecol = [c for c in w.columns if "name" in c.lower()][0]
    atccol = [c for c in w.columns if "atc" in c.lower() or c.lower() == "code"][0]
    for _, r in w.iterrows():
        a = r[atccol].strip()
        if len(a) == 7:
            valid.add(a)
            k = norm(r[namecol])
            if k:
                n2a.setdefault(k, a)
    z = pd.read_csv(LCG, encoding="latin-1", delimiter=";",
                    usecols=["Werkzame -/hulpstof (stam)", "ATC omschrijving Nederlands", "ATC omschrijving Engels", "ATC code"], dtype=str).fillna("")
    for _, r in z.iterrows():
        a = r["ATC code"].strip()
        if len(a) != 7:
            continue
        valid.add(a)
        for col in ("Werkzame -/hulpstof (stam)", "ATC omschrijving Nederlands", "ATC omschrijving Engels"):
            k = norm(r[col])
            if k:
                n2a.setdefault(k, a)
    json.dump(n2a, open("name2atc.json", "w"), ensure_ascii=False)
    json.dump(sorted(valid), open("atc_valid.json", "w"))
    print(f"name2atc.json: {len(n2a)} namen | atc_valid.json: {len(valid)} geldige ATC5-codes")


if __name__ == "__main__":
    main()
