#!/usr/bin/env python3
"""Bouw/actualiseer de persistente PRK-koppeltabel uit de al-gematchte output-CSV's.

Sleutel per melding = stabiele land:productcode (product_no / licence / ndc / registration),
met content-hash (stof+sterkte+vorm+ATC) als fallback voor landen zonder code.
Draai NA de PRK-match: dan zit de LLM-match (van deze week) in de tabel en hoeft die
volgende weken niet opnieuw. De tabel is een auditbare mapping land:code -> PRK.

  uv run --python 3.12 python build_koppeltabel.py
"""
import glob, csv, re, hashlib, json
csv.field_size_limit(10_000_000)

OUT = "prk_koppeltabel.csv"
# Kolommen met een stabiele interne productcode, in prioriteitsvolgorde
CODE_COLS = ["product_no", "licence_number", "license_number", "registration_number",
             "registration_no", "package_ndc", "product_ndc", "application_number", "company_license_no"]
ATC5 = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")
PRK_NAMEN = json.load(open("landkaart/prk_namen.json"))


def stable_key(row):
    for c in CODE_COLS:
        v = (row.get(c) or "").strip()
        if v:
            return f"code:{c}:{v}"
    base = "|".join((row.get(k) or "").strip().upper() for k in ("medicine_name", "strength", "dosage_form", "atc_code"))
    return "hash:" + hashlib.md5(base.encode("utf-8")).hexdigest()[:12]


def load_existing():
    table = {}
    try:
        with open(OUT, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                table[(r["cc"], r["sleutel"])] = (r["prk"], r.get("prk_naam", ""))
    except FileNotFoundError:
        pass
    return table


def main():
    table = load_existing()
    before = len(table)
    for f in sorted(glob.glob("output/*_shortage_*.csv")):
        cc = f.split("/")[-1].split("_")[0]
        if cc in ("NL", "EU"):
            continue
        with open(f, encoding="utf-8", errors="ignore") as fh:
            for row in csv.DictReader(fh):
                atc = (row.get("atc_code") or "").strip().upper()
                if not ATC5.match(atc):
                    continue                                  # alleen matchbare (ATC) rijen; ook geen-match vastleggen
                prk = re.sub(r"\.0$", "", (row.get("prk") or "").strip())
                key = (cc, stable_key(row))
                old = table.get(key)
                if old is None or (not old[0] and prk):       # niet-lege PRK wint bij botsing
                    table[key] = (prk, PRK_NAMEN.get(prk, "") if prk else "")
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["cc", "sleutel", "prk", "prk_naam"])
        for (cc, key), (prk, naam) in sorted(table.items()):
            w.writerow([cc, key, prk, naam])
    print(f"koppeltabel: {len(table)} land:code -> PRK ({len(table)-before} nieuw) -> {OUT}")


if __name__ == "__main__":
    main()
