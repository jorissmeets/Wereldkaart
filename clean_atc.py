#!/usr/bin/env python3
"""Extraheer een schone ATC-5 uit een rommelige atc_code-kolom.

Sommige feeds zetten de ATC niet als kale code neer maar als 'L01EX09 Nintedanib'
(BE) of als dict-string "{'atcCode8': 'B01AC06', ...}" (SE). Eén regex vangt beide:
de eerste ATC-5 (letter+2cijfers+2letters+2cijfers) in de tekst. Overschrijft atc_code
met die schone code (of leeg). Deterministisch, geen AI.

  python3 clean_atc.py <output/XX_.._shortage_YYYY-MM-DD.csv>
"""
import sys, csv, re
csv.field_size_limit(10_000_000)
FIND = re.compile(r"[A-Z]\d{2}[A-Z]{2}\d{2}")


def main():
    path = sys.argv[1]
    rows = list(csv.DictReader(open(path, encoding="utf-8", errors="ignore")))
    if not rows:
        print(f"{path}: leeg"); return
    cols = {c.lower(): c for c in rows[0].keys()}
    ac = cols.get("atc_code") or cols.get("atc")
    if not ac:
        print(f"{path}: geen atc-kolom"); return
    changed = clean = 0
    for r in rows:
        v = (r.get(ac) or "").strip()
        m = FIND.search(v.upper())
        new = m.group(0) if m else ""
        if new != v:
            changed += 1
        r[ac] = new
        if new:
            clean += 1
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"{path.split('/')[-1]}: {clean}/{len(rows)} schone ATC-5 ({changed} aangepast)")


if __name__ == "__main__":
    main()
