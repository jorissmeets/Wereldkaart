#!/usr/bin/env python3
"""Keur de doorgevoerde ATC-correcties met TypeSafe, oud tegen nieuw.

Waarom RELATIEF en niet tegen een vaste drempel: een noul geeft een kans, en die kans
verschilt per vraagsoort en zelfs per stof (obscure stofnamen scoren structureel lager,
ook als het antwoord klopt). Een vaste grens van 0,50 zou daardoor juist de moeilijke
gevallen wegfilteren. Dezelfde vraag twee keer stellen -- eenmaal met de oude code,
eenmaal met de nieuwe -- haalt die gevoeligheid eruit: het enige dat telt is welke van
de twee de keurmeester waarschijnlijker vindt.

  python3 keur_correcties.py            # rapporteren
  python3 keur_correcties.py --apply    # afgekeurde correcties terugdraaien
"""
import csv
import glob
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/Users/karkara/Documents/LCG/Matchen_prk")
os.environ.setdefault("MATCH_LLM", "typesafe")
import enrich_atc_llm as e  # noqa: E402

csv.field_size_limit(10_000_000)
BASE = "/Users/karkara/Documents/LCG/Landkaart"
# Verschil waaronder we het oordeel niet vertrouwen: scoren beide codes vrijwel gelijk,
# dan zegt de keurmeester feitelijk "ik weet het niet" en laten we de correctie staan.
MARGE = 0.15


def main():
    apply = "--apply" in sys.argv
    correcties = json.load(open("/tmp/correcties.json"))
    print(f"{len(correcties)} unieke correcties te keuren ({2 * len(correcties)} vragen)\n")

    paren = []
    for c in correcties:
        paren.append((f"{c['stof']}||oud", c["oud"]))
        paren.append((f"{c['stof']}||nieuw", c["nieuw"]))

    # _ts_verify gebruikt de stofnaam als sleutel; het achtervoegsel houdt oud en nieuw
    # uit elkaar zonder de vraag zelf te veranderen (de stofnaam gaat mee in de state,
    # dus die moet schoon zijn -- daarom splitsen we hem hieronder weer af).
    def keur(p):
        sleutel, code = p
        stof = sleutel.split("||")[0]
        return sleutel, e._ts_verify([(stof, code)]).get(stof, -1.0)

    scores = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sleutel, kans in ex.map(keur, paren):
            scores[sleutel] = kans

    beter = slechter = gelijk = onbepaald = 0
    terug = []
    for c in correcties:
        po = scores.get(f"{c['stof']}||oud", -1.0)
        pn = scores.get(f"{c['stof']}||nieuw", -1.0)
        if po < 0 or pn < 0:
            onbepaald += 1
            continue
        if pn - po > MARGE:
            beter += 1
        elif po - pn > MARGE:
            slechter += 1
            terug.append({**c, "p_oud": round(po, 2), "p_nieuw": round(pn, 2)})
        else:
            gelijk += 1

    print(f"nieuwe code duidelijk beter : {beter}")
    print(f"vrijwel gelijk (laten staan): {gelijk}")
    print(f"OUDE code was beter         : {slechter}")
    print(f"geen oordeel                : {onbepaald}")

    if terug:
        print("\n== AFGEKEURDE CORRECTIES (oud was beter) ==")
        for t in sorted(terug, key=lambda x: x["p_nieuw"] - x["p_oud"]):
            no = e.ATC2NAME.get(t["oud"], "?")
            nn = e.ATC2NAME.get(t["nieuw"], "?")
            print(f"  {t['cc']} {t['stof'][:30]:<32} {t['oud']}={no[:18]:<18} (p={t['p_oud']})"
                  f"  ->  {t['nieuw']}={nn[:18]:<18} (p={t['p_nieuw']})")

    if apply and terug:
        herstel = {(t["stof"].upper(), t["nieuw"]): t["oud"] for t in terug}
        n = 0
        for pad in sorted(glob.glob(os.path.join(BASE, "output", "*_shortage_*.csv"))):
            try:
                rows = list(csv.DictReader(open(pad, encoding="utf-8", errors="ignore")))
            except Exception:
                continue
            if not rows:
                continue
            k = {c.lower(): c for c in rows[0].keys()}
            ca, cs = k.get("atc_code"), k.get("active_substance")
            if not ca or not cs:
                continue
            raak = False
            for r in rows:
                sl = ((r.get(cs) or "").strip().upper(), (r.get(ca) or "").strip().upper())
                if sl in herstel:
                    r[ca] = herstel[sl]
                    raak = True
                    n += 1
            if raak:
                with open(pad, "w", newline="", encoding="utf-8") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                    w.writeheader()
                    w.writerows(rows)
        print(f"\nteruggedraaid: {n} rijen")
    elif terug:
        print("\n(niets gewijzigd — draai met --apply om deze terug te draaien)")


if __name__ == "__main__":
    main()
