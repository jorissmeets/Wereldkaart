#!/usr/bin/env python3
"""Keur de bestaande atc_llm_cache opnieuw met de TypeSafe-poort en herstel wat fout is.

De cache bewaart ATC's die ooit door gpt-4o-mini zijn afgeleid op basis van zijn EIGEN
confidence. Die zelfbeoordeling bleek onbetrouwbaar: in een steekproef van 40 was ~43%
fout, bijna altijd volgens hetzelfde patroon (juiste ATC-klasse, verkeerd molecuul, vooral
bij Latijnse/Japanse/Scandinavische stofnamen). Omdat de pipeline de cache VOOR de LLM
leest, blijven die fouten staan tot ze opnieuw worden afgeleid.

Twee stappen, zodat er geen OpenAI-budget wordt verbrand aan regels die al kloppen:
  1. keuren  — elke (stof, atc) door de noul-poort. Kost alleen TypeSafe-calls.
  2. opnieuw afleiden — alleen de afgekeurde regels via de volledige cascade
     (gpt-4o-mini -> poort -> o4-mini -> poort). Wat ook dan niet door de poort komt,
     wordt LEEG gezet: een tekort onder het verkeerde molecuul is erger dan geen koppeling.

  python3 recheck_atc_cache.py            # alleen rapporteren (niets wijzigen)
  python3 recheck_atc_cache.py --apply    # herstellen, met back-up van de cache
"""
import csv
import os
import shutil
import sys
from datetime import datetime

sys.path.insert(0, "/Users/karkara/Documents/LCG/Matchen_prk")
os.environ.setdefault("MATCH_LLM", "typesafe")
import enrich_atc_llm as e  # noqa: E402


def main():
    apply = "--apply" in sys.argv
    limit = next((int(a.split("=")[1]) for a in sys.argv if a.startswith("--limit=")), 0)

    rows = list(csv.DictReader(open(e.CACHE, encoding="utf-8")))
    gevuld = [r for r in rows if r["atc"].strip() and r["key"].startswith("S:")]
    if limit:
        gevuld = gevuld[:limit]
    print(f"cache: {len(rows)} regels, {len(gevuld)} met ATC -> keuren (poort {e.TS_ACCEPT})")

    # --- 1. keuren -----------------------------------------------------------
    paren = [(r["key"][2:], r["atc"].strip()) for r in gevuld]
    probs = e._ts_verify(paren, workers=8)
    verdacht = [(s, a) for s, a in paren if probs.get(s, -1.0) < e.TS_ACCEPT]
    print(f"door de poort : {len(paren) - len(verdacht)}/{len(paren)}")
    print(f"afgekeurd     : {len(verdacht)}/{len(paren)} ({100 * len(verdacht) // max(len(paren), 1)}%)")

    if not verdacht:
        print("niets te herstellen")
        return
    if not apply:
        print("\n(dry run — geen wijzigingen. Draai met --apply om te herstellen.)")
        print("\nvoorbeelden van afgekeurde regels:")
        for s, a in verdacht[:20]:
            print(f"  p={probs[s]:.2f}  {s[:44]:<44} -> {a}  (code staat voor: {e.ATC2NAME.get(a, '?')})")
        return

    # --- 2. alleen de afgekeurde opnieuw afleiden ----------------------------
    print(f"\nopnieuw afleiden via de cascade: {len(verdacht)} stoffen")
    hersteld = e.resolve_atc([s for s, _ in verdacht], "HERSTEL")

    cache = {r["key"]: r["atc"] for r in rows}
    gewijzigd = geleegd = bevestigd = 0
    for stof, oud in verdacht:
        nieuw = hersteld.get(stof, "")
        cache["S:" + stof] = nieuw
        if not nieuw:
            geleegd += 1
        elif nieuw != oud:
            gewijzigd += 1
        else:
            bevestigd += 1

    back_up = f"{e.CACHE}.bak-{datetime.now():%Y-%m-%d_%H%M}"
    shutil.copy2(e.CACHE, back_up)
    with open(e.CACHE, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["key", "atc"])
        for k, v in sorted(cache.items()):
            w.writerow([k, v])

    print(f"\nandere code : {gewijzigd}")
    print(f"leeggemaakt : {geleegd}")
    print(f"bevestigd   : {bevestigd}")
    print(f"back-up     : {back_up}")
    print("\nLET OP: de landcsv's en data.json zijn hiermee nog NIET bijgewerkt — "
          "draai de enrich-stap en build_data opnieuw om dit in de kaart te krijgen.")


if __name__ == "__main__":
    main()
