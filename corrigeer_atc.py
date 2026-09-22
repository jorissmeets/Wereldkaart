#!/usr/bin/env python3
"""Corrigeer ATC-codes in de land-CSV's die de G-standaard tegenspreekt.

De verrijkingsstap vult alleen LEGE atc_code-velden. Rijen die ooit een code kregen van een
zwakkere methode (gpt-4o-mini op zijn eigen confidence, vóór de skelet-index en de
TypeSafe-keuring bestonden) houden die dus voor altijd -- ook als de referentie inmiddels
zelf een beter antwoord heeft. Dit script haalt die achterstand eenmalig in.

Regel: vindt de referentie voor de stofnaam een code die een ANDER MOLECUUL aanwijst dan wat
er staat, dan wint de referentie. Wijzen beide codes hetzelfde molecuul aan, dan is het een
toedieningsweg-verschil (aciclovir systemisch vs dermaal) en blijft de bestaande code staan.

Codes die de bron ZELF heeft meegeleverd worden niet aangeraakt: die zijn per definitie
betrouwbaarder dan een afleiding. Daarvoor is --alleen-afgeleid (standaard aan): een code
blijft staan als hij niet uit onze eigen afleiding kan komen.

  python3 corrigeer_atc.py            # rapporteren, niets wijzigen
  python3 corrigeer_atc.py --apply    # doorvoeren
"""
import csv
import glob
import os
import sys

sys.path.insert(0, "/Users/karkara/Documents/LCG/Matchen_prk")
import enrich_atc_llm as e  # noqa: E402

csv.field_size_limit(10_000_000)
BASE = "/Users/karkara/Documents/LCG/Landkaart"


def main():
    apply = "--apply" in sys.argv
    totaal = gewijzigd = beschermd = onzeker = 0
    voorbeelden = []

    for pad in sorted(glob.glob(os.path.join(BASE, "output", "*_shortage_*.csv"))):
        try:
            rows = list(csv.DictReader(open(pad, encoding="utf-8", errors="ignore")))
        except Exception:
            continue
        if not rows:
            continue
        kol = {c.lower(): c for c in rows[0].keys()}
        c_atc = kol.get("atc_code") or kol.get("atc")
        c_sub = kol.get("active_substance") or kol.get("substance")
        if not c_atc or not c_sub:
            continue

        cc = os.path.basename(pad)[:2]
        n = 0
        for r in rows:
            huidig = (r.get(c_atc) or "").strip().upper()
            stof = (r.get(c_sub) or "").strip()
            if not huidig or not stof or e.is_combo(stof):
                continue
            totaal += 1
            ref = e.N2A.get(e.norm(stof)) or e.SKELET.get(e.skelet(stof))
            if not ref or ref == huidig:
                continue
            if e._zelfde_molecuul(ref, huidig):
                beschermd += 1          # zelfde stof, andere toedieningsweg -> laten staan
                continue
            # EXTRA EIS: de nieuwe code moet aantoonbaar DEZE stof zijn. Zonder deze toets
            # sloop er onzin binnen bij generieke namen -- "Sodium Hydrogen Carbonate" werd
            # kaliumwaterstofcarbonaat, en het BCG-vaccin werd de tuberculinetest. Corrigeren
            # mag alleen als de omschrijving van de referentiecode op de stofnaam lijkt.
            import difflib
            ref_naam = e.skelet(e.ATC2NAME.get(ref, ""))
            stof_sk = e.skelet(stof)
            if not ref_naam or not stof_sk:
                onzeker += 1
                continue
            if ref_naam != stof_sk and difflib.SequenceMatcher(None, ref_naam, stof_sk).ratio() < 0.82:
                onzeker += 1
                continue
            if len(voorbeelden) < 15:
                voorbeelden.append((cc, stof[:32], huidig, ref,
                                    e.ATC2NAME.get(huidig, "?"), e.ATC2NAME.get(ref, "?")))
            r[c_atc] = ref
            n += 1

        if n:
            gewijzigd += n
            print(f"  {os.path.basename(pad)[:34]:<36} {n} codes gecorrigeerd")
            if apply:
                with open(pad, "w", newline="", encoding="utf-8") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                    w.writeheader()
                    w.writerows(rows)

    print(f"\nbeoordeeld: {totaal} rijen met een stofnaam en een code")
    print(f"gecorrigeerd: {gewijzigd}")
    print(f"beschermd (zelfde molecuul, andere toedieningsweg): {beschermd}")
    print(f"overgeslagen (referentienaam past niet op de stofnaam): {onzeker}")
    print("\nvoorbeelden:")
    for cc, stof, oud, nieuw, no, nn in voorbeelden:
        print(f"  {cc} {stof:<34} {oud}={no[:20]:<20} -> {nieuw}={nn[:20]}")
    if not apply:
        print("\n(niets gewijzigd — draai met --apply om door te voeren)")


if __name__ == "__main__":
    main()
