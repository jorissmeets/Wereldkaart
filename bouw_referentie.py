#!/usr/bin/env python3
"""Maak een slanke referentie-extractie uit de G-standaard.

WAAROM
De pijplijn moet in GitHub Actions kunnen draaien in plaats van op een laptop die om 04:12
aan moet staan. Daarvoor moet de referentiedata mee, en LCG.csv is 32 MB waarvan we vier
kolommen gebruiken. Deze extractie houdt alleen die kolommen over: ongeveer 5 MB.

Er speelt ook iets anders. LCG.csv is een momentopname van april en gstandaard_actueel.csv
is de actuele export, maar die laatste mist juist het Registratienummer (RVG/EU-nummer) dat
build_tab3_data.py nodig heeft om CBG-meldingen te koppelen. Zolang die kolom niet in de
actuele export zit, blijft dit bestand de enige bron ervoor -- en dat is meteen de reden om
het expliciet te bouwen in plaats van stilzwijgend een oud bestand te blijven lezen.

De uitvoer gaat naar referentie/, dat in .gitignore staat: de G-standaard is Z-Index/KNMP-
data en de publieke repo is publiek. In de PRIVATE pijplijnrepo hoort dit bestand wel thuis.

    uv run --python 3.13 python bouw_referentie.py
"""
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE, PRK  # noqa: E402

BRON = os.environ.get("LCG_GSTD_VOL") or os.path.join(PRK, "LCG.csv")
DOEL = os.path.join(BASE, "referentie", "gstd_rvg.csv")

# Precies wat build_tab3_data.py leest, plus ZI-nummer als sleutel voor eventueel later.
KOLOMMEN = [
    "ZI-nummer",
    "Registratienummer",
    "ATC code",
    "ATC omschrijving Nederlands",
    "Werkzame -/hulpstof (stam)",
]


def main() -> None:
    if not os.path.exists(BRON):
        raise SystemExit(f"bron niet gevonden: {BRON}\n"
                         f"Zet LCG_GSTD_VOL als de volledige G-standaard elders staat.")
    os.makedirs(os.path.dirname(DOEL), exist_ok=True)

    with open(BRON, encoding="ISO-8859-1", newline="") as f:
        lezer = csv.DictReader(f, delimiter=";")
        ontbreekt = [k for k in KOLOMMEN if k not in (lezer.fieldnames or [])]
        if ontbreekt:
            raise SystemExit(f"kolommen ontbreken in {BRON}: {ontbreekt}")
        rijen = [{k: (r.get(k) or "").strip() for k in KOLOMMEN} for r in lezer]

    # Ontdubbelen: LCG.csv staat op artikelniveau, wij hebben alleen de koppeling nodig.
    gezien, uniek = set(), []
    for r in rijen:
        sleutel = tuple(r[k] for k in KOLOMMEN)
        if sleutel not in gezien:
            gezien.add(sleutel)
            uniek.append(r)

    with open(DOEL, "w", encoding="ISO-8859-1", newline="", errors="replace") as f:
        w = csv.DictWriter(f, fieldnames=KOLOMMEN, delimiter=";")
        w.writeheader()
        w.writerows(uniek)

    mb_in = os.path.getsize(BRON) / 1024 / 1024
    mb_uit = os.path.getsize(DOEL) / 1024 / 1024
    print(f"{BRON}\n  {len(rijen)} rijen, {mb_in:.0f} MB")
    print(f"{DOEL}\n  {len(uniek)} unieke rijen, {mb_uit:.1f} MB")
    met_reg = sum(1 for r in uniek if r["Registratienummer"])
    print(f"  met Registratienummer: {met_reg}/{len(uniek)}")


if __name__ == "__main__":
    main()
