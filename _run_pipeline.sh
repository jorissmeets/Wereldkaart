#!/bin/bash
# Vervolgstappen na de scrape: ATC opschonen -> verrijken -> PRK koppelen -> ontdubbelen.
# Draait per land zodat een stukgelopen land de rest niet meesleept.
#
#   bash _run_pipeline.sh <datum>       bv. bash _run_pipeline.sh 2026-09-21
set -u
DATUM="${1:?geef de scrape-datum, bv 2026-09-21}"
BASE=/Users/karkara/Documents/LCG/Landkaart
PRKD=/Users/karkara/Documents/LCG/Matchen_prk
UV="uv run --python 3.13 --with typesafe-sdk --with python-dotenv --with openai --with pandas --with requests --with beautifulsoup4 --with lxml --with openpyxl"

# TypeSafe-keten aan; de model-gebaseerde toedieningsweg-keuze staat UIT (drempel 101 is
# onhaalbaar) omdat die voor de versmalde vraag nog niet geijkt is. Het deterministische
# routefilter werkt gewoon.
export MATCH_LLM=typesafe
export PRK_MIN_CONFIDENCE=90
export ATC_ROUTE_CONFIDENCE=101

cd "$BASE" || exit 1
for f in output/*_shortage_"$DATUM".csv; do
  [ -e "$f" ] || { echo "geen bestanden voor $DATUM"; exit 1; }
  CC=$(basename "$f" | cut -c1-2)
  echo "=============== $CC ==============="

  # DK niet door clean_atc: die stript de veterinaire Q-prefix (QI06AD07 -> I06AD07).
  if [ "$CC" != "DK" ]; then
    $UV python clean_atc.py "$f" 2>&1 | tail -1
  fi

  $UV python "$PRKD/enrich_atc_llm.py" "$f" "$CC" 2>&1 | tail -3

  ( cd "$PRKD" && $UV python prk_match_country.py "$BASE/$f" "$CC" 2>&1 | tail -2 )
done

echo "=============== ontdubbelen ==============="
$UV python _dedup_output.py 2>&1 | tail -3
echo "klaar: $(date +%H:%M:%S)"
