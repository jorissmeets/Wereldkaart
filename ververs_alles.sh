#!/bin/bash
# Volledige dataverversing van het tekortendashboard, in één opdracht.
#
# Bedoeld voor de tweedaagse cadans die het LCG wil: de kaart wordt donderdag bij de
# geneesmiddelbespreking gebruikt, dus uiterlijk DINSDAG moet er een geslaagde run zijn.
#
#   bash ververs_alles.sh            # alles verversen, NIET publiceren
#   bash ververs_alles.sh --deploy   # verversen en daarna live zetten
#
# Publiceren gebeurt alleen met --deploy: een mislukte scrape mag nooit vanzelf de live
# kaart overschrijven. De merge met de vorige live-data vangt uitval per land op, maar dat
# vangnet werkt alleen als iemand de cijfers nog even bekijkt.
set -u

BASE=/Users/karkara/Documents/LCG/Landkaart
PRKD=/Users/karkara/Documents/LCG/Matchen_prk
LOGDIR="$BASE/logs"
DATUM=$(date +%Y-%m-%d)
LOG="$LOGDIR/ververs_$DATUM.log"
UV="uv run --python 3.13 --with typesafe-sdk --with python-dotenv --with openai --with pandas --with requests --with beautifulsoup4 --with lxml --with openpyxl --with xlrd --with pdfplumber"

# De landen die daadwerkelijk op de kaart staan. Bewust niet de volledige scraperlijst:
# LT/TR/EE-oud/ZA/KR/TW zijn registers of geen tekortbron, die worden in build_data uitgesloten.
LANDEN="AT AU BE BG CA CH CO CZ DE DK EE ES FI FR GR HR HU IE IS IT JP LV MY NO PT RO SA SE SI SK US"

export MATCH_LLM=typesafe
export PRK_MIN_CONFIDENCE=90
export ATC_ROUTE_CONFIDENCE=101   # model-gebaseerde toedieningswegkeuze staat uit: niet geijkt

mkdir -p "$LOGDIR"
cd "$BASE" || exit 1
exec > >(tee -a "$LOG") 2>&1

echo "=================================================="
echo "VERVERSING GESTART  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================="

# --- 1. Buitenlandse bronnen ------------------------------------------------
echo; echo "### 1. Scrapen ($(echo $LANDEN | wc -w | tr -d ' ') landen)"
$UV python _rerun_targeted.py $LANDEN

# --- 2. Per land: ATC opschonen, verrijken, PRK koppelen --------------------
echo; echo "### 2. Verrijken en PRK koppelen"
for f in output/*_shortage_"$DATUM".csv; do
  [ -e "$f" ] || { echo "  geen verse bestanden voor $DATUM"; break; }
  CC=$(basename "$f" | cut -c1-2)
  echo "--- $CC ---"
  if [ "$CC" = "DK" ]; then
    # DK propt de ATC in de stofnaam ("Loratadin ATC-kode: R06AX13"); clean_atc zou hier de
    # veterinaire Q-prefix strippen, dus die regex draait apart.
    $UV python -c "
import csv, re
p = '$f'
rows = list(csv.DictReader(open(p, encoding='utf-8')))
pat = re.compile(r'ATC-kode:\s*(Q?[A-Z]\d{2}[A-Z]{2}\d{2})')
n = 0
for r in rows:
    if not (r.get('atc_code') or '').strip():
        m = pat.search(r.get('active_substance') or '')
        if m:
            r['atc_code'] = m.group(1); n += 1
w = csv.DictWriter(open(p, 'w', newline='', encoding='utf-8'), fieldnames=list(rows[0].keys()))
w.writeheader(); w.writerows(rows)
print(f'  ATC uit stofnaam gehaald: {n}')
"
  else
    $UV python clean_atc.py "$f" 2>&1 | tail -1
  fi
  $UV python "$PRKD/enrich_atc_llm.py" "$f" "$CC" 2>&1 | tail -3
  ( cd "$PRKD" && $UV python prk_match_country.py "$BASE/$f" "$CC" 2>&1 | tail -1 )
done

# --- 3. Nederlandse bronnen -------------------------------------------------
# Deze horen bij ELKE verversing mee: ze zijn voor het LCG het meest direct relevant.
echo; echo "### 3. Nederlandse bronnen (CBG / Farmanco / SFK)"
$UV python build_tab3_data.py 2>&1 | tail -2      # CBG tijdelijk afwijkende verpakking + EML
$UV python scrape_farmanco.py 2>&1 | tail -2
$UV python scrape_sfk.py 2>&1 | tail -2
# Historie van de SFK-monitor bijwerken. Haalt alleen de NIEUWE week op; de rest staat
# in sfk_historie_cache/. Zonder dit blijft het PRK-verloop staan op de laatste draai.
$UV python scrape_sfk_historie.py 2>&1 | tail -3

# --- 4. Ontdubbelen en kaart bouwen ----------------------------------------
echo; echo "### 4. Ontdubbelen en bouwen"
$UV python _dedup_output.py 2>&1 | tail -2
$UV python landkaart/build_data.py 2>&1 | tail -4

# --- 5. Samenvoegen met de vorige live-data --------------------------------
# Vangnet: een land waarvan de scraper faalde zakt zo niet weg uit de kaart.
echo; echo "### 5. Samenvoegen met vorige live-data"
$UV python _merge_live.py 2>&1 | tail -2

# --- 6. ATC4-dekking (als dat script bestaat) ------------------------------
[ -f build_atc4_dekking.py ] && { echo; echo "### 6. ATC4-dekking"; $UV python build_atc4_dekking.py 2>&1 | tail -3; }

# --- 7. Controle vóór publicatie -------------------------------------------
echo; echo "### 7. Controle"
$UV python -c "
import json
d = json.load(open('data.json'))
r = d['records']
import collections
c = collections.Counter(x['cc'] for x in r)
prk = sum(1 for x in r if x.get('prk'))
print(f'  {len(r)} records | {len(c)} landen | {prk} met PRK')
leeg = [k for k, v in c.items() if v < 10]
if leeg: print(f'  LET OP, landen met minder dan 10 records: {leeg}')
"

if [ "${1:-}" = "--deploy" ]; then
  echo; echo "### 8. Publiceren"
  git add -A data.json atc4_dekking.json landkaart.html output/ 2>/dev/null
  git commit -q -m "Dataverversing $DATUM" && git push -q origin HEAD && echo "  live gezet"
else
  echo; echo "NIET gepubliceerd. Controleer de cijfers en draai daarna:"
  echo "  cd $BASE && git add -A data.json output/ && git commit -m 'Dataverversing $DATUM' && git push"
fi

echo; echo "KLAAR  $(date '+%Y-%m-%d %H:%M:%S')  ->  $LOG"
