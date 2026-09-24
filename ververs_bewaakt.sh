#!/bin/bash
# Bewaakte verversing: draait ververs_alles.sh en publiceert ALLEEN als de uitkomst klopt.
#
# Bedoeld om onbeheerd te draaien (launchd, zie com.lcg.tekortendashboard.plist). Daarom zit
# hier een rem op die er bij handmatig draaien niet hoeft te zijn: niemand kijkt om 04:12 mee.
#
# De vraag die dit script beantwoordt is niet "is de scrape geslaagd" maar "is de nieuwe kaart
# geloofwaardiger dan de huidige". Een scraper die stilletjes de helft van een land laat vallen
# meldt namelijk netjes succes -- dat is deze zomer negen keer gebeurd. Vandaar dat de toets
# naar de UITKOMST kijkt en die vergelijkt met wat er nu live staat.
#
#   bash ververs_bewaakt.sh              # verversen, toetsen, publiceren als het klopt
#   bash ververs_bewaakt.sh --droog      # alles doen behalve publiceren
set -u

BASE="${LCG_BASE:-/Users/karkara/Documents/LCG/Landkaart}"
export LCG_BASE="$BASE"
export PATH="${PATH:-}:/Users/karkara/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
DROOG=0
[ "${1:-}" = "--droog" ] && DROOG=1

cd "$BASE" || exit 1
mkdir -p logs
DATUM=$(date +%Y-%m-%d)
LOG="$BASE/logs/bewaakt_$DATUM.log"
STATUS="$BASE/logs/laatste_run.txt"
# Zowel naar het logbestand als naar de standaarduitvoer. Alleen naar het bestand is op de
# Mac prima, maar in GitHub Actions blijft het stapvenster dan leeg en is een mislukte run
# niet te lezen zonder het artefact te downloaden.
exec > >(tee -a "$LOG") 2>&1

melding() {                       # verschijnt in het Berichtencentrum van macOS
  [ -x /usr/bin/osascript ] || return 0        # geen macOS (bv. GitHub Actions)
  /usr/bin/osascript -e "display notification \"$2\" with title \"LCG-tekortendashboard\" subtitle \"$1\"" 2>/dev/null || true
}

echo "=================================================="
echo "BEWAAKTE VERVERSING  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================="

# Momentopname van de huidige (= live) kaart, als ijkpunt voor de toets straks.
VOOR="$BASE/logs/_voor_$DATUM.json"
cp data.json "$VOOR" 2>/dev/null || { echo "FOUT: geen data.json om mee te vergelijken"; exit 1; }

# --- de eigenlijke verversing; publiceert zelf NIET -------------------------
bash ververs_alles.sh
UITKOMST=$?
echo; echo "ververs_alles.sh eindigde met code $UITKOMST"

# --- toets ------------------------------------------------------------------
# Elke regel geeft GOED of een reden. Faalt er een, dan publiceren we niet.
OORDEEL=$(uv run --python 3.13 python - "$VOOR" "$BASE/data.json" <<'PY'
import json, sys, collections

voor = json.load(open(sys.argv[1])); na = json.load(open(sys.argv[2]))
rv, rn = voor["records"], na["records"]
cv = collections.Counter(x["cc"] for x in rv)
cn = collections.Counter(x["cc"] for x in rn)
av = sum(1 for x in rv if x["st"] == "active")
an = sum(1 for x in rn if x["st"] == "active")

bezwaren = []
def eis(goed, tekst):
    if not goed:
        bezwaren.append(tekst)

# 1. Geen land mag verdwijnen. Het vangnet in _merge_live hoort dit al op te vangen;
#    gebeurt het toch, dan is er iets structureel mis.
weg = sorted(set(cv) - set(cn))
eis(not weg, f"landen verdwenen: {weg}")

# 2. Geen land mag meer dan de helft van zijn records verliezen. Dit is de toets die de
#    stille veldverliezen van deze zomer zou hebben gevangen.
gekrompen = [f"{cc} {cv[cc]}->{cn.get(cc,0)}" for cc in cv if cn.get(cc, 0) < cv[cc] * 0.5]
eis(not gekrompen, f"land meer dan gehalveerd: {gekrompen}")

# 3. Het totaal mag niet meer dan een kwart afwijken, omhoog noch omlaag.
eis(len(rn) >= len(rv) * 0.75, f"totaal te sterk gedaald: {len(rv)} -> {len(rn)}")
eis(len(rn) <= len(rv) * 1.25, f"totaal te sterk gestegen: {len(rv)} -> {len(rn)}")

# 4. Het aantal ACTIEVE meldingen is het cijfer waar de vergadering naar kijkt.
eis(an >= av * 0.6, f"actieve meldingen gekelderd: {av} -> {an}")
eis(an <= av * 1.4, f"actieve meldingen te sterk gestegen: {av} -> {an}")

# 4b. De registerval, per land. Dit is de fout die deze zomer het vaakst is voorgekomen en
#     die een globale marge NIET vangt: een bron gaat ineens ook niet-tekorten meetellen,
#     waardoor het aandeel actieve meldingen van dat land omhoog schiet. Zo telden 8.215
#     normaal geleverde Japanse producten als tekort, en 207 leverbare Oostenrijkse
#     verpakkingen. Een land mag best 100% actief zijn -- maar niet ineens.
def aandeel(recs, cc):
    van = [x for x in recs if x["cc"] == cc]
    return (sum(1 for x in van if x["st"] == "active") / len(van)) if van else 0.0

#     Twee voorwaarden samen, en de tweede is essentieel. Een stijgend AANDEEL alleen is
#     geen bewijs: bij Saoedi-Arabie ging het van 40% naar 74% doordat de nieuwe SFDA-bron
#     geen datums publiceert, waardoor oude meldingen niet meer naar 'inactief' verouderen.
#     Er kwamen daar 30 actieve meldingen bij op 625 -- geen registerval maar een schralere
#     bron. Bij een echte registerval stijgt het AANTAL actieve meldingen juist hard: Japan
#     kreeg er in een klap 8.215 bij. Daarom eisen we allebei.
def actief(recs, cc):
    return sum(1 for x in recs if x["cc"] == cc and x["st"] == "active")

sprong = []
for cc in sorted(set(cv) & set(cn)):
    if cn[cc] < 50:                      # te weinig records voor een betekenisvol aandeel
        continue
    was, wordt = aandeel(rv, cc), aandeel(rn, cc)
    av_cc, an_cc = actief(rv, cc), actief(rn, cc)
    meer_actief = an_cc > max(25, av_cc * 1.25)
    if wordt - was > 0.30 and meer_actief:
        sprong.append(f"{cc} {was:.0%}->{wordt:.0%} en {av_cc}->{an_cc} actieve meldingen")
eis(not sprong, f"aandeel EN aantal actieve meldingen springen omhoog (registerval?): {sprong}")
#     GRENS VAN DEZE TOETS, eerlijk opgeschreven: getest op nagebootste storingen pakt hij een
#     val van 6.000 records wel en een van 400 niet (die blijft onder de 30 procentpunt). Dit
#     is een publicatierem tegen zichtbare rampen, geen kwaliteitsaudit. Een sluipende
#     verslechtering van een paar honderd records ziet niemand hier -- daarvoor blijft
#     periodiek met de hand kijken nodig.

# 5. Een land met minder dan tien records is vrijwel altijd een kapotte scraper.
mager = sorted(cc for cc, n in cn.items() if n < 10)
eis(not mager, f"landen met minder dan 10 records: {mager}")

# 6. Duplicaten: die zijn er eerder 17.088 geweest.
sleutels = [tuple(sorted((k, str(v)) for k, v in x.items())) for x in rn]
dubbel = len(sleutels) - len(set(sleutels))
eis(dubbel == 0, f"{dubbel} duplicaten")

# 7. Uitgesloten landen mogen niet ongemerkt terugkeren. CH om de licentie, PT omdat de bron
#    structureel te weinig meldt, LT/TR/ZA/KR/TW omdat het registers zijn en geen tekortenlijst,
#    GB omdat het in geen enkele verversingslijst stond en daardoor bevroor. Komt er een land
#    terug doordat iemand het bewust weer opneemt, dan hoort die persoon deze regel OOK aan te
#    passen -- dat is de bedoeling, niet een obstakel. Sluipt het terug via een CSV die in
#    output/ is blijven liggen, dan vangt deze toets het.
verboden = sorted({"CH", "PT", "LT", "TR", "ZA", "KR", "TW", "NL", "EU", "GB"} & set(cn))
eis(not verboden, f"uitgesloten landen staan er weer in: {verboden}")

print("SAMENVATTING|"
      f"{len(rv)}->{len(rn)} records, {len(cv)}->{len(cn)} landen, {av}->{an} actief")
print("GOED" if not bezwaren else "FOUT|" + " ; ".join(bezwaren))
PY
)

SAMENVATTING=$(echo "$OORDEEL" | grep '^SAMENVATTING|' | cut -d'|' -f2-)
echo; echo "TOETS: $SAMENVATTING"

# Hoeveel bronnen hebben het gehaald? Dit ontbrak, en de eerste echte run liet meteen zien
# waarom het nodig is: achttien van de dertig scrapers vielen om op netwerkfouten, de merge
# vulde alles keurig aan uit de vorige live-data, en de toets keurde goed. De cijfers zagen
# er plausibel uit omdat ze grotendeels van gisteren waren. Een verversing die niets ververst
# hoort niet gepubliceerd te worden.
# Alleen DEZE run tellen: het logboek is per dag en wordt aangevuld, dus bij een tweede run
# op dezelfde dag zou je de bronnen van de eerste meetellen en altijd ruim boven de drempel
# uitkomen. Vanaf de laatste startmarkering lezen.
VERSLOG="$BASE/logs/ververs_$DATUM.log"
if [ -f "$VERSLOG" ]; then
  VANAF=$(grep -n "VERVERSING GESTART" "$VERSLOG" | tail -1 | cut -d: -f1)
  VANAF=${VANAF:-1}
  GELUKT=$(tail -n +"$VANAF" "$VERSLOG" | grep -cE '^OK ' || true)
  GEFAALD=$(tail -n +"$VANAF" "$VERSLOG" | grep -cE '^(ERR|TIME) ' || true)
  TOTAAL=$((GELUKT + GEFAALD))
  echo "bronnen: $GELUKT geslaagd, $GEFAALD mislukt"
  if [ "$TOTAAL" -gt 0 ] && [ "$GELUKT" -lt $((TOTAAL * 2 / 3)) ]; then
    echo "NIET GEPUBLICEERD -- maar $GELUKT van de $TOTAAL bronnen geslaagd"
    echo "$(date '+%F %T')  NIET GEPUBLICEERD  $SAMENVATTING  ::  slechts $GELUKT/$TOTAAL bronnen geslaagd" > "$STATUS"
    melding "Niet gepubliceerd" "slechts $GELUKT van $TOTAAL bronnen geslaagd"
    rm -f "$VOOR"
    exit 1
  fi
fi

# Een verwerkende stap die omviel maakt de cijfers onbetrouwbaar, ook als ze plausibel ogen:
# een omgevallen build_tab3_data laat de kaart ongemoeid maar bevriest de EMS-beoordeling.
FOUTEN="$BASE/logs/_kritieke_fouten.txt"
if [ -s "$FOUTEN" ]; then
  MISLUKT=$(tr '\n' ' ' < "$FOUTEN")
  echo "NIET GEPUBLICEERD -- kritieke stappen mislukt: $MISLUKT"
  echo "$(date '+%F %T')  NIET GEPUBLICEERD  $SAMENVATTING  ::  kritieke stappen mislukt: $MISLUKT" > "$STATUS"
  melding "Niet gepubliceerd" "kritieke stappen mislukt: $MISLUKT"
  rm -f "$VOOR"
  exit 1
fi

if ! echo "$OORDEEL" | grep -q '^GOED$'; then
  REDEN=$(echo "$OORDEEL" | grep '^FOUT|' | cut -d'|' -f2-)
  echo "NIET GEPUBLICEERD -- $REDEN"
  echo "$(date '+%F %T')  NIET GEPUBLICEERD  $SAMENVATTING  ::  $REDEN" > "$STATUS"
  melding "Niet gepubliceerd" "$REDEN"
  rm -f "$VOOR"
  exit 1
fi

if [ "$DROOG" = "1" ]; then
  echo "TOETS GESLAAGD, maar --droog: niet gepubliceerd."
  echo "$(date '+%F %T')  DROOG, toets geslaagd  $SAMENVATTING" > "$STATUS"
  melding "Droge run geslaagd" "$SAMENVATTING"
  rm -f "$VOOR"
  exit 0
fi

# --- publiceren -------------------------------------------------------------
# De vier tab-3-bestanden staan er expliciet bij. Ze werden wel ververst maar nooit
# gepubliceerd: ze ontbraken in deze lijst, dus vroegsignalering.html laadde maandenlang
# verouderde Farmanco-, CBG- en SFK-gegevens terwijl de run als geslaagd gold.
# analyse/ staat er expliciet bij: panel.csv groeit alleen als hij elke run wordt
# meegecommit. Zonder die map blijft het panel een nulmeting en komt het volgorde-onderzoek
# nooit van de grond.
# dossiers.json staat er expliciet bij: zonder dat blijft de Dossierstatuspagina op de
# ingebakken terugvallijst hangen, ook al is hij netjes opgehaald.
git add -A data.json atc4_dekking.json sfk_verloop.json sfk_tekorten.json eml_atc5.json cbg_tav_eml.json farmanco_eml.json sfk_historie.json dossiers.json analyse/ output/ 2>/dev/null
if git diff --cached --quiet; then
  echo "Niets veranderd; niets te publiceren."
  echo "$(date '+%F %T')  GEEN WIJZIGING  $SAMENVATTING" > "$STATUS"
  melding "Geen wijziging" "$SAMENVATTING"
else
  git commit -q -m "Dataverversing $DATUM

$SAMENVATTING" && git push -q origin HEAD
  if [ $? -eq 0 ]; then
    echo "LIVE GEZET."
    echo "$(date '+%F %T')  LIVE  $SAMENVATTING" > "$STATUS"
    melding "Live gezet" "$SAMENVATTING"
  else
    echo "PUBLICEREN MISLUKT (commit of push)."
    echo "$(date '+%F %T')  PUSH MISLUKT  $SAMENVATTING" > "$STATUS"
    melding "Publiceren mislukt" "controleer $LOG"
    rm -f "$VOOR"
    exit 1
  fi
fi

rm -f "$VOOR"
# Logs ouder dan 60 dagen opruimen, anders groeit deze map ongemerkt aan.
find "$BASE/logs" -name "*.log" -mtime +60 -delete 2>/dev/null
echo "KLAAR  $(date '+%Y-%m-%d %H:%M:%S')"
