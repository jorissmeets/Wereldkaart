"""Merge: verse rerun-data (landkaart/data.json) UNIE last-live-data (backup).
- Verse records hebben voorrang (verse datums).
- Verse records zonder PRK erven de PRK van het matchende last-live-record.
- Last-live records die niet in vers zitten worden toegevoegd (niets verloren).
Herberekent de indices en schrijft naar root data.json."""
import json, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lcg_paden import BASE as _B

FRESH = os.path.join(_B, "landkaart", "data.json")
# Bevroren momentopname van de live kaart op 21-09-2026, als vangnet voor landen waarvan
# de scraper faalt. Stond eerder in /private/tmp; dat wordt door macOS opgeruimd en bestaat
# helemaal niet op een andere machine, waardoor de wekelijkse run stilletjes zou breken.
OLD = os.path.join(_B, "referentie", "lastlive_2026-09-21.json")
OUT = os.path.join(_B, "data.json")

_ws = re.compile(r"\s+")


def norm(s):
    return _ws.sub(" ", str(s or "").upper()).strip()


def key(r):
    return (r.get("cc"), (r.get("atc") or "").upper(), norm(r.get("mn") or r.get("sub") or ""))


fresh = json.load(open(FRESH))
old = json.load(open(OLD))
fr = fresh["records"]
orl = old["records"]

# Vormcontrole bij het lenen: zie vorm.py (gedeeld met toets_prk_atc.py).
from vorm import vorm_botst  # noqa: E402


def stofsleutel(r):
    """Land + molecuul + STOFNAAM, zonder de productnaam.

    De gewone sleutel valt terug op mn (de productnaam) en pas daarna op sub. Levert een bron
    ineens alleen nog stofnamen, dan vergelijkt hij "Hydralazine Sciegen" met "HYDRALAZINE" en
    matcht er niets meer. Zo verloor Saoedi-Arabie in een klap 638 PRK-koppelingen: de nieuwe
    SFDA-export heeft bij alle 539 records een lege productnaam. Beide kanten hebben wel een
    stofnaam, dus daar valt op terug te vallen.
    """
    return (r.get("cc"), (r.get("atc") or "").upper(), norm(r.get("sub") or ""))


# PRK lenen: eerste last-live-record per sleutel dat een PRK heeft
old_prk = {}
old_stof = {}
for r in orl:
    if r.get("prk"):
        old_prk.setdefault(key(r), (r.get("prk"), r.get("prk_naam")))
        old_stof.setdefault(stofsleutel(r), set()).add((r.get("prk"), r.get("prk_naam")))

# Tweede kans alleen waar het ONDUBBELZINNIG is: geeft dezelfde stof in hetzelfde land
# meerdere PRK's, dan weten we niet welke en lenen we niets. Dat slaat ~1.100 gevallen over
# en levert er ~770 op -- de juiste kant om die afweging te laten vallen, want een verkeerde
# PRK wijst een specifiek Nederlands product als geraakt aan.
old_stof = {k: next(iter(v)) for k, v in old_stof.items() if len(v) == 1}

fresh_keys = set()
borrowed = 0
borrowed_stof = 0
geweigerd_vorm = 0
for r in fr:
    k = key(r)
    fresh_keys.add(k)
    if r.get("prk"):
        continue
    bron = old_prk.get(k)
    if bron is None:
        bron = old_stof.get(stofsleutel(r))
        if bron is not None:
            borrowed_stof += 1
    else:
        borrowed += 1
    if bron is not None and vorm_botst(r, bron[1]):
        # Een lening die over de vormgrens gaat is erger dan geen lening: hij wijst een
        # specifiek Nederlands product aan dat niet geraakt is.
        geweigerd_vorm += 1
        bron = None
    if bron is not None:
        r["prk"], nm = bron[0], bron[1]
        if nm:
            r["prk_naam"] = nm

# Unie op ATC×land: voeg last-live-records alleen toe voor een molecuul-in-land dat in
# vers HELEMAAL ontbreekt (voorkomt dubbele meldingen door naamvarianten; vers is leidend
# voor moleculen die het al dekt).
fresh_ccatc = {(r.get("cc"), (r.get("atc") or "").upper()) for r in fr}
# Niet via last-live terughalen. Twee redenen om hier in te staan:
#  1. Van de kaart gehaald (registratieregister i.p.v. tekortmeldingen, of geen tekortbron).
#  2. Bron dit keer GEREPAREERD: de verse scrape is de volledige actuele lijst, dus oude
#     records terughalen zou verouderde meldingen naast de nieuwe zetten. Bij CH bijvoorbeeld
#     122 oude records zonder enige datum naast 831 nieuwe MET datum; bij GR de aprillijst
#     naast de augustuslijst, waardoor allang opgeloste tekorten actief blijven staan.
# Landen waarvan de scraper juist FAALDE (CA, MY) horen hier NIET in: daar is last-live het
# enige wat we hebben.
EXCLUDE = {"NL", "EU", "LT", "TR", "ZA", "KR", "TW", "PT",      # geen bruikbare tekortbron
           "CH",   # licentievoorbehoud in de bron; zie build_data.py
           "GB",   # stond in geen enkele verversingslijst en bevroor daardoor; zie build_data.py
           "GR", "DE", "JP", "AT",                              # bron gerepareerd, vers is leidend
           # AT: de datums stonden als XML-ATTRIBUTEN op <Packung> en werden daardoor nooit
           # gelezen (shortage_start stond hard op ""). Nu gerepareerd. De last-live-records
           # zijn exact de datumloze versie van die bug; terughalen zou ze naast de gerepareerde
           # regels zetten. De verse export is bovendien het volledige actuele register.
           # SK: de oude export telde elke melding sinds 2019 als actief door een omgekeerde
           # tiebreak op de aanmelddatum. MY: het oude endpoint maakte gevulde broncellen leeg
           # (datums verdwenen stil). In beide gevallen is de verse data de gecorrigeerde.
           # CA staat hier BEWUST NIET: de Tier 3-terugval levert maar 26 meldingen, terwijl
           # last-live er 1.222 actieve heeft. Die zijn oud (maart) maar wel echt.
           "SK", "MY",
           # SA stond hier ook, maar is teruggehaald: de nieuwe SFDA-bron is een verschraling
           # (alleen stofnaam, geen ATC/datum/productnummer). Uitsluiten kostte 1.260 records
           # en 804 PRK-koppelingen. Vers blijft leidend waar het dekking heeft; voor moleculen
           # die het niet dekt vult last-live aan.
           "EE"}    # filterfout hersteld (Mõlemad -> Tarneraskusega); oude 400 records waren
                    # vervuild met 'marketing beëindigd' en mogen niet terugkomen
# ── Alleen aanvullen voor landen waarvan deze run GEEN verse scrape heeft ──────────
# Het vangnet werkte per molecuul, ook als het land zelf compleet binnenkwam. Dan haalt het
# tekorten terug die de bron inmiddels heeft laten vallen, en omdat de momentopname
# bevroren is, blijven ze voor altijd staan. Op 30-09 kwamen zo 2.659 meldingen op de live
# kaart uit geen enkele verse scrape, waarvan 1.444 als actief. Spaanse capecitabine was er
# een van: CIMA had hem al laten vallen, onze scrape ook, de kaart toonde hem nog.
# Een land met een verse scrape van deze run is compleet; alleen landen waarvan de scrape
# faalde vallen nog terug op de momentopname.
#
# Maar "vers" is niet "van vandaag". Viel een bron een dag uit (OGYEI en ANM op 01-10), dan
# gebruikt build_data gewoon het complete bestand van gisteren -- en vulde deze stap dat land
# daarna toch aan uit 21-09: 51 spookmeldingen voor Hongarije, 38 als actief. Dat is precies
# het mechanisme van 30-09, alleen via de achterdeur van een mislukte scrape. _dedup_output
# laat per bron een bestand staan, en dat is het bestand dat build_data leest. Is dat nieuwer
# dan de momentopname, dan weet de momentopname niets wat dat bestand niet ook weet.
SNAPDAG = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(OLD)).group(1)

def _verse_landen():
    import glob
    nieuwste = {}
    for p in glob.glob(os.path.join(_B, "output", "*_shortage_*.csv")):
        m = re.search(r"/([A-Z]{2})_[^/]*_shortage_(\d{4}-\d{2}-\d{2})\.csv$", p)
        if m and m.group(2) > nieuwste.get(m.group(1), ""):
            nieuwste[m.group(1)] = m.group(2)
    if not nieuwste:
        return set(), set(), None
    # De rundatum komt van buiten als die bekend is; anders de jongste datum in output/.
    # Een run die over middernacht loopt, schrijft al zijn bestanden met de startdatum.
    dag = os.environ.get("LCG_DATUM") or max(nieuwste.values())
    vandaag = {cc for cc, d in nieuwste.items() if d == dag}
    na_snap = {cc for cc, d in nieuwste.items() if d > SNAPDAG}
    return na_snap, vandaag, dag

VERS_DEZE_RUN, VANDAAG_GESCRAPET, RUNDAG = _verse_landen()
# Canada is de uitzondering, en een bewuste. De Tier 3-terugval levert maar een handvol
# meldingen terwijl de momentopname er ruim duizend heeft. Die zijn oud (maart) en deels
# opgelost -- precies Jespers klacht -- maar zonder account is er niets beters. Zolang dat
# zo is, blijft Canada aanvullen; zet hem hier weg zodra er een volledige bron is.
ALTIJD_AANVULLEN = {"CA"}
merged = list(fr)
added = 0
overgeslagen_vers = 0
aangevuld_landen = set()
for r in orl:
    cc = r.get("cc")
    if cc in EXCLUDE:
        continue
    if cc in VERS_DEZE_RUN and cc not in ALTIJD_AANVULLEN:
        overgeslagen_vers += 1
        continue
    if (cc, (r.get("atc") or "").upper()) not in fresh_ccatc:
        merged.append(r)
        added += 1
        aangevuld_landen.add(cc)

# Ontdubbelen NA de samenvoeging. build_data ontdubbelt zijn eigen uitvoer al, maar de
# last-live-data komt uit een eerdere draai die dat nog niet deed; die sleept zijn kopieen
# hierheen. Zonder deze stap telt de kaart bronregels in plaats van meldingen -- precies de
# vertekening die bij Canada 13.608 niet te onderscheiden kopieen opleverde.
_voor = len(merged)
_gezien, _uniek = set(), []
for _r in merged:
    _k = tuple(sorted((k, str(v)) for k, v in _r.items()))
    if _k not in _gezien:
        _gezien.add(_k)
        _uniek.append(_r)
merged = _uniek
if _voor != len(merged):
    print(f"ontdubbeld na samenvoegen: {_voor - len(merged)} identieke records verwijderd")

# Indices herberekenen
from collections import Counter
ccs = sorted({r["cc"] for r in merged})
atcs = sorted({r["atc"] for r in merged})
acc = {}
for r in merged:
    acc.setdefault(r["atc"], set()).add(r["cc"])
acc = {k: len(v) for k, v in acc.items()}

out = dict(fresh)  # behoud generated, ems_rood_atcs, reason_labels, etc.
out["records"] = merged
out["monitored_countries"] = ccs
out["total_countries"] = len(ccs)
out["total_atc"] = len(atcs)
out["atc_country_count"] = acc

json.dump(out, open(OUT, "w"), ensure_ascii=False, separators=(",", ":"))
prk_n = sum(1 for r in merged if r.get("prk"))
print(f"vers: {len(fr)} | last-live: {len(orl)} | toegevoegd uit last-live: {added} | MERGED: {len(merged)}")
print(f"landen: {len(ccs)} | ATC5: {len(atcs)} | met PRK: {prk_n} | PRK geleend: {borrowed} op naam + {borrowed_stof} op stofnaam"
      f" | geweigerd op vorm: {geweigerd_vorm}")
print(f"verse scrape van {RUNDAG}: {len(VANDAAG_GESCRAPET)} landen; met een bestand nieuwer dan de momentopname"
      f" ({SNAPDAG}): {len(VERS_DEZE_RUN)} -> niet aangevuld ({overgeslagen_vers} oude records overgeslagen)")
_terug = sorted(VERS_DEZE_RUN - VANDAAG_GESCRAPET)
if _terug:
    print(f"  scrape van vandaag ontbreekt, eerder compleet bestand gebruikt: {_terug}")
if aangevuld_landen:
    print(f"  wel aangevuld (geen verse scrape, of bewust): {sorted(aangevuld_landen)}")
