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

# PRK lenen: eerste last-live-record per sleutel dat een PRK heeft
old_prk = {}
for r in orl:
    if r.get("prk"):
        old_prk.setdefault(key(r), (r.get("prk"), r.get("prk_naam")))

fresh_keys = set()
borrowed = 0
for r in fr:
    k = key(r)
    fresh_keys.add(k)
    if not r.get("prk") and k in old_prk:
        r["prk"], nm = old_prk[k][0], old_prk[k][1]
        if nm:
            r["prk_naam"] = nm
        borrowed += 1

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
merged = list(fr)
added = 0
for r in orl:
    if r.get("cc") in EXCLUDE:
        continue
    if (r.get("cc"), (r.get("atc") or "").upper()) not in fresh_ccatc:
        merged.append(r)
        added += 1

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
print(f"landen: {len(ccs)} | ATC5: {len(atcs)} | met PRK: {prk_n} | PRK geleend: {borrowed}")
