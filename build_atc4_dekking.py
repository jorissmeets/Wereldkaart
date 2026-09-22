#!/usr/bin/env python3
"""Bouw atc4_dekking.json: per ATC4-pool hoeveel voorschrijfbare producten er nog over zijn.

De vraag die deze pagina moet beantwoorden is niet "hoeveel meldingen zijn er?" maar
"is er binnen deze therapeutische groep nog een alternatief?". Daarom wordt er geteld
in PRK-codes (voorschrijfbaar product: stof + sterkte + vorm) uit de G-standaard, en
niet in meldingen.

Waarom PRK en niet ATC5/ZI/GPK:
  - ATC5: 208 van de 593 pools bevatten maar één ATC5, daar kan het percentage alleen
    0 of 100 zijn. Dat meet niets.
  - ZI/HPK: tellen parallelimport en verpakkingsvarianten mee; het getal meet dan hoe
    versnipperd de generieke markt is, niet hoeveel therapeutische ruimte er over is.
  - GPK: 736 GPK's hangen onder meerdere PRK's, dus geen schone deler.
  - PRK is bovendien de eenheid waarop dit project (SFK, landkaart) al gekoppeld is:
    teller en noemer zitten daarmee in dezelfde eenheid.

Waarom alleen productgroep SPECIALITEES in de noemer: zonder dat filter krijgt J01XA
125 PRK in plaats van 5 (96% apotheekbereidingen) en verdunt elk tekort tot bijna nul.
Bereidingen zijn in de praktijk wel een uitwijk, maar horen niet in dezelfde breuk; ze
komen als losse telling mee (n_bereidingen_prk), nooit in noemer of percentage.

Draaien:
    uv run --python 3.13 python build_atc4_dekking.py

Leest : /Users/karkara/Documents/LCG/Matchen_prk/LCG.csv (G-standaard),
        data.json, sfk_tekorten.json, farmanco_eml.json, eml_atc5.json, atc4_namen.json
Schrijft: atc4_dekking.json
"""
import csv
import json
import os
import sys
import datetime as dt
from collections import defaultdict, Counter

BASE = os.path.dirname(os.path.abspath(__file__))
GSTD = "/Users/karkara/Documents/LCG/Matchen_prk/LCG.csv"
OUT = os.path.join(BASE, "atc4_dekking.json")

# Alleen handelsproducten in de noemer; zie module-docstring.
NOEMER_PRODUCTGROEP = "SPECIALITEES"
# Apotheekbereidingen worden apart geteld en blijven buiten elke breuk.
BEREIDING_PRODUCTGROEPEN = {
    "DOORGELEVERDE BEREIDING",
    "MAGISTRALE RECEPTUUR",
    "INTRAMURALE BEREIDING",
}
# Onder deze poolgrootte zegt een percentage niets: bij 3 PRK springt het in stappen van
# 33 procentpunt. De grens ligt op 5 en niet op 3 omdat er inkoopbeslissingen op volgen.
MIN_PRK_VOOR_PERCENTAGE = 5
# Aantal meldende landen waarboven een buitenlands signaal als pool-signaal telt.
MIN_LANDEN_BUITENLAND = 3
# Venster voor "nieuw": bewust kort, dit is de kolom die op donderdag gelezen wordt.
NIEUW_DAGEN = 7
# Onder deze datumdekking wordt er geen recentheidsgetal getoond.
MIN_DATUMDEKKING = 0.5
# Vroegste datum die we als echte brondatum accepteren; DK levert o.a. '0001-01-01'.
VROEGSTE_PLAUSIBELE_DATUM = "1990-01-01"

EML_RANG = {"rood": 3, "oranje": 2, "geel": 1}


def norm(x):
    return (x or "").strip()


def lees_json(naam):
    with open(os.path.join(BASE, naam), encoding="utf-8") as f:
        return json.load(f)


def ernstigste(kleuren):
    """Zwaarste EMS-kleur uit een verzameling, of None als er geen beoordeling is."""
    best, rang = None, 0
    for k in kleuren:
        r = EML_RANG.get(k, 0)
        if r > rang:
            best, rang = k, r
    return best


# ---------------------------------------------------------------------------
# 1. G-standaard: de noemer
# ---------------------------------------------------------------------------
def lees_gstandaard():
    """Per ATC4-pool de PRK's uit de G-standaard, ontdubbeld op ZI-nummer.

    De G-standaard kent geen uit-de-handel-vlag, dus de noemer bevat een bekende,
    niet te kwantificeren overschatting: producten die formeel nog geregistreerd zijn
    maar feitelijk niet meer geleverd worden, tellen mee als alternatief.
    """
    gezien_zi = set()
    # pool -> prk -> gegevens
    pools = defaultdict(lambda: defaultdict(lambda: {
        "naam": "", "zi": set(), "firmas": set(), "atc5": set()}))
    bereiding_prk = defaultdict(set)
    bereiding_art = Counter()
    atc5_per_pool = defaultdict(set)
    atc5_naam = {}
    atc4_naam = {}
    zi_naar_pool_prk = {}
    stat = Counter()

    with open(GSTD, encoding="ISO-8859-1", newline="") as f:
        rd = csv.DictReader(f, delimiter=";")
        for r in rd:
            stat["regels"] += 1
            zi = norm(r.get("ZI-nummer"))
            if not zi or zi in gezien_zi:
                continue
            gezien_zi.add(zi)
            stat["unieke_zi"] += 1
            atc = norm(r.get("ATC code")).upper()
            # ATC4 (5 tekens) is genoeg om een pool te bepalen. Sommige groepen kennen
            # geen 5e niveau (A12AX calcium met vit. D, B03AC parenteraal ijzer, B05XC
            # vitaminen); die producten staan met een ATC van 5 tekens in de G-standaard.
            # De eis len==7 gooide ze weg, waardoor hun pool helemaal uit dit overzicht
            # verdween - inclusief de Nederlandse tekorten erin.
            if len(atc) not in (5, 7):
                stat["zi_zonder_bruikbare_atc4"] += 1
                continue
            if len(atc) == 5:
                stat["zi_alleen_atc4"] += 1
                nl4 = norm(r.get("ATC omschrijving Nederlands"))
                if nl4:
                    atc4_naam.setdefault(atc, nl4)
            pool = atc[:5]
            pg = norm(r.get("Productgroep omschrijving"))
            if pg in BEREIDING_PRODUCTGROEPEN:
                prk_b = norm(r.get("PRK code"))
                if prk_b:
                    bereiding_prk[pool].add(prk_b)
                bereiding_art[pool] += 1
                continue
            if pg != NOEMER_PRODUCTGROEP:
                stat["zi_buiten_noemer_productgroep"] += 1
                continue
            prk = norm(r.get("PRK code"))
            if not prk:
                stat["zi_zonder_prk"] += 1
                continue
            stat["zi_in_noemer"] += 1
            if len(atc) == 7:
                atc5_per_pool[pool].add(atc)
                nl_naam = norm(r.get("ATC omschrijving Nederlands"))
                if nl_naam:
                    atc5_naam.setdefault(atc, nl_naam)
            p = pools[pool][prk]
            p["zi"].add(zi)
            p["atc5"].add(atc)
            firma = norm(r.get("Handelsvergunning-/registratiehouder"))
            if firma:
                p["firmas"].add(firma)
            if not p["naam"]:
                p["naam"] = norm(r.get("PRK omschrijving"))
            zi_naar_pool_prk[zi] = (pool, prk)

    return {
        "pools": pools,
        "bereiding_prk": bereiding_prk,
        "bereiding_art": bereiding_art,
        "atc5_per_pool": atc5_per_pool,
        "atc5_naam": atc5_naam,
        "atc4_naam": atc4_naam,
        "zi_naar_pool_prk": zi_naar_pool_prk,
        "stat": stat,
    }


# ---------------------------------------------------------------------------
# 2. Buitenlandse meldingen: schoonmaken en per ATC5 samenvatten
# ---------------------------------------------------------------------------
def is_beschikbaarheidsmelding(r):
    """True als deze 'active' regel in werkelijkheid zegt dat het middel WEL beschikbaar is.

    JP publiceert de volledige leveringsstatus van elk product, waarvan 'normal' gewoon
    'geen probleem' betekent. AT doet hetzelfde met 'verfügbar'. Meetellen maakt een
    derde van alle actieve meldingen vals. 'eingeschränkt verfügbar' en 'Nicht verfügbar'
    blijven wel staan: dat zijn echte beperkingen.
    """
    cc = r.get("cc")
    sr = (r.get("sr") or "").strip().lower()
    if cc == "JP" and sr == "normal":
        return True
    if cc == "AT" and sr.startswith("verfügbar"):
        return True
    return False


def bronsignaal(r):
    """Laatste datum die echt van de bron komt, of None.

    Verschillende scrapers vullen lu/ss met de scrapedatum (sa) als de bron zelf geen
    datum geeft; CZ doet dat voor 468 records. Zo'n datum zegt alleen wanneer wij keken,
    niet wanneer er iets gebeurde, en mag dus nooit als recentheid gelden.
    """
    sa = r.get("sa")
    lu, ss = r.get("lu"), r.get("ss")
    if lu and lu != sa:
        return lu
    if ss and ss != sa:
        return ss
    return None


def lees_buitenland(data, peildatum):
    recs = data.get("records") or []
    grens = (dt.date.fromisoformat(peildatum) - dt.timedelta(days=NIEUW_DAGEN)).isoformat()
    landen_per_atc5 = defaultdict(set)
    meld_per_pool = Counter()
    gedateerd_per_pool = Counter()
    nieuw_per_pool = Counter()
    landen_per_pool = defaultdict(set)
    stat = Counter()

    for r in recs:
        st = r.get("st")
        if st not in ("active", "upcoming"):
            continue
        stat["active_upcoming"] += 1
        if is_beschikbaarheidsmelding(r):
            stat["gefilterd_beschikbaar"] += 1
            continue
        stat["schoon"] += 1
        atc = (r.get("atc") or "").upper()
        # zelfde regel als bij de G-standaard: ATC4 volstaat voor de pool. Vandaag raakt
        # dit 0 meldingen, maar zo vallen ATC4-only-meldingen later niet stil weg.
        if len(atc) not in (5, 7):
            stat["zonder_atc4"] += 1
            continue
        pool = atc[:5]
        if len(atc) == 7:
            landen_per_atc5[atc].add(r.get("cc"))
        landen_per_pool[pool].add(r.get("cc"))
        meld_per_pool[pool] += 1
        sig = bronsignaal(r)
        if sig and sig >= VROEGSTE_PLAUSIBELE_DATUM:
            gedateerd_per_pool[pool] += 1
            stat["met_bronsignaal"] += 1
            if grens <= sig <= peildatum:
                nieuw_per_pool[pool] += 1
            elif sig > peildatum:
                # aangekondigde startdatum op een 'upcoming' melding: wel een echte
                # brondatum (telt mee voor de datumdekking), maar geen nieuw signaal
                stat["datum_in_toekomst"] += 1
        elif sig:
            # o.a. DK ss='0001-01-01': geen bruikbare datum, niet als gedateerd tellen
            stat["datum_onbruikbaar"] += 1

    return {
        "landen_per_atc5": landen_per_atc5,
        "landen_per_pool": landen_per_pool,
        "meld_per_pool": meld_per_pool,
        "gedateerd_per_pool": gedateerd_per_pool,
        "nieuw_per_pool": nieuw_per_pool,
        "stat": stat,
    }


# ---------------------------------------------------------------------------
# 3. Bouwen
# ---------------------------------------------------------------------------
def main():
    g = lees_gstandaard()
    data = lees_json("data.json")
    sfk = lees_json("sfk_tekorten.json")
    farm = lees_json("farmanco_eml.json")
    eml = lees_json("eml_atc5.json").get("map", {})
    a4namen = lees_json("atc4_namen.json")

    peildatum = data.get("generated")
    if not peildatum:
        sys.exit("data.json heeft geen 'generated'; zonder peildatum geen recentheid.")

    bui = lees_buitenland(data, peildatum)

    # --- SFK: welke ZI-artikelen staan op de tekortenlijst -------------------
    sfk_items = sfk.get("items") or []
    sfk_zi = {str(i.get("zi")).strip() for i in sfk_items if i.get("zi")}
    sfk_in_noemer = {z for z in sfk_zi if z in g["zi_naar_pool_prk"]}
    sfk_stat = {
        "artikelen": len(sfk_items),
        "unieke_zi": len(sfk_zi),
        # Deze artikelen kunnen nergens meetellen: niet in de G-standaard, of niet in een
        # SPECIALITEES-regel met volledige ATC5. Expliciet melden, niet stil laten vallen.
        "zi_niet_in_noemer": len(sfk_zi) - len(sfk_in_noemer),
    }

    # --- Farmanco: blijft op ATC5-niveau ------------------------------------
    # 0 van de 291 Farmanco-items heeft een prk-veld, dus Farmanco kan niet in de
    # PRK-teller. Raden zou de teller grover maken dan de noemer.
    farm_items = farm.get("items") or []
    farm_atc5_per_pool = defaultdict(set)
    farm_kleur_per_pool = defaultdict(set)
    farm_zonder_atc = 0
    # Farmanco heeft per melding een datum. Zonder die datum toont de recentheidskolom
    # alleen buitenlandse meldingen, en leest een pool met een gloednieuw NEDERLANDS
    # tekort als "0 nieuw" - precies verkeerd om voor de donderdagbespreking.
    farm_grens = (dt.date.fromisoformat(peildatum) - dt.timedelta(days=NIEUW_DAGEN)).isoformat()
    farm_nieuw_per_pool = defaultdict(list)
    for i in farm_items:
        a = (i.get("atc") or "").upper()
        if len(a) != 7:
            farm_zonder_atc += 1
            continue
        farm_atc5_per_pool[a[:5]].add(a)
        if i.get("eml"):
            farm_kleur_per_pool[a[:5]].add(i["eml"])
        dat = (i.get("datum") or "").strip()
        if dat and dat >= farm_grens:
            farm_nieuw_per_pool[a[:5]].append({"stof": i.get("stof"), "datum": dat,
                                               "atc": a, "eml": i.get("eml") or None})
    for v in farm_nieuw_per_pool.values():
        v.sort(key=lambda x: (x["datum"], x["stof"] or ""), reverse=True)
    farm_stat = {
        "items": len(farm_items),
        "zonder_bruikbare_atc5": farm_zonder_atc,
        "met_prk_veld": sum(1 for i in farm_items if i.get("prk")),
        "met_datum": sum(1 for i in farm_items if (i.get("datum") or "").strip()),
        "nieuw_in_venster": sum(len(v) for v in farm_nieuw_per_pool.values()),
    }

    # --- Pools samenstellen -------------------------------------------------
    alle_pools = set(g["pools"]) | set(bui["meld_per_pool"]) | set(farm_atc5_per_pool)
    uit = []
    for pool in sorted(alle_pools):
        prks = g["pools"].get(pool, {})
        in_noemer = bool(prks)

        prk_rows = []
        n_weg = n_geraakt = n_solo = 0
        n_art = 0
        for prk, p in sorted(prks.items(), key=lambda kv: kv[0]):
            n_zi = len(p["zi"])
            n_tek = len(p["zi"] & sfk_zi)
            weg = n_zi > 0 and n_tek == n_zi
            geraakt = n_tek > 0
            solo = len(p["firmas"]) == 1
            n_art += n_zi
            n_weg += 1 if weg else 0
            n_geraakt += 1 if geraakt else 0
            n_solo += 1 if solo else 0
            prk_rows.append({
                "prk": prk,
                "naam": p["naam"],
                "n_artikelen": n_zi,
                "n_artikelen_tekort": n_tek,
                "n_firmas": len(p["firmas"]),
                "weg": weg,
                "geraakt": geraakt,
                "solo": solo,
            })
        n_prk = len(prk_rows)

        # Percentage alleen bij een pool die groot genoeg is om te meten.
        toon_pct = in_noemer and n_prk >= MIN_PRK_VOOR_PERCENTAGE
        pct_weg = round(100.0 * n_weg / n_prk, 1) if toon_pct else None
        pct_geraakt = round(100.0 * n_geraakt / n_prk, 1) if toon_pct else None

        # EMS-kleur van de pool: zwaarste beoordeling over alle ATC5-codes die in deze
        # pool voorkomen, uit de G-standaard of uit een bron.
        atc5_bekend = set(g["atc5_per_pool"].get(pool, set())) | farm_atc5_per_pool.get(pool, set())
        atc5_bekend |= {a for a in bui["landen_per_atc5"] if a[:5] == pool}
        kleuren = Counter(eml[a] for a in atc5_bekend if eml.get(a))
        pool_eml = ernstigste(kleuren)

        # Buitenland: telling, nooit een percentage. Statussemantiek verschilt per land
        # (CA is grotendeels archief, JP/AT/CZ/CO/EE/BG sluiten vrijwel nooit iets af),
        # dus landen zijn niet optelbaar tot een noemer.
        bui_atc5 = {a: len(c) for a, c in bui["landen_per_atc5"].items() if a[:5] == pool}
        n_bui3 = sum(1 for v in bui_atc5.values() if v >= MIN_LANDEN_BUITENLAND)
        n_bui1 = len(bui_atc5)

        # Recentheid: één telling, geen score en geen leeftijd.
        n_meld = bui["meld_per_pool"].get(pool, 0)
        n_gedateerd = bui["gedateerd_per_pool"].get(pool, 0)
        dekking_ok = n_meld > 0 and (n_gedateerd / n_meld) >= MIN_DATUMDEKKING
        n_nieuw = bui["nieuw_per_pool"].get(pool, 0) if dekking_ok else None

        # Naam: liever de code met het label 'naam onbekend' dan een verzonnen groepsnaam.
        naam = a4namen.get(pool)
        naam_bron = "atc4_namen"
        if not naam and g["atc4_naam"].get(pool):
            # de G-standaard voert deze pool zelf als ATC4-code op, met groepsnaam
            naam = g["atc4_naam"][pool]
            naam_bron = "atc4_gstandaard"
        if not naam:
            atc5_namen = {g["atc5_naam"][a] for a in g["atc5_per_pool"].get(pool, set())
                          if g["atc5_naam"].get(a)}
            if len(atc5_namen) == 1:
                naam = next(iter(atc5_namen))
                naam_bron = "atc5_gstandaard"
            else:
                naam = None
                naam_bron = "onbekend"

        if not in_noemer:
            blok = None          # geen NL-noemer: geen percentage, geen 0% en geen 100%
        elif n_weg > 0:
            blok = "A"
        elif n_geraakt > 0:
            blok = "B"
        else:
            blok = "C"

        uit.append({
            "atc4": pool,
            "naam": naam,
            "naam_bron": naam_bron,
            "eml": pool_eml,
            "eml_telling": {"rood": kleuren.get("rood", 0), "oranje": kleuren.get("oranje", 0),
                            "geel": kleuren.get("geel", 0), "atc5_beoordeeld": sum(kleuren.values())},
            "in_noemer": in_noemer,
            "blok": blok,
            "noemer": {
                "n_atc5": len(g["atc5_per_pool"].get(pool, set())),
                "n_prk": n_prk,
                "n_artikelen": n_art,
                "n_bereidingen_prk": len(g["bereiding_prk"].get(pool, set())),
                "n_bereidingen_artikelen": g["bereiding_art"].get(pool, 0),
            },
            "nl": {
                "n_prk_weg": n_weg,
                "n_prk_geraakt": n_geraakt,
                "n_prk_solo": n_solo,
                "pct_weg": pct_weg,
                "pct_geraakt": pct_geraakt,
                "pct_onderdrukt": in_noemer and not toon_pct,
                "pct_onderdrukt_reden": (
                    None if toon_pct else
                    ("geen NL-noemer: pool niet als specialite op de NL-markt" if not in_noemer
                     else f"pool te klein voor een percentage (<{MIN_PRK_VOOR_PERCENTAGE} PRK)")),
            },
            "farmanco": {
                "n_atc5": len(farm_atc5_per_pool.get(pool, set())),
                "eml": ernstigste(farm_kleur_per_pool.get(pool, set())),
                "atc5": sorted(farm_atc5_per_pool.get(pool, set())),
            },
            "buitenland": {
                "n_atc5_min3landen": n_bui3,
                "n_atc5_min1land": n_bui1,
                "n_landen": len(bui["landen_per_pool"].get(pool, set())),
                "n_meldingen": n_meld,
                "landen": sorted(bui["landen_per_pool"].get(pool, set())),
            },
            "recent": {
                # buitenland en NL blijven gescheiden, net als bij de percentages
                "n_nieuw_7d": n_nieuw,
                "n_meldingen": n_meld,
                "n_gedateerd": n_gedateerd,
                "toon": dekking_ok,
                "n_nl_nieuw_7d": len(farm_nieuw_per_pool.get(pool, [])),
                "nl_nieuw": farm_nieuw_per_pool.get(pool, []),
            },
            "prk": prk_rows,
        })

    # --- Sorteren: drie vaste blokken, geen vrije sorteerkolom ---------------
    def sleutel(p):
        b = p["blok"]
        nl, n = p["nl"], p["noemer"]["n_prk"]
        if b == "A":
            # echt verlies in een ondiepe pool bovenaan: eerst aantal weg, dan aandeel weg
            return (0, -nl["n_prk_weg"], -(nl["n_prk_weg"] / n), -(nl["n_prk_solo"] / n), p["atc4"])
        if b == "B":
            return (1, -nl["n_prk_geraakt"], -(nl["n_prk_geraakt"] / n), 0, p["atc4"])
        if b == "C":
            return (2, 0, 0, 0, p["atc4"])
        return (3, -p["buitenland"]["n_atc5_min3landen"], 0, 0, p["atc4"])

    uit.sort(key=sleutel)

    gstd_mtime = dt.datetime.fromtimestamp(os.path.getmtime(GSTD)).date().isoformat()
    doc = {
        "generated": dt.date.today().isoformat(),
        "peildatum_buitenland": peildatum,
        "bronnen": {
            "gstandaard": {
                "pad": GSTD,
                "bestandsdatum": gstd_mtime,
                "let_op": ("de G-standaard zelf bevat geen exportdatum; dit is de "
                           "bestandsdatum. Zonder vaste versie is een percentage van vorige "
                           "week niet vergelijkbaar met dat van deze week."),
                "beperking": ("de G-standaard kent geen uit-de-handel-vlag: de noemer bevat "
                              "een bekende, niet te kwantificeren overschatting."),
                "regels": g["stat"]["regels"],
                "unieke_zi": g["stat"]["unieke_zi"],
                "zi_in_noemer": g["stat"]["zi_in_noemer"],
                "zi_alleen_atc4": g["stat"]["zi_alleen_atc4"],
                "zi_zonder_bruikbare_atc4": g["stat"]["zi_zonder_bruikbare_atc4"],
                "zi_buiten_noemer_productgroep": g["stat"]["zi_buiten_noemer_productgroep"],
                "zi_zonder_prk": g["stat"]["zi_zonder_prk"],
            },
            "landkaart": {"bestand": "data.json", "generated": data.get("generated"),
                          **{k: v for k, v in bui["stat"].items()}},
            "sfk": {"bestand": "sfk_tekorten.json", "generated": sfk.get("generated"), **sfk_stat},
            "farmanco": {"bestand": "farmanco_eml.json", "generated": farm.get("generated"),
                         **farm_stat},
            "eml": {"bestand": "eml_atc5.json", "atc5_beoordeeld": len(eml)},
            "atc4_namen": {"bestand": "atc4_namen.json", "codes": len(a4namen)},
        },
        "definities": {
            "pool": "ATC4 = eerste 5 tekens van de ATC-code.",
            "noemer": (f"unieke PRK-codes in de G-standaard met een ATC van 5 (ATC4) of 7 "
                       f"(ATC5) tekens en productgroep {NOEMER_PRODUCTGROEP}, ontdubbeld op "
                       "ZI-nummer. Groepen zonder 5e ATC-niveau (A12AX, B03AC, B05XC, ...) "
                       "tellen dus gewoon mee; n_atc5 blijft 0 voor zulke pools."),
            "weg": "PRK waarvan ALLE onderliggende ZI-artikelen op de SFK-tekortenlijst staan.",
            "geraakt": ("PRK met minstens één artikel op de SFK-tekortenlijst. Vroegsignaal, "
                        "geen beschikbaarheid: de meeste geraakte PRK's hebben nog een leverancier."),
            "solo": "PRK met één handelsvergunninghouder over alle artikelen.",
            "percentage": (f"alleen getoond bij >= {MIN_PRK_VOOR_PERCENTAGE} PRK in de pool; "
                           "daaronder staat uitsluitend de kale breuk."),
            "buitenland": (f"telling, nooit een percentage: aantal ATC5-codes met een actieve of "
                           f"aankomende melding in >= {MIN_LANDEN_BUITENLAND} landen, na verwijdering "
                           "van JP 'normal' en AT 'verfügbar' (dat zijn beschikbaarheidsmeldingen)."),
            "farmanco": ("telling op ATC5-niveau met EMS-kleur; Farmanco heeft geen prk-veld en "
                         "zit daarom niet in de PRK-teller."),
            "cbg_tav": ("niet meegeteld: een tijdelijk afwijkende verpakking is een oplossing voor "
                        "een tekort, geen tekort."),
            "nieuw_7d": (f"ALLEEN BUITENLAND. Meldingen met een echt bronsignaal (lu als "
                         f"lu != sa, anders ss als ss != sa) in de laatste {NIEUW_DAGEN} dagen "
                         f"voor de peildatum; null als minder dan {int(MIN_DATUMDEKKING * 100)}% "
                         "van de meldingen in de pool gedateerd is. Een aangekondigde startdatum "
                         "in de toekomst telt wel als gedateerd, maar nooit als nieuw signaal."),
            "nl_nieuw_7d": (f"ALLEEN NEDERLAND. Farmanco-meldingen met datum in de laatste "
                            f"{NIEUW_DAGEN} dagen voor de peildatum, met stof en datum erbij. "
                            "Staat los van n_nieuw_7d en wordt er nooit bij opgeteld: de ene "
                            "telt meldingen in 31 registers, de andere meldingen in NL."),
            "nl_en_buitenland": "worden nooit in één breuk of één percentage samengevoegd.",
            "blokken": {
                "A": "uitwijk verloren: minstens één volledig weggevallen PRK",
                "B": "signaal, uitwijk nog aanwezig: geraakt > 0 en weg = 0",
                "C": "geen NL-signaal",
                "null": "geen NL-noemer: pool niet als specialite op de NL-markt (n.v.t.)",
            },
        },
        "totalen": {
            "pools": len(uit),
            "pools_met_noemer": sum(1 for p in uit if p["in_noemer"]),
            "pools_zonder_noemer": sum(1 for p in uit if not p["in_noemer"]),
            "prk_in_noemer": sum(p["noemer"]["n_prk"] for p in uit),
            "artikelen_in_noemer": sum(p["noemer"]["n_artikelen"] for p in uit),
            "pools_met_percentage": sum(1 for p in uit if p["nl"]["pct_weg"] is not None),
            "pools_percentage_onderdrukt": sum(1 for p in uit if p["nl"]["pct_onderdrukt"]),
            "blok_a": sum(1 for p in uit if p["blok"] == "A"),
            "blok_b": sum(1 for p in uit if p["blok"] == "B"),
            "blok_c": sum(1 for p in uit if p["blok"] == "C"),
            "prk_weg": sum(p["nl"]["n_prk_weg"] for p in uit),
            "prk_geraakt": sum(p["nl"]["n_prk_geraakt"] for p in uit),
            "pools_zonder_naam": sum(1 for p in uit if p["naam_bron"] == "onbekend"),
            "nieuw_7d_totaal": sum(p["recent"]["n_nieuw_7d"] or 0 for p in uit),
            "nl_nieuw_7d_totaal": sum(p["recent"]["n_nl_nieuw_7d"] for p in uit),
            "pools_met_nl_nieuw_7d": sum(1 for p in uit if p["recent"]["n_nl_nieuw_7d"]),
        },
        "pools": uit,
    }

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
    print(f"geschreven: {OUT}  ({os.path.getsize(OUT)/1024:.0f} KB)")
    return doc


if __name__ == "__main__":
    main()
