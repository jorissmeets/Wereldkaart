"""
Preprocessor: leest alle CSV-bestanden uit output/ en genereert data.json
met schone ATC5-codes, tijdvelden, afgeleide status en KPI-data.
"""
from __future__ import annotations

import json
import re
import ast
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DATA_FILE = Path(__file__).resolve().parent / "data.json"
EMS_FILE = Path(__file__).resolve().parent.parent / "LijstenEMS" / "Achtergrondlijst 2025-Tabel 1.csv"

ATC_COLUMNS = ["atc_code", "atc_level1", "Atc Code"]
ATC5_RE = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")

# Statussen die als "resolved" gelden
RESOLVED_STATUSES = {
    "resolved", "reversed", "avoided shortage",
}
# Statussen die als "discontinued" gelden
DISCONTINUED_STATUSES = {
    "discontinued", "to be discontinued", "to be discontinued",
    "leaving the market", "permanent_discontinuation",
    "supply_discontinuation", "d",
    "8 abregistriert  - ausser handel", "afskráning", "discontinuation",
}
# Statussen die als "upcoming" gelden
UPCOMING_STATUSES = {
    "upcoming", "anticipated", "anticipated shortage",
    "2 angekündigter engpass",
}


def extract_atc5(raw: str) -> str | None:
    """Haal een schone ATC5-code uit diverse formaten."""
    raw = str(raw).strip()
    if not raw or raw.lower() == "nan":
        return None

    if raw.startswith("{"):
        try:
            d = ast.literal_eval(raw)
            raw = d.get("atcCode8", d.get("atcCode", ""))
        except Exception:
            return None

    token = raw.split()[0].upper()

    if len(token) == 8 and re.match(r"^[A-Z]\d{2}[A-Z]{2}\d{3}$", token):
        token = token[:7]

    if ATC5_RE.match(token):
        return token
    return None


def safe_str(val) -> str:
    """Return stripped string or empty string for NaN/None."""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    return "" if s.lower() == "nan" else s


def parse_date(val) -> str | None:
    """Probeer een datum te parsen naar ISO-formaat (YYYY-MM-DD)."""
    s = safe_str(val)
    if not s:
        return None

    # Probeer diverse formaten
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:19] if "T" in s else s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Numeriek formaat (KR): 20130731
    if s.isdigit() and len(s) == 8:
        try:
            return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def derive_status(raw_status: str, shortage_start: str | None,
                  estimated_end: str | None, actual_end: str | None,
                  last_updated: str | None = None) -> str:
    """Leid een gestandaardiseerde status af.

    Statussen: active, upcoming, resolved, discontinued, inactive
    'inactive' = zou actief zijn, maar al > 1 jaar geen update/start (verouderde bron-data)
    """
    today = date.today().isoformat()
    cutoff_1yr = (date.today() - timedelta(days=365)).isoformat()
    lower = raw_status.lower().strip()

    if lower in RESOLVED_STATUSES:
        return "resolved"
    if lower in DISCONTINUED_STATUSES:
        return "discontinued"
    if lower in UPCOMING_STATUSES:
        return "upcoming"

    # Datumgebaseerde afleiding
    if shortage_start and shortage_start > today:
        return "upcoming"

    # Als estimated_end in het verleden ligt en actual_end bekend is
    if actual_end and actual_end <= today:
        return "resolved"

    # Inactief: > 1 jaar geen recente melding (last_updated heeft voorrang, anders shortage_start)
    last_known = last_updated or shortage_start
    if last_known and last_known < cutoff_1yr:
        return "inactive"

    return "active"


# ── Mapping dosage_form → gestandaardiseerde toedieningsvorm ──
# Keywords (lowercase) → EMS-categorie. Volgorde is belangrijk: eerste match wint.
_TV_RULES: list[tuple[list[str], str]] = [
    # OCULAIR — vóór generieke "solution/drops"
    (["eye drop", "eye gel", "ophthalm", "augentropfen", "augengel", "augensalbe",
      "augndrop", "augnhlaup", "augnsmyrsli", "silmatilg", "silmageel",
      "oculair", "intravit", "intra-ocul", "intracamer", "subretinal",
      "akių lašai", "akių gelis"], "OCULAIR"),
    (["nasal", "nasaal", "nosies", "ninasprei", "ninatilg",
      "pršilo za nos", "prašek za nos", "neus"], "NASAAL"),
    (["inhal", "nebuli", "eimgjaf", "įkvepiam", "suslėgt",
      "dreifa til íkomu í barka", "dreifa í eimgjafa",
      "prašek za inhaliranje",
      "innöndunarduft", "innúðalyf", "innöndunarlausn"], "INHALATIE"),
    (["rectal", "rectaal", "suppos", "endaþarm", "rektaal",
      "žvakut", "sveèka", "tiesiosios žarnos", "ovul"], "RECTAAL"),
    (["vaginal", "vaginaal", "makšt", "vartojimo į makštį"], "VAGINAAL"),
    (["sublingual", "podjezič", "poliežuvin"], "SUBLINGUAAL"),
    (["transderm", "pleistras", "plaaster", "pflaster", "obliž",
      "drug delivery system", "forðaplástur"], "TRANSDERMAAL"),
    (["cream", "crema", "creme", "kreem", "kremas", "krema ",
      "ointment", "salve", "salv ", "tepalas", "mazilo", "smyrsli",
      "cutaneous", "odos tirpalas", "nahala", "nahapasta", "nahasprei",
      "šampoon", "šampūn", "wirkstoffhalt", "ravimküüne",
      "nagų lak", "epilesion",
      "gelis", "gel,", "hlaup",
      "krem", "húðlausn"], "CUTAAN"),
    (["inject", "infus", "stungulyf", "süstelah", "süstesuspen",
      "milteliai injekc", "milteliai infuz", "koncentrat",
      "raztopina za injic", "raztopina za infund",
      "depot-injektions", "lyfjapenn",
      "conc. pt. sol. inj", "conc. pt. sol. perf",
      "prašek za raztopino za", "prašek za koncentrat",
      "prašek za disperzijo", "prašek za suspenzijo",
      "prašek in", "suspenzija za injic",
      "komplet za pripravo", "rinkinys radiofarmac",
      "radiofarmatseutiline",
      "emulsion zur infusion",
      "duft og leysir",
      "hemodial", "extracorpor",
      "implanta", "voor implantatie",
      "injekcinis", "infuzinis",
      "innrennslislyf", "innrennslisþykkni",
      "solución inyectable", "inyectable",
      "solução injetável",
      "injektionslösung",
      "injekcinė", "milteliai ir tirpiklis injekc",
      "sol. inj"], "PARENTERAAL"),
    (["tablet", "tafla", "tabletė", "tabletės", "compr",
      "capsul", "kapsul", "hylki", "caps.",
      "hartkapsel", "kõvakapsel", "pehmekapsel",
      "sirup", "siirup", "syrup",
      "oral sol", "oral susp", "oral liq",
      "suukaud", "geriam", "peroralna", "peroralni",
      "oralna", "oraal",
      "loseng", "pastil", "lozenge",
      "granul", "zrnca",
      "plėvele dengt", "õhukese polümeer",
      "skrandyje neir", "magensaft", "gastrorez", "enteric",
      "brausetab", "effervesc", "šumeč", "kihisev",
      "filmtab", "film coat", "film-coat", "filmuhúð",
      "überzog", "forðatafla", "coated tab",
      "nærimis", "kramtom", "chewable", "žveèljiv",
      "disperguoj", "dispergeer", "dispersi",
      "dreifitafla",
      "dropar til inntöku", "suukaudsed tilg",
      "modifikuoto", "modifitseeritult",
      "pailginto", "prolongeeritult", "podaljšan",
      "extended release", "modified release", "controlled",
      "šnypščios", "buccaltab",
      "duft til inntöku",
      "baðlyf", "valgomasis gel",
      "suuõõnesprei",
      "milteliai geriam",
      "vaistinė kramtom",
      "inntöku",
      "retardiert",
      "mixtúra", "lyfjatyggigúmmí", "mixtúruduft"], "ORAAL"),
    (["ear drop", "ear/eye", "auriculair"], "AURICULAIR"),
]


def map_toedieningsvorm(dosage_form: str) -> str:
    """Map een dosage_form string naar een gestandaardiseerde EMS toedieningsvorm."""
    low = dosage_form.lower()
    for keywords, tv in _TV_RULES:
        for kw in keywords:
            if kw in low:
                return tv
    return ""


def load_ems_data() -> tuple[list[str], dict[str, list[str]], dict[str, str]]:
    """Lees EMS Achtergrondlijst.

    Retourneert:
      - ems_rood:       sorted list van ATC5-codes met beoordeling 'rood'
      - ems_rood_detail: dict {ATC5 → [toedieningsvormen met 'rood']}
      - atc_tv_map:     dict {ATC5 → toedieningsvorm} alleen voor ATC5
                         met precies 1 unieke toedieningsvorm (unambiguous)
    """
    if not EMS_FILE.exists():
        print(f"EMS-bestand niet gevonden: {EMS_FILE}")
        return [], {}, {}

    df = pd.read_csv(EMS_FILE, sep=";", skiprows=1, encoding="utf-8",
                     on_bad_lines="skip", low_memory=False)

    required = {"Atc", "Uiteindelijke beoordeling", "Toedieningsvorm"}
    if not required.issubset(df.columns):
        print(f"EMS-bestand mist verwachte kolommen: {required - set(df.columns)}")
        return [], {}, {}

    # --- Rood (kritiek) ---
    rood = df[df["Uiteindelijke beoordeling"].str.strip().str.lower() == "rood"]
    rood_atcs: set[str] = set()
    rood_detail: dict[str, set[str]] = {}
    for _, row in rood.iterrows():
        raw_atc = row.get("Atc")
        if pd.isna(raw_atc):
            continue
        code = str(raw_atc).strip().upper()
        if not ATC5_RE.match(code):
            continue
        rood_atcs.add(code)
        tv = safe_str(row.get("Toedieningsvorm")).upper()
        if tv:
            rood_detail.setdefault(code, set()).add(tv)

    # --- ATC → toedieningsvorm (alleen unambiguous) ---
    all_atc_tvs: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        raw_atc = row.get("Atc")
        if pd.isna(raw_atc):
            continue
        code = str(raw_atc).strip().upper()
        if not ATC5_RE.match(code):
            continue
        tv = safe_str(row.get("Toedieningsvorm")).upper()
        if tv:
            all_atc_tvs.setdefault(code, set()).add(tv)

    atc_tv_map = {atc: next(iter(tvs)) for atc, tvs in all_atc_tvs.items() if len(tvs) == 1}

    return (
        sorted(rood_atcs),
        {k: sorted(v) for k, v in rood_detail.items()},
        atc_tv_map,
    )


def build():
    if not OUTPUT_DIR.exists():
        print(f"Output-map niet gevonden: {OUTPUT_DIR}")
        return

    # Laad EMS-data één keer (voor tv-fallback via ATC)
    ems_rood, ems_rood_detail, atc_tv_map = load_ems_data()
    if atc_tv_map:
        print(f"  EMS ATC→toedieningsvorm lookup: {len(atc_tv_map)} unambiguous ATC5-codes")

    records: list[dict] = []

    for p in sorted(OUTPUT_DIR.glob("*_shortage_*.csv")):
        try:
            df = pd.read_csv(p, encoding="utf-8", on_bad_lines="skip", low_memory=False)
        except Exception:
            try:
                df = pd.read_csv(p, encoding="latin-1", on_bad_lines="skip", low_memory=False)
            except Exception:
                continue

        atc_col = None
        for c in ATC_COLUMNS:
            if c in df.columns:
                atc_col = c
                break
        if atc_col is None or "country_code" not in df.columns:
            continue

        for _, row in df.iterrows():
            atc5 = extract_atc5(row.get(atc_col, ""))
            if not atc5:
                continue

            cc = safe_str(row.get("country_code")).upper()
            if not cc or len(cc) != 2:
                continue
            if cc == "NL":
                continue

            cn = safe_str(row.get("country_name")) or cc
            med = safe_str(row.get("medicine_name"))
            substance = safe_str(row.get("active_substance"))
            dosage_form = safe_str(row.get("dosage_form"))
            status_raw = safe_str(row.get("status")) or "shortage"

            shortage_start = parse_date(row.get("shortage_start"))
            estimated_end = parse_date(row.get("estimated_end"))
            actual_end = parse_date(row.get("actual_end"))
            last_updated = parse_date(row.get("last_updated"))
            scraped_at = parse_date(row.get("scraped_at"))

            # Bereken resolved_date
            resolved_date = None
            derived = derive_status(status_raw, shortage_start, estimated_end, actual_end, last_updated)
            if derived == "resolved":
                resolved_date = actual_end or estimated_end or scraped_at

            rec = {
                "atc": atc5,
                "cc": cc,
                "cn": cn,
                "st": derived,          # derived_status: active/upcoming/resolved/discontinued
                "sr": status_raw,       # raw status
                "ss": shortage_start,   # shortage_start
                "ee": estimated_end,    # estimated_end
                "rd": resolved_date,    # resolved_date
                "sa": scraped_at,       # scraped_at
            }
            if med:
                rec["mn"] = med
            if substance:
                rec["sub"] = substance

            # Toedieningsvorm bepalen:
            #   1. uit dosage_form via keyword-mapping
            #   2. fallback: via ATC-code als EMS maar 1 unieke tv heeft
            if dosage_form:
                rec["df"] = dosage_form
                tv = map_toedieningsvorm(dosage_form)
                if tv:
                    rec["tv"] = tv
            if "tv" not in rec and atc5 in atc_tv_map:
                rec["tv"] = atc_tv_map[atc5]
                rec["tv_src"] = "atc"

            records.append(rec)

    if not records:
        print("Geen records gevonden met ATC-data.")
        return

    # ── Vul ontbrekende active_substance aan via ATC-lookup ──
    # Bouw lookup: ATC → meest voorkomende substance uit records die het wél hebben
    from collections import Counter
    atc_sub_counts: dict[str, Counter] = {}
    for r in records:
        sub = r.get("sub", "").strip()
        if sub:
            atc_sub_counts.setdefault(r["atc"], Counter())[sub] += 1

    atc_sub_lookup = {atc: counts.most_common(1)[0][0] for atc, counts in atc_sub_counts.items()}

    filled = 0
    for r in records:
        if not r.get("sub") and r["atc"] in atc_sub_lookup:
            r["sub"] = atc_sub_lookup[r["atc"]]
            filled += 1
    if filled:
        print(f"  Active substance aangevuld via ATC-lookup: {filled} records")

    # Bouw indices en KPI-data
    today = date.today().isoformat()
    all_countries = sorted({r["cc"] for r in records})
    all_atcs = sorted({r["atc"] for r in records})

    # Per ATC: in hoeveel landen actief
    atc_country_count = {}
    for r in records:
        if r["atc"] not in atc_country_count:
            atc_country_count[r["atc"]] = set()
        atc_country_count[r["atc"]].add(r["cc"])
    atc_country_count = {k: len(v) for k, v in atc_country_count.items()}

    if ems_rood:
        print(f"  EMS kritieke ATC5-codes (rood): {len(ems_rood)}")

    result = {
        "generated": today,
        "monitored_countries": all_countries,
        "total_atc": len(all_atcs),
        "total_countries": len(all_countries),
        "atc_country_count": atc_country_count,
        "ems_rood_atcs": ems_rood,
        "ems_rood_detail": ems_rood_detail,
        "records": records,
    }

    DATA_FILE.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    # Stats
    active = sum(1 for r in records if r["st"] == "active")
    upcoming = sum(1 for r in records if r["st"] == "upcoming")
    resolved = sum(1 for r in records if r["st"] == "resolved")
    discontinued = sum(1 for r in records if r["st"] == "discontinued")
    inactive = sum(1 for r in records if r["st"] == "inactive")
    # Toedieningsvorm stats
    tv_via_df = sum(1 for r in records if "tv" in r and r.get("tv_src") != "atc")
    tv_via_atc = sum(1 for r in records if r.get("tv_src") == "atc")
    tv_none = sum(1 for r in records if "tv" not in r)
    print(f"Data geschreven naar {DATA_FILE}")
    print(f"  {len(records)} records, {len(all_atcs)} ATC5-codes, {len(all_countries)} landen")
    print(f"  Status: {active} actief, {upcoming} upcoming, {resolved} opgelost, {discontinued} discontinued, {inactive} inactief (>1 jaar)")
    print(f"  Toedieningsvorm: {tv_via_df} via dosage_form, {tv_via_atc} via ATC-lookup, {tv_none} onbekend")


if __name__ == "__main__":
    build()
