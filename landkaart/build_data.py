"""
Preprocessor: leest alle CSV-bestanden uit output/ en genereert data.json
met schone ATC5-codes, tijdvelden, afgeleide status en KPI-data.
"""

import json
import re
import ast
from datetime import date, datetime
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DATA_FILE = Path(__file__).resolve().parent / "data.json"

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
                  estimated_end: str | None, actual_end: str | None) -> str:
    """Leid een gestandaardiseerde status af."""
    today = date.today().isoformat()
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

    return "active"


def build():
    if not OUTPUT_DIR.exists():
        print(f"Output-map niet gevonden: {OUTPUT_DIR}")
        return

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

            cn = safe_str(row.get("country_name")) or cc
            med = safe_str(row.get("medicine_name"))
            substance = safe_str(row.get("active_substance"))
            status_raw = safe_str(row.get("status")) or "shortage"

            shortage_start = parse_date(row.get("shortage_start"))
            estimated_end = parse_date(row.get("estimated_end"))
            actual_end = parse_date(row.get("actual_end"))
            last_updated = parse_date(row.get("last_updated"))
            scraped_at = parse_date(row.get("scraped_at"))

            # Bereken resolved_date
            resolved_date = None
            derived = derive_status(status_raw, shortage_start, estimated_end, actual_end)
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

    result = {
        "generated": today,
        "monitored_countries": all_countries,
        "total_atc": len(all_atcs),
        "total_countries": len(all_countries),
        "atc_country_count": atc_country_count,
        "records": records,
    }

    DATA_FILE.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    # Stats
    active = sum(1 for r in records if r["st"] == "active")
    upcoming = sum(1 for r in records if r["st"] == "upcoming")
    resolved = sum(1 for r in records if r["st"] == "resolved")
    discontinued = sum(1 for r in records if r["st"] == "discontinued")
    print(f"Data geschreven naar {DATA_FILE}")
    print(f"  {len(records)} records, {len(all_atcs)} ATC5-codes, {len(all_countries)} landen")
    print(f"  Status: {active} actief, {upcoming} upcoming, {resolved} opgelost, {discontinued} discontinued")


if __name__ == "__main__":
    build()
