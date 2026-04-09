"""Add ATC codes to medication shortage CSV files.

Reads LCG.csv to build substance-name → ATC-code lookups, then processes
CSV files in the target directory.  For each file it adds an ``atc_code``
column (if missing) and fills empty cells by matching ``active_substance``
(or ``medicine_name`` as fallback) against the lookups.

Usage::

    python add_atc_to_shortages.py                       # default paths
    python add_atc_to_shortages.py --dry-run              # report only
    python add_atc_to_shortages.py --report unmatched.csv # save unmatched
    python add_atc_to_shortages.py --input-dir /some/path --lcg-path /other/LCG.csv
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from collections import Counter
from pathlib import Path

import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_LCG = _SCRIPT_DIR / "medicatie_matcher" / "data" / "LCG.csv"
_DEFAULT_INPUT = _SCRIPT_DIR.parent / "Nieuwe Scraper" / "output"

# ── LCG columns used for lookup ─────────────────────────────────────────────

_LOOKUP_COLS = [
    "Werkzame -/hulpstof (stam)",
    "Werkzame -/hulpstof (specifiek)",
    "ATC omschrijving Engels",
    "ATC omschrijving Nederlands",
]

# ── Salt suffixes to strip (multi-language) ──────────────────────────────────

_SALT_SUFFIXES: list[str] = sorted(
    [
        # English
        "HYDROCHLORIDE", "DIHYDROCHLORIDE", "SODIUM", "DISODIUM", "TRISODIUM",
        "POTASSIUM", "SULFATE", "SULPHATE", "CITRATE", "ACETATE", "TARTRATE",
        "MALEATE", "FUMARATE", "MESYLATE", "BESYLATE", "PHOSPHATE", "SUCCINATE",
        "NITRATE", "CHLORIDE", "BROMIDE", "DECANOATE", "MONOHYDRATE", "DIHYDRATE",
        "CALCIUM", "TROMETHAMINE", "ANHYDROUS", "LACTATE", "GLUCONATE", "OXIDE",
        "CARBONATE", "VALERATE", "PROPIONATE", "BUTYRATE", "BENZOATE",
        "HEMISULFATE", "HEMIHYDRATE", "TRIHYDRATE", "HEXAHYDRATE",
        # German
        "NATRICUM", "KALIUM", "HYDROCHLORID", "SULFAT", "TARTRAT", "MALEAT",
        "FUMARAT", "MESILAT", "DIHYDROCHLORID", "HYDROBROMID",
        # French
        "SODIQUE", "POTASSIQUE",
        # Italian
        "SOLFATO", "CLORIDRATO", "SODICO", "POTASSICO", "TARTRATO", "CITRATO",
        "ACETATO", "MALEATO", "FUMARATO", "MESILATO", "NITRATO", "FOSFATO",
        "MONOIDRATO", "DIIDRATO",
        # Dutch
        "NATRIUM", "KALIUM", "DINATRIUM", "TRINATRIUM",
        # Spanish
        "SODIO", "POTASIO", "CLORHIDRATO", "SULFATO",
    ],
    key=len,
    reverse=True,  # longest first so "DIHYDROCHLORIDE" is tried before "HYDROCHLORIDE"
)

_SALT_PREFIX_PATTERN = re.compile(
    r"^(ACIDO|ACID|ACIDE)\s+", re.IGNORECASE,
)

_PAREN_SUFFIX = re.compile(r"\s*\(.*\)\s*$")


# ── Normalisation ────────────────────────────────────────────────────────────

def _remove_accents(s: str) -> str:
    """Remove diacritics/accents from a string."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize(name: str) -> str:
    """Uppercase, strip accents, collapse whitespace."""
    if not name:
        return ""
    s = _remove_accents(str(name)).upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _strip_salts(name: str) -> str:
    """Remove known salt suffixes from a substance name."""
    s = name
    # Remove parenthetical qualifiers like "(ALS SULFAAT)" or "(BASE)"
    s = _PAREN_SUFFIX.sub("", s).strip()
    changed = True
    while changed:
        changed = False
        for suffix in _SALT_SUFFIXES:
            if s.endswith(" " + suffix):
                s = s[: -(len(suffix) + 1)].strip()
                changed = True
                break
    return s


def _strip_acid_prefix(name: str) -> str:
    """Remove 'ACIDO'/'ACID'/'ACIDE' prefix (Italian/French/English)."""
    m = _SALT_PREFIX_PATTERN.match(name)
    if m:
        return name[m.end():].strip()
    return name


# ── Build lookups from LCG ───────────────────────────────────────────────────

def build_lookups(lcg_path: Path) -> dict[str, str]:
    """Build a dict mapping normalised substance name → ATC code.

    When a substance maps to multiple ATC codes, the most frequent one wins.
    """
    df = pd.read_csv(lcg_path, sep=";", dtype=str, encoding="utf-8-sig")

    # Collect all (normalised_name, atc_code) pairs
    pairs: list[tuple[str, str]] = []
    for col in _LOOKUP_COLS:
        if col not in df.columns:
            continue
        for _, row in df[[col, "ATC code"]].dropna(subset=[col, "ATC code"]).iterrows():
            raw_name = str(row[col]).strip()
            atc = str(row["ATC code"]).strip()
            if not raw_name or not atc:
                continue
            norm = _normalize(raw_name)
            if norm:
                pairs.append((norm, atc))

    # For each name, pick the most common ATC code
    name_atc_counts: dict[str, Counter[str]] = {}
    for name, atc in pairs:
        name_atc_counts.setdefault(name, Counter())[atc] += 1

    lookup: dict[str, str] = {}
    for name, counter in name_atc_counts.items():
        lookup[name] = counter.most_common(1)[0][0]

    return lookup


# ── Matching functions ───────────────────────────────────────────────────────

def _match_single(name: str, lookup: dict[str, str]) -> str | None:
    """Try to match a single substance name to an ATC code (tiers 1-2)."""
    norm = _normalize(name)
    if not norm:
        return None

    # Tier 1: exact match
    if norm in lookup:
        return lookup[norm]

    # Tier 2a: salt-stripped
    stripped = _strip_salts(norm)
    if stripped != norm and stripped in lookup:
        return lookup[stripped]

    # Tier 2b: acid prefix stripped
    no_acid = _strip_acid_prefix(norm)
    if no_acid != norm and no_acid in lookup:
        return lookup[no_acid]

    # Tier 2c: salt-stripped + acid prefix stripped
    no_acid_stripped = _strip_salts(no_acid)
    if no_acid_stripped != no_acid and no_acid_stripped in lookup:
        return lookup[no_acid_stripped]

    return None


def _match_combination(name: str, lookup: dict[str, str]) -> str | None:
    """Split combination products and try to match components."""
    norm = _normalize(name)
    # Split on common separators
    parts = re.split(r"\s*[+/,]\s*", norm)
    if len(parts) < 2:
        return None

    atc_codes: set[str] = set()
    for part in parts:
        part = part.strip()
        if not part:
            continue
        code = _match_single(part, lookup)
        if code:
            atc_codes.add(code)

    # Only return if all components map to a single ATC (combination product)
    if len(atc_codes) == 1:
        return atc_codes.pop()
    return None


def _match_contains(name: str, lookup: dict[str, str]) -> str | None:
    """Substring match — only if exactly one ATC code matches."""
    norm = _normalize(name)
    if not norm or len(norm) < 4:
        return None

    matches: set[str] = set()
    for lname, atc in lookup.items():
        if norm in lname or lname in norm:
            matches.add(atc)

    if len(matches) == 1:
        return matches.pop()
    return None


def _extract_candidates(name: str) -> list[str]:
    """Extract plausible substance-name candidates from a medicine_name string.

    Strategies:
    - Full name as-is
    - Text before first digit (e.g. "Sotalol" from "Sotalol Viatris 40 mg tabletter")
    - First word(s) before common stopwords (mg, ml, injection, tablet, solution, etc.)
    - Text inside parentheses (e.g. "allopurinol" from "Product (allopurinol)")
    - First token / first two tokens as brand→generic guess
    """
    candidates = [name]
    norm = name.strip()

    # Text before first digit
    m = re.match(r'^([A-Za-zÀ-ÿ\s\-/+]+?)(?:\s+\d)', norm)
    if m:
        before_digit = m.group(1).strip().rstrip('"\'.,;:')
        if len(before_digit) >= 3:
            candidates.append(before_digit)

    # Text inside parentheses — often the INN name
    paren_matches = re.findall(r'\(([^()]+)\)', norm)
    for pm in paren_matches:
        pm = pm.strip()
        # Skip if it looks like a dosage form or packaging info
        if re.search(r'\d', pm) and not re.search(r'[a-zA-Z]{4,}', pm):
            continue
        if len(pm) >= 3:
            candidates.append(pm)

    # Before common stopwords
    stop_pattern = re.compile(
        r'\s+(?:\d|mg|ml|mcg|µg|microgramo|comprimido|tablet|capsul|inject|'
        r'soluc|suspen|infus|oploss|prášok|inj\b|sol\b|sus\b|sup\b|'
        r'solucion|solution|pour|para|for|in\b|i\.v|film|dragee|'
        r'flakon|ampul|vial|amp\b)',
        re.IGNORECASE,
    )
    m2 = stop_pattern.search(norm)
    if m2 and m2.start() > 2:
        before_stop = norm[:m2.start()].strip().rstrip('"\'.,;:')
        if len(before_stop) >= 3:
            candidates.append(before_stop)
            # Also try stripping brand suffixes like "Accord", "Viatris", "Teva"
            brand_strip = re.sub(
                r'\s+(?:Accord|Viatris|Teva|Sandoz|Mylan|Hexal|Ratiopharm|'
                r'Neuraxpharm|Zentiva|Aurobindo|Fresenius|Kabi|Vet\.?)\s*$',
                '', before_stop, flags=re.IGNORECASE,
            ).strip()
            if brand_strip != before_stop and len(brand_strip) >= 3:
                candidates.append(brand_strip)

    # First word only (often the INN name)
    first_word = norm.split()[0] if norm.split() else ""
    if len(first_word) >= 4 and first_word.isalpha():
        candidates.append(first_word)

    # Deduplicate preserving order
    seen = set()
    unique = []
    for c in candidates:
        c_upper = c.upper().strip()
        if c_upper not in seen and len(c_upper) >= 3:
            seen.add(c_upper)
            unique.append(c)
    return unique


def match_substance(name: str, lookup: dict[str, str]) -> str | None:
    """4-tier matching cascade for a substance name, with candidate extraction."""
    # First try direct matching on the full name
    # Tier 1+2: exact / salt-stripped
    result = _match_single(name, lookup)
    if result:
        return result

    # Tier 3: combination splitting
    result = _match_combination(name, lookup)
    if result:
        return result

    # Try extracted candidates from medicine_name
    candidates = _extract_candidates(name)
    for candidate in candidates[1:]:  # skip first (= original, already tried)
        result = _match_single(candidate, lookup)
        if result:
            return result
        result = _match_combination(candidate, lookup)
        if result:
            return result

    # Tier 4: contains match (on all candidates)
    for candidate in candidates:
        result = _match_contains(candidate, lookup)
        if result:
            return result

    return None


# ── Process files ────────────────────────────────────────────────────────────

def process_file(
    path: Path,
    lookup: dict[str, str],
    dry_run: bool = False,
) -> dict:
    """Process one CSV file: add/fill atc_code column.

    Returns a stats dict with keys: file, total, already_had, matched, unmatched.
    """
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    total = len(df)

    has_atc_col = "atc_code" in df.columns
    if not has_atc_col:
        df["atc_code"] = ""

    # Determine which rows need filling
    needs_fill = df["atc_code"].isna() | (df["atc_code"].str.strip() == "")
    already_had = int((~needs_fill).sum())

    if needs_fill.sum() == 0:
        return {
            "file": path.name,
            "total": total,
            "already_had": already_had,
            "matched": 0,
            "unmatched": 0,
            "unmatched_substances": [],
        }

    matched = 0
    unmatched_substances: list[str] = []

    for idx in df.index[needs_fill]:
        row = df.loc[idx]

        # Try active_substance first, then medicine_name as fallback
        substance = ""
        if "active_substance" in df.columns:
            val = row.get("active_substance", "")
            if pd.notna(val) and str(val).strip():
                substance = str(val).strip()
        if not substance and "medicine_name" in df.columns:
            val = row.get("medicine_name", "")
            if pd.notna(val) and str(val).strip():
                substance = str(val).strip()

        if not substance:
            unmatched_substances.append("(empty)")
            continue

        atc = match_substance(substance, lookup)
        if atc:
            df.at[idx, "atc_code"] = atc
            matched += 1
        else:
            unmatched_substances.append(substance)

    unmatched_count = int(needs_fill.sum()) - matched

    if not dry_run and (matched > 0 or not has_atc_col):
        # Reorder: put atc_code after strength/dosage_form if possible
        cols = list(df.columns)
        if not has_atc_col and "atc_code" in cols:
            cols.remove("atc_code")
            # Insert after dosage_form, strength, or package_size
            insert_after = None
            for candidate in ["dosage_form", "strength", "package_size"]:
                if candidate in cols:
                    insert_after = cols.index(candidate) + 1
            if insert_after is not None:
                cols.insert(insert_after, "atc_code")
            else:
                cols.append("atc_code")
            df = df[cols]

        df.to_csv(path, index=False, encoding="utf-8-sig")

    return {
        "file": path.name,
        "total": total,
        "already_had": already_had,
        "matched": matched,
        "unmatched": unmatched_count,
        "unmatched_substances": list(set(unmatched_substances)),
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add ATC codes to medication shortage CSV files.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=_DEFAULT_INPUT,
        help=f"Directory with shortage CSV files (default: {_DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--lcg-path",
        type=Path,
        default=_DEFAULT_LCG,
        help=f"Path to LCG.csv (default: {_DEFAULT_LCG})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be done without writing files.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write unmatched substances to this CSV file.",
    )
    args = parser.parse_args()

    if not args.lcg_path.exists():
        print(f"ERROR: LCG file not found: {args.lcg_path}")
        return
    if not args.input_dir.exists():
        print(f"ERROR: Input directory not found: {args.input_dir}")
        return

    print(f"Loading LCG data from {args.lcg_path} ...")
    lookup = build_lookups(args.lcg_path)
    print(f"  Built lookup with {len(lookup)} unique substance names.\n")

    csv_files = sorted(args.input_dir.glob("*.csv"))
    if not csv_files:
        print("No CSV files found in input directory.")
        return

    mode = "DRY RUN" if args.dry_run else "PROCESSING"
    print(f"{'='*70}")
    print(f"  {mode}: {len(csv_files)} files in {args.input_dir}")
    print(f"{'='*70}\n")

    all_stats: list[dict] = []
    all_unmatched: list[dict] = []

    for path in csv_files:
        stats = process_file(path, lookup, dry_run=args.dry_run)
        all_stats.append(stats)

        # Collect unmatched for report
        for sub in stats["unmatched_substances"]:
            all_unmatched.append({"file": stats["file"], "substance": sub})

        # Print per-file summary
        status = ""
        if stats["already_had"] == stats["total"]:
            status = "  (skipped, all filled)"
        elif stats["matched"] > 0:
            status = f"  +{stats['matched']} matched"

        print(
            f"  {stats['file']:<50} "
            f"total={stats['total']:>5}  "
            f"had={stats['already_had']:>5}  "
            f"matched={stats['matched']:>4}  "
            f"unmatched={stats['unmatched']:>4}"
            f"{status}"
        )

    # Overall summary
    total_matched = sum(s["matched"] for s in all_stats)
    total_unmatched = sum(s["unmatched"] for s in all_stats)
    total_had = sum(s["already_had"] for s in all_stats)
    total_rows = sum(s["total"] for s in all_stats)

    print(f"\n{'='*70}")
    print(f"  TOTALS: {total_rows} rows, {total_had} already had ATC, "
          f"{total_matched} matched, {total_unmatched} unmatched")
    print(f"{'='*70}")

    # Write report
    if args.report and all_unmatched:
        report_df = pd.DataFrame(all_unmatched)
        report_df.to_csv(args.report, index=False, encoding="utf-8-sig")
        print(f"\nUnmatched substances written to: {args.report}")


if __name__ == "__main__":
    main()
