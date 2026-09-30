"""ATC-bewuste dedup: houd per land (prefix vóór _shortage_) de NIEUWSTE CSV die
daadwerkelijk bruikbare ATC-codes bevat. Zo vallen rauwe verse scrapes van landen
die zelf geen ATC publiceren automatisch terug op hun verrijkte oudere CSV
(voorkomt de regressie waarbij die landen wegvallen). Oudere/superseded CSV's
gaan naar output/_superseded_backup/ zodat build_data niet dubbeltelt.
"""
import os, glob, re, shutil
from collections import defaultdict
import pandas as pd

OUT = "output"
BK = os.path.join(OUT, "_superseded_backup")
os.makedirs(BK, exist_ok=True)
ATC5_RE = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")


def score(path):
    """(-> (heeft_atc, n_rijen)). heeft_atc = >=5% van de rijen heeft een geldige ATC5."""
    try:
        df = pd.read_csv(path, dtype=str, on_bad_lines="skip", low_memory=False).fillna("")
    except Exception:
        try:
            df = pd.read_csv(path, dtype=str, encoding="latin-1", on_bad_lines="skip", low_memory=False).fillna("")
        except Exception:
            return (False, 0)
    n = len(df)
    if n == 0:
        return (False, 0)
    atc_cols = [c for c in df.columns if "atc" in c.lower()]
    if not atc_cols:
        return (False, n)
    # rijen met minstens één geldige ATC5 over de atc-kolommen
    hits = sum(
        1 for _, row in df[atc_cols].iterrows()
        if any(ATC5_RE.match(str(row[c]).strip().upper()[:7]) for c in atc_cols)
    )
    return (hits >= 1, n)  # elke CSV met >=1 geldige ATC "heeft ATC" (verse zonder ATC valt terug op verrijkt)


groups = defaultdict(list)
for p in glob.glob(os.path.join(OUT, "*_shortage_*.csv")):
    base = os.path.basename(p)
    prefix, date = base.split("_shortage_")[0], base.split("_shortage_")[1].replace(".csv", "")
    groups[prefix].append((date, p))

kept, moved = [], []
for prefix, items in groups.items():
    items.sort(reverse=True)  # nieuwste datum eerst
    scored = [(d, p, score(p)) for d, p in items]
    keep = next((p for d, p, (atc, n) in scored if atc), None)          # nieuwste MET ATC
    if keep is None:
        keep = next((p for d, p, (atc, n) in scored if n > 0), None)     # anders nieuwste niet-lege
    if keep is None:
        keep = items[0][1]                                               # anders nieuwste
    kept.append((prefix, os.path.basename(keep)))
    for d, p in items:
        if p != keep:
            shutil.move(p, os.path.join(BK, os.path.basename(p)))
            moved.append(os.path.basename(p))

print(f"[dedup] {len(kept)} landen behouden, {len(moved)} superseded verplaatst")
for prefix, fn in sorted(kept):
    print(f"    {fn}")
