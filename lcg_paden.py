"""Centrale padbepaling voor de tekortenpijplijn.

Alle scripts hadden /Users/karkara/... ingebakken. Dat werkt op precies een machine, en de
bedoeling is nu dat de wekelijkse verversing in GitHub Actions draait in plaats van op een
laptop die aan moet staan. Deze module lost de drie paden op die ertoe doen, in deze
volgorde: omgevingsvariabele, dan de opstelling op de Mac.

    LCG_BASE   de Landkaart-map (deze repo)
    LCG_PRK    de matcher-map (Matchen_prk): enrich_atc_llm.py, prk_match_country.py
    LCG_GSTD   de actuele G-standaard-export

LET OP -- EEN VALSTRIK DIE AL EEN KEER IS GEZET
Binnen de Landkaart-map staat OOK een submap Matchen_prk, maar dat is een ander, ouder
project (openai_atc_finder en verwanten, 52 MB). De matcher die werkelijk draait staat
ERNAAST, in /Users/karkara/Documents/LCG/Matchen_prk. Daarom is de terugval hier het
expliciete buurpad en NIET os.path.join(BASE, "Matchen_prk") -- dat zou stil de verkeerde
map pakken en pas opvallen als de PRK-koppeling niets meer oplevert.

In GitHub Actions worden alle drie via de omgeving gezet; dan raakt de terugval nooit aan bod.
"""
import os

_HIER = os.path.dirname(os.path.abspath(__file__))


def _eerste_bestaande(*paden: str) -> str:
    for p in paden:
        if p and os.path.exists(p):
            return p
    return paden[-1] if paden else ""


#: De Landkaart-map. Deze module staat in de wortel daarvan.
BASE = os.environ.get("LCG_BASE") or _HIER

#: De matcher-map. Zie de waarschuwing hierboven over de gelijknamige submap.
PRK = os.environ.get("LCG_PRK") or _eerste_bestaande(
    os.path.join(os.path.dirname(BASE), "Matchen_prk"),
    "/Users/karkara/Documents/LCG/Matchen_prk",
)

#: De actuele G-standaard. Valt terug op LCG.csv, de momentopname van april, maar die is
#: als noemer aantoonbaar te oud: hij miste 32 SFK-artikelen die de actuele export wel heeft.
GSTD = os.environ.get("LCG_GSTD") or _eerste_bestaande(
    os.path.join(BASE, "gstandaard_actueel.csv"),
    os.path.join(PRK, "LCG.csv"),
)


def pad(*delen: str) -> str:
    """Pad binnen de Landkaart-map."""
    return os.path.join(BASE, *delen)


def prk_pad(*delen: str) -> str:
    """Pad binnen de matcher-map."""
    return os.path.join(PRK, *delen)


if __name__ == "__main__":
    for naam, waarde in (("LCG_BASE", BASE), ("LCG_PRK", PRK), ("LCG_GSTD", GSTD)):
        merk = "" if os.path.exists(waarde) else "   <-- BESTAAT NIET"
        bron = "uit omgeving" if os.environ.get(naam) else "terugval"
        print(f"{naam:10} {waarde}   ({bron}){merk}")
