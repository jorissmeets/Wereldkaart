"""Sterkte uit een productnaam halen en twee sterktes vergelijken.

Gebruikt door toets_prk_atc.py: een PRK is een Nederlands product van EEN sterkte. Hangt een
buitenlandse melding van 150 mg aan de PRK van 500 mg, dan wijst het dashboard het verkeerde
Nederlandse product als geraakt aan (Sloveense Ecansya 150 mg, Roemeense Endoxan 200 mg en 1 g
hingen alle aan de 500 mg-PRK).

Bewust behoudend. Er wordt alleen geoordeeld als BEIDE namen precies een sterkte noemen, in
dezelfde soort eenheid, en pas bij een verschil groter dan een factor 1,25: zout en base schelen
vaak een paar procent (metoprololsuccinaat 47,5 mg tegenover "50 mg"), naburige sterktes bijna
altijd een factor 1,5 of meer. Combinaties ("500 et 1000 mg", "5/20 mg"), doseringen per uur of
per dosis en losse volumes tellen niet mee.
"""
import re
import unicodedata

_MASSA = {"MG": 1.0, "G": 1000.0, "GR": 1000.0, "GM": 1000.0, "UG": 0.001, "MCG": 0.001, "µG": 0.001, "ΜG": 0.001,
          "MICROGRAM": 0.001, "MICROGRAMS": 0.001, "MIKROGRAM": 0.001, "MIKROGRAMOV": 0.001,
          "MIKROGRAMA": 0.001, "MIKROGRAMM": 0.001, "MICROGRAMMES": 0.001, "MICROGRAMOS": 0.001,
          "MICROGRAMMI": 0.001, "MIKROG": 0.001, "NG": 0.000001}
_EENHEID = {"IE": "IE", "IU": "IE", "UI": "IE", "E": "IE", "MMOL": "MMOL", "%": "%"}
_PER = {"ML": "ml", "G": "g", "GR": "g"}
_TIJD_DOSIS = re.compile(r"^(H|UUR|24H|24UUR|DO|DOSIS|DOSE|DOSES|DAWKA|ACT|PUFF|HUB|SPRAY)$")

_EENHEDEN = sorted(list(_MASSA) + list(_EENHEID), key=len, reverse=True)
_RE = re.compile(
    r"(?<![A-Z0-9.])(\d+(?:\.\d+)?)\s*(" + "|".join(re.escape(e) for e in _EENHEDEN) + r")(?![A-Z])"
    r"(?:\s*/\s*(\d+(?:\.\d+)?)?\s*([A-Z]+))?")
# "500 et 1000 mg", "5/20 mg", "10 + 20 mg": een kale waarde vlak voor een voegwoord of streep
_MEERVOUD = re.compile(r"\d(?:\.\d+)?\s*(?:ET|AND|EN|Y|E|UND|OG|OCH|JA|I|/|\+|-)\s*\d+(?:\.\d+)?\s*(?:MG|G|UG|MCG|IE)\b")


def _norm(tekst: str) -> str:
    t = unicodedata.normalize("NFKC", str(tekst or "")).upper()
    t = re.sub(r"(\d),(\d)", r"\1.\2", t)
    # Duizendtallen voor een eenheid: "1 200 mg" (Noorse Nootropil), "1.000 mg", "25.000 IE".
    # Zonder dit werd 1 200 mg gelezen als 200 mg. Een eerste groep die met 0 begint is een
    # decimaal (0.125 mg digoxine) en blijft staan.
    duizend = r"(?<![\d.])([1-9]\d{0,2})[ .](\d{3})(?=\s*(?:MG|G|UG|MCG|IE|IU|UI|E|ML)(?![A-Z]))"
    for _ in range(2):
        t = re.sub(duizend, r"\1\2", t)
    return t.replace("Μ", "µ")


def sterktes(tekst: str):
    """Verzameling (waarde, eenheid, per) uit een naam; None als er een combinatie in staat."""
    t = _norm(tekst)
    if _MEERVOUD.search(t):
        return None
    uit = set()
    for m in _RE.finditer(t):
        waarde, eenheid, deler, per = float(m.group(1)), m.group(2), m.group(3), m.group(4)
        if eenheid in _MASSA:
            waarde, soort = waarde * _MASSA[eenheid], "mg"
        else:
            soort = _EENHEID[eenheid]
        if per:
            if _TIJD_DOSIS.match(per):
                return None                     # per uur of per dosis: niet vergelijkbaar
            if per not in _PER:
                if per in _MASSA:               # "50 mg / 12,5 mg": een combinatie
                    return None
                per = None
            else:
                waarde = waarde / (float(deler) if deler else 1.0)
                per = _PER[per]
        uit.add((round(waarde, 6), soort, per))
    return uit


def sterkte_botst(melding_naam: str, prk_naam: str) -> bool:
    """True als beide namen precies een sterkte noemen, van dezelfde soort, die meer dan een
    factor 1,25 verschillen. In elk ander geval False: niet beslissen is hier de veilige kant."""
    a, b = sterktes(melding_naam), sterktes(prk_naam)
    if not a or not b or len(a) != 1 or len(b) != 1:
        return False
    (va, sa, pa), = a
    (vb, sb, pb), = b
    if sa != sb or pa != pb or va <= 0 or vb <= 0:
        return False
    return max(va, vb) / min(va, vb) > 1.25
