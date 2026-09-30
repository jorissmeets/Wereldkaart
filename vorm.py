"""Grove toedieningsvormklasse: oraal of parenteraal, of onbekend.

Alleen bedoeld om een PRK-koppeling te weigeren die over die grens gaat. Op 30-09 hing de
Franse injectie "Endoxan 500 et 1000 mg, poudre pour solution injectable" aan PRK 957,
cyclofosfamide DRAGEE 50 mg. De stof klopte, dus een ATC-toets ziet het niet.

Bewust smal. Een eerdere versie telde elk "Pulver zur Herstellung" en elk "polvo para
solución" als parenteraal, en dan wordt Movicol ("... einer Lösung zum Einnehmen") een
injectie. Poeder is geen vorm; waar het poeder voor is, wel. Daarom tellen hier alleen
woorden die zelf injectie, infuus of iets ter inname betekenen, in de talen die we binnen-
krijgen. Onbekend is geen botsing: weigeren op niet-weten kost meer dan het oplevert.

Gedeeld door _merge_live.py (lenen) en toets_prk_atc.py (controle achteraf), zodat die
twee niet uit elkaar kunnen gaan lopen.
"""
import re

# Parenteraal: injectie, infuus, en de Oostenrijkse "Trockenstechampulle" (droge ampul
# voor injectie). Per taal het stamwoord, zodat vervoegingen meetellen.
_PARENT = re.compile(
    r"inject|injekt|injekc|inyect|iniett|injiz"          # EN FR DE/NO/FI SI/HR ES IT
    r"|infus|infuz|perfus"                                # EN DE NL / HR SI / FR ES
    r"|trockenstech|ampul|ampoule|\bvial\b|flacon"         # AT, ampul, flacon
    r"|i\.v\.|\bi\.m\.|parenter|subcutan|intraven|intramusc"
    r"|\bINJ|\bINF\b|INFVLST|INFOPL|INJPDR|INJSUSP|INJVLST|INFUSIEPOEDER|INJECTIEPOEDER",
    re.I)

# Oraal: vaste orale vormen en uitdrukkelijk "ter inname".
_ORAAL = re.compile(
    r"tablet|comprim|dragee|dragée|capsul|kapsel|kapsl|filmtabl|\btabl\b"
    r"|zum einnehmen|buvable|oral\b|\boral|per os|peroral"
    r"|siroop|sirop|jarabe|sciroppo|mikstur|drank"
    r"|\bTABLET|\bCAPSULE|\bDRAGEE|GRANULAAT|\bDRANK",
    re.I)


def vormklasse(tekst):
    t = str(tekst or "")
    p, o = bool(_PARENT.search(t)), bool(_ORAAL.search(t))
    if p and not o:
        return "parenteraal"
    if o and not p:
        return "oraal"
    # Beide of geen van beide: niet uit te maken. Zo'n tekst beslist niets.
    return None


def vorm_botst(melding, prk_naam):
    """True als de melding duidelijk een andere toedieningsvorm heeft dan het PRK."""
    a = vormklasse(" ".join(str(melding.get(k) or "") for k in ("mn", "df", "tv")))
    b = vormklasse(prk_naam)
    return bool(a and b and a != b)
