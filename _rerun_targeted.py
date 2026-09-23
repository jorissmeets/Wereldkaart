"""Gerichte rerun: alleen de landen die op de kaart staan, met een harde time-out
per scraper (SIGALRM) zodat één trage/hangende bron de batch niet blokkeert.
Landcodes optioneel via argv (bv. `python _rerun_targeted.py CO HR RO SA`)."""
import inspect, signal, sys, time
import scrapers as scr

DEFAULT = {"AT", "AU", "BE", "CA", "CH", "CO", "DE", "DK", "ES", "FI", "FR", "GR", "HR",
           "HU", "IE", "IS", "IT", "JP", "KR", "LV", "MY", "NO", "PT", "RO", "SA", "SE",
           "SI", "SK", "TW", "US", "ZA"}
ALLOWED = set(a.upper() for a in sys.argv[1:]) or DEFAULT
TIMEOUT = 150  # seconden per scraper

# Uitzonderingen. Deze bronnen zijn aantoonbaar traag, niet kapot: Spanje liep in twee
# opeenvolgende runs over de 150 seconden en viel daardoor elke keer af, terwijl de scraper
# het gewoon doet. De limiet is er om een HANGENDE bron te stoppen, niet om een langzame te
# straffen. Ruimer per land is beter dan de drempel voor iedereen omhoog: dan duurt het
# slechtste geval (alles hangt) niet onnodig lang.
RUIMER = {
    "ES": 420,   # AEMPS pagineert traag door ~900 meldingen
    "SK": 420,   # SUKL bouwt een toestandsmachine over 17.904 meldingen
    "CZ": 300,   # SUKL-CZ doet een detailaanroep per melding
    "SE": 300,   # sinds de reden per melding wordt opgehaald (1.061 extra GETs)
    "BG": 300,   # APEX-app, traag bij elke stap
}


class Timeout(Exception):
    pass


def _handler(signum, frame):
    raise Timeout()


signal.signal(signal.SIGALRM, _handler)

scrapers = [c() for name, c in inspect.getmembers(scr, inspect.isclass)
            if name.endswith("Scraper") and name != "BaseScraper"]
scrapers = sorted((s for s in scrapers if getattr(s, "country_code", "") in ALLOWED),
                  key=lambda s: s.country_code)
print(f"Gericht rerun: {len(scrapers)} scrapers", flush=True)

ok = to = err = 0
for s in scrapers:
    t0 = time.time()
    limiet = RUIMER.get(s.country_code, TIMEOUT)
    signal.alarm(limiet)
    try:
        df = s.scrape()
        s.save_csv(df)
        signal.alarm(0)
        print(f"OK   {s.country_code} {s.source_name}: {len(df)} rijen ({time.time()-t0:.0f}s)", flush=True)
        ok += 1
    except Timeout:
        signal.alarm(0)
        print(f"TIME {s.country_code} {s.source_name}: >{limiet}s -> overgeslagen", flush=True)
        to += 1
    except Exception as e:
        signal.alarm(0)
        print(f"ERR  {s.country_code} {s.source_name}: {str(e)[:90]}", flush=True)
        err += 1

print(f"\nKlaar: {ok} ok, {to} timeout, {err} fout", flush=True)
