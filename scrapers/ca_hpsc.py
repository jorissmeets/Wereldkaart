"""Scraper voor Canada: Drug Shortages Canada (DSC), met een publieke terugval.

De dienst is per 18-01-2026 verhuisd naar healthproductshortages.ca. Alles buiten
/api/v1 zit achter Cloudflare -- ook voor een echte headless browser -- dus de site
zelf scrapen is geen optie.

Twee wegen:
  1. MET account: de volledige DSC-lijst (~28k meldingen) via /api/v1. Vereist een
     gratis account op healthproductshortages.ca; zet in Matchen_prk/.env:
         DSC_EMAIL=...
         DSC_PASSWORD=...
     Flow: POST /api/v1/login (auth-token in de response-header) -> gepagineerde
     GET /api/v1/search. Limiet 1.000 requests per uur.
  2. ZONDER account: de Tier 3-lijst op canada.ca, de tekorten met de grootste
     verwachte impact. Maar 26 meldingen, maar wel actueel en publiek.

Bewust geen harde fout meer bij ontbrekende inloggegevens: dat liet Canada maandenlang
op data van maart staan (28.358 records waarvan 22.507 al opgelost) terwijl er publiek
wel actuele tekorten beschikbaar waren.

Geen ATC in de bron -> die wordt afgeleid uit de werkzame stof door de verrijkingsstap.
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime

import requests

try:
    from dotenv import load_dotenv
    for _p in ("/Users/karkara/Documents/LCG/Matchen_prk/.env",
               "/Users/karkara/Documents/LCG/Landkaart/.env"):
        if os.path.exists(_p):
            load_dotenv(_p)
except Exception:
    pass

import pandas as pd

from scrapers.base_scraper import BaseScraper


class CaHpscScraper(BaseScraper):
    """Drug Shortages Canada via de officiële API (auth-token)."""

    # De dienst is per 18-01-2026 verhuisd van drugshortagescanada.ca naar
    # healthproductshortages.ca; het oude adres antwoordt nog met een 301. Alles buiten
    # /api/v1 zit achter Cloudflare, ook voor een echte browser.
    # LET OP DE HOSTNAAM: healthproductshortages.ca bestaat WEL, maar
    # www.healthproductshortages.ca is NXDOMAIN -- die subdomeinnaam is nooit aangemaakt.
    # Met 'www.' ervoor loopt elke aanroep dood op een DNS-fout, ook mét geldige
    # inloggegevens. Zonder 'www.' antwoordt /api/v1/search met 400 (route bestaat,
    # token ontbreekt) en /api/v1/drugs met 404 -- de API leeft dus.
    API = "https://healthproductshortages.ca/api/v1"

    # Terugval zonder account: Health Canada publiceert de Tier 3-bepalingen (de tekorten met
    # de grootste verwachte impact) als gewone HTML op canada.ca. Dat is maar een fractie van
    # de DSC-lijst, maar het is WEL actueel en vergt geen inloggegevens -- beter dan een land
    # dat op maanden oude data blijft staan.
    # VALKUIL: canada.ca antwoordt hier alleen op de STANDAARD python-requests User-Agent.
    # De browser-UA die deze scraper voor de API zet, veroorzaakt op dat adres een timeout.
    TIER3_URL = ("https://www.canada.ca/en/health-canada/services/drugs-health-products/"
                 "drug-products/drug-shortages/tier-3-shortages.html")
    PAGE = 100
    # Statussen die we als lopend/relevant meenemen (opgeloste laten we weg — validatie Jesper 28-08).
    KEEP_STATUS = {
        "active_confirmed": "shortage",
        "anticipated_shortage": "anticipated",
        "to_be_discontinued": "to be discontinued",
        "discontinued": "discontinued",
    }

    def __init__(self):
        super().__init__(
            country_code="CA",
            country_name="Canada",
            source_name="DSC",
            base_url="https://healthproductshortages.ca",
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "application/json",
        })

    # ---- helpers ----------------------------------------------------------
    def _login(self) -> str:
        email = os.environ.get("DSC_EMAIL")
        password = os.environ.get("DSC_PASSWORD")
        if not email or not password:
            raise RuntimeError(
                "DSC_EMAIL/DSC_PASSWORD ontbreken. Registreer een gratis account op "
                "healthproductshortages.ca en zet de gegevens in Matchen_prk/.env")
        # De API accepteert form-encoded login; token komt terug in de 'auth-token'-header.
        resp = self.session.post(f"{self.API}/login",
                                 data={"email": email, "password": password}, timeout=30)
        token = resp.headers.get("auth-token") or resp.headers.get("Auth-Token")
        if not token:
            try:
                token = (resp.json() or {}).get("auth_token") or (resp.json() or {}).get("token")
            except Exception:
                token = None
        if not token:
            raise RuntimeError(f"Login mislukt (status {resp.status_code}): geen auth-token ontvangen")
        self.session.headers.update({"auth-token": token})
        return token

    @staticmethod
    def _txt(v) -> str:
        if isinstance(v, dict):
            return str(v.get("en") or v.get("name") or v.get("label") or "").strip()
        if isinstance(v, list):
            return ", ".join(CaHpscScraper._txt(x) for x in v if CaHpscScraper._txt(x))
        return "" if v is None else str(v).strip()

    def _field(self, item: dict, drug: dict, *keys) -> str:
        for k in keys:
            for src in (item, drug):
                if k in src and src[k] not in (None, ""):
                    return self._txt(src[k])
        return ""

    def _record(self, item: dict) -> dict | None:
        status_raw = str(item.get("status", "")).strip().lower()
        if status_raw not in self.KEEP_STATUS:
            return None
        drug = item.get("drug") if isinstance(item.get("drug"), dict) else {}
        name = self._field(item, drug, "en_drug_brand_name", "drug_brand_name", "brand_name", "name")
        substance = self._field(item, drug, "active_ingredients", "active_ingredient", "ingredients")
        company = self._field(item, drug, "company_name", "company")
        din = self._field(item, drug, "din", "drug_din")
        strength = self._field(item, drug, "strength", "drug_strength")
        form = self._field(item, drug, "dosage_form", "drug_dosage_form", "form")
        start = self._field(item, drug, "actual_shortage_start_date", "actual_start_date",
                            "anticipated_start_date", "shortage_start_date")
        end = self._field(item, drug, "estimated_end_date", "actual_shortage_end_date",
                          "actual_end_date", "estimated_shortage_end_date")
        # De ENIGE datum die Canada in de praktijk vult. In de export van 13-03-2026 was de
        # bijwerkdatum voor alle 28.358 records gevuld (2017-03-13 t/m 2026-03-13), terwijl
        # shortage_start, estimated_end en package_size voor 100% leeg waren. build_data leest
        # die kolom bewust ('last_updated or update_date') voor de >1-jaar-inactiefregel.
        # Zonder deze kolom heeft GEEN ENKEL Canadees record een datum, vuurt die regel nooit,
        # en komt alles als 'actief' op de kaart -- precies de fout die tekorten uit 2019
        # actief liet staan. Leeg laten als de bron niets levert; nooit de scrapedatum invullen.
        updated = self._field(item, drug, "updated_date", "update_date", "last_updated",
                              "updated_at", "en_updated_date", "date_updated")
        if not name and not substance:
            return None
        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": name,
            "active_substance": substance,
            "strength": strength,
            "package_size": "",
            "product_no": din,
            "marketing_auth_holder": company,
            "shortage_start": (start or "")[:10],
            "estimated_end": (end or "")[:10],
            "update_date": (updated or "")[:10],
            "dosage_form": form,
            "status": self.KEEP_STATUS[status_raw],
            "scraped_at": datetime.now().isoformat(),
        }

    def _fetch(self, filter_type: str) -> list[dict]:
        """Gepagineerd de search-API aflopen voor een filter_type (shortages/discontinuations).

        LET OP -- de lijst schuift tijdens het pagineren. In de export van 13-03-2026 stonden
        266 paren rijen die op alle 18 bronkolommen identiek waren en alleen in scraped_at
        1,3 tot 4,2 seconden verschilden: precies een pagina-aanroep uit elkaar (de run deed
        ~2,3 s per pagina). De search-endpoint hersorteert dus tussen twee offset-aanroepen,
        waardoor een melding op de paginagrens tweemaal langskomt. Daarom hier ontdubbelen op
        het meldingsnummer.

        Diezelfde verschuiving kan een melding ook OVERSLAAN, en dat ziet een rijtelling per
        definitie niet. Daarom zetten we het aantal opgehaalde meldingen af tegen het totaal
        dat de bron ZELF noemt; wijkt dat af, dan zegt de run dat hardop in plaats van zich
        stil als geslaagd te melden.
        """
        out, offset, gezien, dubbel = [], 0, set(), 0
        bron_totaal = None
        while True:
            params = {"filter_type": filter_type, "term": "", "offset": offset, "limit": self.PAGE}
            r = self.session.get(f"{self.API}/search", params=params, timeout=45)
            if r.status_code != 200:
                print(f"  search {filter_type} offset {offset}: HTTP {r.status_code} — stop")
                break
            try:
                data = r.json()
            except Exception:
                break
            items = data.get("data") if isinstance(data, dict) else data
            if not items:
                break
            for it in items:
                rid = it.get("id") or it.get("report_id") or it.get("shortage_id") \
                    if isinstance(it, dict) else None
                if rid is not None:
                    if rid in gezien:
                        dubbel += 1
                        continue
                    gezien.add(rid)
                out.append(it)
            total = data.get("total") if isinstance(data, dict) else None
            if total is not None:
                bron_totaal = int(total)
            offset += self.PAGE
            if total is not None and offset >= int(total):
                break
            if len(items) < self.PAGE:
                break
            time.sleep(0.2)
        if dubbel:
            print(f"  {filter_type}: {dubbel} dubbel geleverde meldingen overgeslagen "
                  f"(pagineringsverschuiving)")
        if bron_totaal is not None and len(out) != bron_totaal:
            print(f"  LET OP {filter_type}: de bron noemt {bron_totaal} meldingen, "
                  f"opgehaald {len(out)} — er ontbreekt of dubbelt iets")
        return out

    def _scrape_tier3(self) -> list:
        """Health Canada's Tier 3-lijst: de tekorten met de grootste verwachte impact.

        Kolommen: Drug (Active Ingredient) | Date of Tier 3 Determination |
        TAC Committee Membership | Used in the treatment of.
        Er is geen einddatum en geen merknaam; alleen de werkzame stof en de datum waarop
        de tekortcommissie het als Tier 3 bestempelde. Die datum is de START van het tekort,
        niet de scrapedatum -- dat onderscheid is hier belangrijk.
        """
        from bs4 import BeautifulSoup
        # Bewust een KALE sessie: canada.ca weigert de browser-UA die de API-sessie zet.
        resp = requests.get(self.TIER3_URL, timeout=60)
        resp.raise_for_status()
        tabel = BeautifulSoup(resp.text, "html.parser").find("table")
        if tabel is None:
            raise RuntimeError("Tier 3-tabel niet gevonden op canada.ca")

        records = []
        for rij in tabel.find_all("tr"):
            cellen = [c.get_text(" ", strip=True) for c in rij.find_all("td")]
            if len(cellen) < 2 or not cellen[0]:
                continue
            datum = ""
            m = re.search(r"(\d{4})-(\d{2})-(\d{2})", cellen[1])
            if m:
                datum = m.group(0)
            else:                                   # "September 14, 2026"
                try:
                    datum = datetime.strptime(cellen[1].strip(), "%B %d, %Y").strftime("%Y-%m-%d")
                except ValueError:
                    datum = ""
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": "Health Canada Tier 3",
                "medicine_name": "",
                "active_substance": cellen[0],
                "strength": "",
                "package_size": "",
                "product_no": "",
                "marketing_auth_holder": "",
                "shortage_start": datum,
                "estimated_end": "",
                "status": "shortage",
                "reason": cellen[3] if len(cellen) > 3 else "",
                "notes": "Tier 3: tekort met de grootste verwachte impact",
                "scraped_at": datetime.now().isoformat(),
            })
        return records

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Met een account de volledige DSC-lijst; zonder account de Tier 3-lijst. Bewust géén
        # harde fout meer bij ontbrekende inloggegevens: dan bleef Canada op maanden oude data
        # staan terwijl er publiek wél actuele tekorten beschikbaar zijn.
        if os.environ.get("DSC_EMAIL") and os.environ.get("DSC_PASSWORD"):
            self._login()
            raw = self._fetch("shortages") + self._fetch("discontinuations")
            print(f"  API leverde {len(raw)} items")
            records = [rec for rec in (self._record(it) for it in raw) if rec]
        else:
            print("  geen DSC-inloggegevens -> terugval op de publieke Tier 3-lijst")
            records = self._scrape_tier3()

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
