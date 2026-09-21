"""Scraper for Canada Drug Shortages Canada (DSC) via the official API.

De publieke site healthproductshortages.ca zit sinds ~sep-2026 achter Cloudflare
(requests/cloudscraper/headless-browser worden geblokkeerd). De officiële DSC-API
(drugshortagescanada.ca/api/v1) is NIET geblokkeerd en is bedoeld voor aggregators;
hij vereist een gratis account. Zet de inloggegevens in Matchen_prk/.env:
    DSC_EMAIL=...
    DSC_PASSWORD=...

Flow: POST /api/v1/login (krijgt 'auth-token' in de response-header) -> GET
/api/v1/search met die header, gepagineerd. Geen ATC in de bron -> afgeleid uit de
werkzame stof door de verrijkingsstap.
"""
from __future__ import annotations

import os
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

    API = "https://www.drugshortagescanada.ca/api/v1"
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
            base_url="https://www.drugshortagescanada.ca",
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
                "drugshortagescanada.ca en zet de gegevens in Matchen_prk/.env")
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
            "dosage_form": form,
            "status": self.KEEP_STATUS[status_raw],
            "scraped_at": datetime.now().isoformat(),
        }

    def _fetch(self, filter_type: str) -> list[dict]:
        """Gepagineerd de search-API aflopen voor een filter_type (shortages/discontinuations)."""
        out, offset = [], 0
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
            out.extend(items)
            total = data.get("total") if isinstance(data, dict) else None
            offset += self.PAGE
            if total is not None and offset >= int(total):
                break
            if len(items) < self.PAGE:
                break
            time.sleep(0.2)
        return out

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        self._login()
        raw = self._fetch("shortages") + self._fetch("discontinuations")
        print(f"  API leverde {len(raw)} items")
        records = [rec for rec in (self._record(it) for it in raw) if rec]
        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
