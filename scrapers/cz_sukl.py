"""Scraper for Czech Republic SÚKL (Státní ústav pro kontrolu léčiv) shortage data."""

import csv
import io
import json
import re
import time
import zipfile
from pathlib import Path

import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class CzSuklScraper(BaseScraper):
    """Scraper for SÚKL unavailable medicines API at prehledy.sukl.cz"""

    API_URL = "https://prehledy.sukl.cz/hsz/v1/nedostupne-lp"
    DRUG_API_URL = "https://prehledy.sukl.cz/dlp/v1/lecive-pripravky"

    # Open-datacatalogus met de maandelijkse DLP-export. Daarin zit dlp_lecivelatky.csv:
    # de enige plek waar SÚKL de stofnaam bij een stof-ID publiceert (zie _load_substances).
    OPENDATA_CATALOG = "https://opendata.sukl.cz/?q=katalog/databaze-lecivych-pripravku-dlp"
    SUBSTANCE_MEMBER = "dlp_lecivelatky.csv"

    CACHE_DIR = Path(__file__).resolve().parent / "_cache"

    # typ values: 1 = ?, 2 = ?
    TYPE_MAP = {
        1: "shortage",
        2: "shortage - monitored",
    }

    def __init__(self):
        super().__init__(
            country_code="CZ",
            country_name="Czech Republic",
            source_name="SUKL",
            base_url="https://prehledy.sukl.cz",
        )

    # ── Stofnamen ────────────────────────────────────────────────────────────────
    # De tekortenlijst noemt alleen een merknaam ('nazev'). De detail-API geeft per
    # product wél de werkzame stoffen, maar uitsluitend als ID's ("leciveLatky": [936]).
    # De ID->naam-tabel zit niet in de REST-API: /dlp/v1/lecive-latky loopt in een 504 en
    # 'lecive-latky' staat niet in de 24 toegestane ciselniky. Hij staat wél in de
    # maandelijkse open-data-export (dlp_lecivelatky.csv, KOD_LATKY;NAZEV_INN;NAZEV_EN;...).
    # Die halen we één keer op en cachen we op de URL (die de publicatiedatum bevat).

    def _substance_cache(self) -> Path:
        return self.CACHE_DIR / "cz_lecivelatky.csv"

    def _find_dlp_zip_url(self) -> str:
        """Zoek de actuele DLP-zip in de open-datacatalogus (URL bevat de datum)."""
        resp = requests.get(self.OPENDATA_CATALOG, headers={"User-Agent": "Mozilla/5.0"},
                            timeout=60)
        resp.raise_for_status()
        urls = re.findall(r'href="(https://opendata\.sukl\.cz/soubory/[^"]*DLP\d+\.zip)"',
                          resp.text)
        return sorted(urls)[-1] if urls else ""

    def _load_substances(self) -> dict[str, str]:
        """{stof-ID: stofnaam} uit dlp_lecivelatky.csv, met cache op de bron-URL."""
        stamp_path = self.CACHE_DIR / "cz_lecivelatky.stamp"
        cache_path = self._substance_cache()

        try:
            url = self._find_dlp_zip_url()
        except requests.RequestException as exc:
            print(f"  LET OP: DLP-catalogus onbereikbaar ({exc}) — val terug op cache")
            url = ""

        cached_url = ""
        if stamp_path.exists():
            try:
                cached_url = json.loads(stamp_path.read_text()).get("url", "")
            except (OSError, ValueError):
                cached_url = ""

        # Cache geldig zolang SÚKL dezelfde export publiceert.
        if cache_path.exists() and (not url or url == cached_url):
            try:
                rows = list(csv.reader(io.StringIO(
                    cache_path.read_text(encoding="utf-8")), delimiter=";"))
                tabel = {r[0]: r[1] for r in rows if len(r) >= 2 and r[0]}
                if tabel:
                    print(f"  stoftabel uit cache: {len(tabel)} stoffen")
                    return tabel
            except OSError as exc:
                print(f"  cache stoftabel onbruikbaar ({exc}) — opnieuw ophalen")

        if not url:
            print("  LET OP: geen DLP-export gevonden — stofnamen blijven leeg")
            return {}

        print(f"  stoftabel ophalen uit {url.rsplit('/', 1)[-1]}...")
        blob = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=300)
        blob.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(blob.content)) as z:
            ruw = z.read(self.SUBSTANCE_MEMBER).decode("cp1250")

        tabel: dict[str, str] = {}
        for rij in csv.reader(io.StringIO(ruw), delimiter=";"):
            # KOD_LATKY;NAZEV_INN;NAZEV_EN;NAZEV;ZAV -- NAZEV_EN is de Engelse stofnaam
            # (dichtst bij de INN die andere landen leveren), NAZEV_INN is de Latijnse.
            if len(rij) < 4 or not rij[0].strip().isdigit():
                continue
            naam = rij[2].strip() or rij[1].strip()
            if naam:
                tabel[rij[0].strip()] = naam

        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8", newline="") as fh:
            csv.writer(fh, delimiter=";").writerows(tabel.items())
        stamp_path.write_text(json.dumps({"url": url}))
        print(f"  stoftabel geladen: {len(tabel)} stoffen")
        return tabel

    def _lookup_detail(self, kod_sukl: str) -> tuple[str, list[str]]:
        """ATC-code én stof-ID's ophalen via de SÚKL geneesmiddel-detail-API.

        De lijst-API (nedostupne-lp) geeft geen ATC en geen stof; de detail-API geeft
        'ATCkod' en 'leciveLatky' (stof-ID's, bv. [936]). Beide komen uit dezelfde
        response, dus de stofnaam kost geen extra request.
        """
        if not kod_sukl:
            return "", []
        try:
            resp = requests.get(
                f"{self.DRUG_API_URL}/{kod_sukl}",
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            if resp.status_code != 200:
                return "", []
            data = resp.json()
            if isinstance(data, list):
                data = data[0] if data else {}
            ids = [str(x).strip() for x in (data.get("leciveLatky") or []) if str(x).strip()]
            return str(data.get("ATCkod", "")).strip().upper(), ids
        except Exception:
            return "", []

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        response = requests.get(self.API_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0",
                                         "Accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        print(f"  Downloaded {len(data)} records from API")

        # Stof-ID -> stofnaam (één keer, uit de open-data-export; daarna gecachet)
        substances = self._load_substances()

        # Per uniek product de ATC-code én de stof-ID's ophalen via de detail-API
        unique_kods = {str(item.get("kodSUKL", "")).strip() for item in data if item.get("kodSUKL")}
        print(f"  ATC en stof opzoeken voor {len(unique_kods)} unieke producten...")
        kod_atc_map: dict[str, str] = {}
        kod_sub_map: dict[str, str] = {}
        for i, kod in enumerate(unique_kods):
            atc, stof_ids = self._lookup_detail(kod)
            kod_atc_map[kod] = atc
            namen: list[str] = []
            for sid in stof_ids:
                naam = substances.get(sid, "")
                if naam and naam not in namen:      # [936, 936] -> één keer
                    namen.append(naam)
            kod_sub_map[kod] = " + ".join(namen)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_kods)}")
            time.sleep(0.1)
        found = sum(1 for v in kod_atc_map.values() if v)
        found_sub = sum(1 for v in kod_sub_map.values() if v)
        print(f"  ATC gevonden voor {found}/{len(unique_kods)} producten")
        print(f"  Stofnaam gevonden voor {found_sub}/{len(unique_kods)} producten")

        records = []
        for item in data:
            kod = str(item.get("kodSUKL", "")).strip()
            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": str(item.get("nazev", "")).strip(),
                "active_substance": kod_sub_map.get(kod, ""),
                "strength": str(item.get("doplnek", "")).strip(),
                "package_size": "",
                "product_no": kod,
                "shortage_start": item.get("platOd", ""),
                "estimated_end": item.get("platDo", ""),
                "status": self.TYPE_MAP.get(item.get("typ"), "shortage"),
                "atc_code": kod_atc_map.get(kod, ""),
                "reference_no": str(item.get("cisloJednaciOd", "")).strip(),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
