"""Scraper for Sweden Läkemedelsverket shortage data via STS API."""

import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class SeLvScraper(BaseScraper):
    """Scraper for Läkemedelsverket STS (shortage tracking system) API."""

    API_URL = "https://www.lakemedelsverket.se/api/sts/search"
    PAGE_SIZE = 100

    STATUS_MAP = {
        "1": "upcoming",
        "2": "shortage",
        "3": "resolved",
    }

    def __init__(self):
        super().__init__(
            country_code="SE",
            country_name="Sweden",
            source_name="Lakemedelsverket",
            base_url="https://www.lakemedelsverket.se",
        )

    def _parse_date(self, date_str) -> str | None:
        if not date_str or date_str.startswith("0001"):
            return None
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    DETAIL_URL = "https://www.lakemedelsverket.se/api/sts/"

    def _haal_redenen(self, ids: list, workers: int = 6) -> dict:
        """id -> reden, via de detail-endpoint. Zes tegelijk; de bron is geen grote dienst."""
        from concurrent.futures import ThreadPoolExecutor

        uit, mislukt = {}, 0

        def een(pid):
            try:
                r = requests.get(self.DETAIL_URL, params={"id": pid}, timeout=30,
                                 headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code != 200:
                    return pid, None
                return pid, (r.json().get("reason") or "").strip()
            except Exception:                        # noqa: BLE001
                return pid, None

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for pid, reden in pool.map(een, ids):
                if reden is None:
                    mislukt += 1
                elif reden:
                    uit[pid] = reden
        print(f"  Redenen opgehaald: {len(uit)}/{len(ids)}"
              + (f" ({mislukt} mislukt)" if mislukt else ""))
        return uit

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_items = []
        skip = 0

        # Fetch ongoing + coming shortages
        while True:
            payload = {
                "shortageStatusOngoing": "true",
                "shortageStatusComing": "true",
                "skip": skip,
                "take": self.PAGE_SIZE,
            }
            response = requests.post(self.API_URL, json=payload, timeout=30,
                                     headers={"User-Agent": "Mozilla/5.0",
                                              "Content-Type": "application/json"})
            response.raise_for_status()
            data = response.json()

            items = data.get("packageShortageDocument", [])
            total = data.get("totalMatching", 0)
            all_items.extend(items)

            if skip == 0:
                print(f"  Total matching: {total}")

            if not items or skip + len(items) >= total:
                break
            skip += len(items)

        print(f"  Downloaded {len(all_items)} records")

        # De reden staat NIET in de zoekrespons maar wel achter GET /api/sts/?id=<id>.
        # Zweden stond daardoor op 0 redenen. Parallel ophalen, want het zijn er ruim duizend;
        # mislukt er een, dan blijft die ene reden leeg en draait de rest gewoon door.
        redenen = self._haal_redenen([it.get("id") for it in all_items if it.get("id")])

        records = []
        for item in all_items:
            status_raw = item.get("status", "")
            status = self.STATUS_MAP.get(status_raw, status_raw or "shortage")

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": item.get("compositeMedprodName", ""),
                "active_substance": item.get("activeSubstName", ""),
                "strength": item.get("strength", ""),
                "package_size": item.get("packageDescription", ""),
                "product_no": item.get("itemNo", ""),
                "npl_pack_id": item.get("nplPackId", ""),
                "atc_code": item.get("atcInfo", ""),
                "pharmaceutical_form": item.get("pharmFormSwe", ""),
                "shortage_start": self._parse_date(item.get("forecastStartDate")),
                "estimated_end": self._parse_date(item.get("forecastEndDate")),
                "actual_end": self._parse_date(item.get("actualEndDate")),
                "first_published": self._parse_date(item.get("firstPublicationDate")),
                "last_updated": self._parse_date(item.get("lastUpdate")),
                "reason": redenen.get(item.get("id"), ""),
                "status": status,
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df
