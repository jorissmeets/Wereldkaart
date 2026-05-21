"""Scraper for New Zealand — Pharmac Medicine Notices (skeleton).

Pharmac (`pharmac.govt.nz/medicine-funding-and-supply/medicine-notices`) publiceert per
geneesmiddel een aparte detailpagina. De index-pagina lijkt vooral nav-/footer-HTML te
bevatten — de werkelijke lijst van notices komt waarschijnlijk via een JS-call.

**Alternatieven om de listing te krijgen:**
1. Sitemap: `https://www.pharmac.govt.nz/sitemap.xml` — daar zouden alle medicine-notice
   slug-URL's in moeten staan, gefilterd op pad `/medicine-funding-and-supply/medicine-notices/`.
2. RSS via NZF (New Zealand Formulary) — bevat volgens Pharmac wekelijks bijgewerkte
   index van current supply notifications.
3. Browser-network-tab om de JS-call af te kijken.

Detailpagina-voorbeelden:
- https://www.pharmac.govt.nz/medicine-funding-and-supply/medicine-notices/methylphenidate
- https://www.pharmac.govt.nz/medicine-funding-and-supply/medicine-notices/nifedipine-20-mg
- https://www.pharmac.govt.nz/medicine-funding-and-supply/medicine-notices/mercaptopurine

Per detailpagina vind je medicijnnaam, sterkte, verpakkingsgrootte, datum, oorzaak en
of de melding actief of opgelost is.
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class NzPharmacScraper(BaseScraper):
    """New Zealand Pharmac medicine supply notices — skeleton."""

    INDEX_URL = "https://www.pharmac.govt.nz/medicine-funding-and-supply/medicine-notices"
    SITEMAP_URL = "https://www.pharmac.govt.nz/sitemap.xml"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml",
    }

    def __init__(self) -> None:
        super().__init__(
            country_code="NZ",
            country_name="New Zealand",
            source_name="Pharmac",
            base_url="https://www.pharmac.govt.nz",
        )

    # ─── Notice-URL's vinden via sitemap ────────────────────────────────────

    def _discover_notice_urls(self) -> list[str]:
        """Pak alle URL's onder /medicine-funding-and-supply/medicine-notices/ uit de sitemap."""
        try:
            resp = requests.get(self.SITEMAP_URL, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException:
            return []

        soup = BeautifulSoup(resp.text, "lxml-xml")
        urls: list[str] = []
        for loc in soup.find_all("loc"):
            url = loc.get_text(strip=True)
            if "/medicine-funding-and-supply/medicine-notices/" in url and not url.rstrip("/").endswith("medicine-notices"):
                urls.append(url)
        return urls

    # ─── Detailpagina parsen ────────────────────────────────────────────────

    def _parse_notice(self, url: str) -> dict | None:
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=20)
            resp.raise_for_status()
        except requests.RequestException:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        title = soup.find("h1")
        title_text = title.get_text(" ", strip=True) if title else ""

        # Pharmac-titels volgen vaak een pattern:
        # "Methylphenidate for ADHD (Concerta, Ritalin, Rubifen, Teva): Supply shortages"
        # "Nifedipine (Nyefax Retard) Tab long-acting 20 mg: Supply issue resolved"
        med_match = re.match(r"^([^:]+?)\s*:\s*(.+)$", title_text)
        medicine_name = med_match.group(1).strip() if med_match else title_text
        status_phrase = med_match.group(2).strip().lower() if med_match else ""

        if "resolved" in status_phrase:
            status = "resolved"
        elif "discontin" in status_phrase:
            status = "discontinued"
        elif "brand chang" in status_phrase:
            status = "brand_change"
        elif "supply" in status_phrase or "shortage" in status_phrase:
            status = "shortage"
        else:
            status = "notice"

        # Werkzame stof: probeer eerste woord uit de titel
        first_word = medicine_name.split()[0] if medicine_name else ""

        # Datum extractie: zoek datums in de bodytekst
        body_text = soup.get_text(" ", strip=True)
        date_match = re.search(r"(\d{1,2})\s+([A-Z][a-z]+)\s+(\d{4})", body_text)
        shortage_start = None
        if date_match:
            try:
                d = datetime.strptime(
                    f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}",
                    "%d %B %Y",
                )
                shortage_start = d.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine_name,
            "active_substance": first_word,
            "strength": "",
            "package_size": "",
            "atc_code": "",
            "shortage_start": shortage_start,
            "estimated_end": None,
            "status": status,
            "notice_title": title_text,
            "notice_url": url,
            "scraped_at": datetime.now().isoformat(),
        }

    # ─── Main ───────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        urls = self._discover_notice_urls()
        print(f"  Found {len(urls)} medicine-notice URLs via sitemap")

        records: list[dict] = []
        for i, url in enumerate(urls):
            if (i + 1) % 20 == 0:
                print(f"    ... {i + 1}/{len(urls)} pages")
            rec = self._parse_notice(url)
            if rec and rec["medicine_name"]:
                records.append(rec)

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} NZ notice records")
        return df
