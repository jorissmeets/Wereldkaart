"""Scraper for Poland — Ministerstwo Zdrowia (MZ) lijst leków zagrożonych brakiem dostępności.

Bron: Dziennik Urzędowy Ministra Zdrowia (https://dziennikmz.mz.gov.pl/keywords/55).
Het ministerie publiceert elke ~2 maanden een obwieszczenie met een lijst van geneesmiddelen
die met tekort dreigen. Elke obwieszczenie staat als PDF in de dziennikmz; de index-pagina is
een SPA, dus de lijst van publicaties wordt via een interne JSON-API geleverd.

Stand van zaken: skeleton met TODO's. De PDF parsing-tabelextractie is niet generiek
oplosbaar zonder eerst een paar concrete PDF's te inspecteren (tabula-py is in requirements
maar layout per uitgave kan verschillen).

Aanbevolen werkstappen om dit af te maken:
  1. In de browser dziennikmz.mz.gov.pl/keywords/55 openen, network tab → de JSON-call
     ophalen die de lijst publicaties retourneert. Vul `INDEX_API` hieronder in.
  2. Voor 2-3 historische obwieszczenia de PDF-tabelstructuur bekijken (kolommen, dosering,
     verpakkingsgrootte). Pas `_parse_pdf_table` aan.
  3. Velden mappen naar het BaseScraper-contract: medicine_name, active_substance, strength,
     package_size, status="shortage", shortage_start=publicatiedatum.
"""

from __future__ import annotations

import io
import re
from datetime import datetime
from typing import Iterable

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper


class PlMzScraper(BaseScraper):
    """Poland — Ministerstwo Zdrowia drug shortage list scraper (skeleton)."""

    # SPA-frontend; ware data komt uit een onbekende JSON-endpoint. TODO: invullen.
    INDEX_PAGE = "https://dziennikmz.mz.gov.pl/keywords/55"
    INDEX_API: str | None = None  # bv. "https://dziennikmz.mz.gov.pl/api/legalacts?keyword=55"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json, text/html",
    }

    def __init__(self) -> None:
        super().__init__(
            country_code="PL",
            country_name="Poland",
            source_name="MZ",
            base_url="https://dziennikmz.mz.gov.pl",
        )

    # ─── Index ophalen ──────────────────────────────────────────────────────

    def _list_publications(self) -> list[dict]:
        """Return list of dicts {publication_date, pdf_url, title} voor alle relevante obwieszczenia.

        TODO: implementeer met INDEX_API zodra die bekend is. Fallback: HTML scrapen indien
        de pagina server-side gerendered raakt.
        """
        if not self.INDEX_API:
            raise NotImplementedError(
                "INDEX_API niet gezet. Inspecteer dziennikmz.mz.gov.pl/keywords/55 in de "
                "browser-network-tab om de JSON-endpoint te vinden. Zie module-docstring."
            )

        resp = requests.get(self.INDEX_API, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # TODO: pas response-shape aan zodra bekend
        return [
            {
                "publication_date": item.get("publicationDate"),
                "pdf_url": item.get("pdfUrl") or item.get("attachmentUrl"),
                "title": item.get("title", ""),
            }
            for item in data
            if item.get("pdfUrl") or item.get("attachmentUrl")
        ]

    # ─── PDF parsen ─────────────────────────────────────────────────────────

    def _download_pdf(self, url: str) -> bytes:
        resp = requests.get(url, headers=self.HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.content

    def _parse_pdf_table(self, pdf_bytes: bytes) -> pd.DataFrame:
        """Extract de tabel uit een MZ-obwieszczenie PDF.

        TODO: bekijk eerst een echte PDF en bepaal:
          - kolomnamen (typisch: nazwa, postać, dawka, opakowanie)
          - of er één tabel doorloopt of meerdere per pagina staan
        Voor nu gebruiken we tabula-py (al in requirements). Bij complexe layout overstappen
        op pdfplumber of camelot.
        """
        try:
            import tabula  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "tabula-py niet beschikbaar. Installeer via requirements.txt."
            ) from e

        tables = tabula.read_pdf(io.BytesIO(pdf_bytes), pages="all", lattice=True)
        if not tables:
            return pd.DataFrame()

        # Heuristiek: pak de grootste tabel; in praktijk meestal samenvoegen nodig
        df = max(tables, key=len)
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # ─── Normaliseren naar BaseScraper-contract ─────────────────────────────

    def _normalize_row(self, row: pd.Series, publication_date: str | None) -> dict:
        """Zet één PDF-tabel-rij om naar een standaard shortage-record.

        TODO: mapping zal afhankelijk zijn van de echte PDF-kolomnamen. Voorbeeldveronderstelling:
            kolom 'Nazwa produktu leczniczego' → medicine_name
            kolom 'Substancja czynna'          → active_substance
            kolom 'Dawka'                      → strength
            kolom 'Wielkość opakowania'        → package_size
        """
        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": str(row.get("Nazwa produktu leczniczego", "")).strip(),
            "active_substance": str(row.get("Substancja czynna", "")).strip(),
            "strength": str(row.get("Dawka", "")).strip(),
            "package_size": str(row.get("Wielkość opakowania", "")).strip(),
            "atc_code": "",
            "shortage_start": publication_date,
            "estimated_end": None,
            "status": "at_risk_of_shortage",  # NB: PL is preventieve lijst, geen actuele tekort
            "notification_type": "ministerial_announcement",
            "scraped_at": datetime.now().isoformat(),
        }

    # ─── Main ───────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        publications = self._list_publications()
        print(f"  Found {len(publications)} publications")

        # Pak de meest recente obwieszczenie
        publications.sort(key=lambda p: p["publication_date"] or "", reverse=True)
        latest = publications[0] if publications else None
        if latest is None:
            print("  Geen publicaties gevonden")
            return pd.DataFrame()

        print(f"  Verwerken: {latest['title']} ({latest['publication_date']})")
        pdf_bytes = self._download_pdf(latest["pdf_url"])
        table = self._parse_pdf_table(pdf_bytes)

        records: list[dict] = []
        for _, row in table.iterrows():
            rec = self._normalize_row(row, latest["publication_date"])
            if rec["medicine_name"]:
                records.append(rec)

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} records")
        return df
