"""Scraper for Colombia INVIMA vital unavailable medicines list."""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import tempfile
import os

from scrapers.base_scraper import BaseScraper


class CoInvimaScraper(BaseScraper):
    """Scraper for INVIMA (Colombia) vital unavailable medicines.

    Downloads the latest monthly Excel file from the INVIMA website
    listing medicines classified as "Vital No Disponible" (vital unavailable).
    """

    LIST_URL = (
        "https://www.invima.gov.co/productos-vigilados/"
        "medicamentos-y-productos-biologicos/"
        "medicamentos-vitales-no-disponibles"
    )
    BASE = "https://www.invima.gov.co"

    def __init__(self):
        super().__init__(
            country_code="CO",
            country_name="Colombia",
            source_name="INVIMA",
            base_url="https://www.invima.gov.co",
        )

    def _find_latest_excel_url(self) -> str:
        """Find the latest Excel download link from the listing page."""
        resp = requests.get(
            self.LIST_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"]
            if "vitales" in href.lower() and "excel" in text and "pdf" not in href.lower():
                if href.startswith("/"):
                    href = self.BASE + href
                return href

        raise ValueError("Could not find Excel download link on INVIMA listing page")

    def _resolve_download(self, url: str) -> bytes:
        """Resolve a URL to actual file bytes.

        Some links point to /biblioteca/ pages that contain the real
        download link, while others are direct .xls file URLs.
        """
        resp = requests.get(
            url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30
        )
        resp.raise_for_status()

        # Check if response is an actual Excel file (OLE or ZIP signature)
        if resp.content[:4] in (b"\xd0\xcf\x11\xe0", b"PK\x03\x04"):
            return resp.content

        # Otherwise it's an HTML page — find the download link
        soup = BeautifulSoup(resp.text, "lxml")
        dl_link = soup.find("a", href=re.compile(r"/biblioteca/download/"))
        if dl_link:
            dl_url = dl_link["href"]
            if dl_url.startswith("/"):
                dl_url = self.BASE + dl_url
            resp2 = requests.get(
                dl_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60
            )
            resp2.raise_for_status()
            return resp2.content

        raise ValueError(f"Could not resolve download from {url}")

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        excel_url = self._find_latest_excel_url()
        print(f"  Found Excel: {excel_url[:80]}...")

        content = self._resolve_download(excel_url)
        print(f"  Downloaded {len(content)} bytes")

        # Save to temp file
        tmp_path = os.path.join(tempfile.gettempdir(), "co_invima.xls")
        with open(tmp_path, "wb") as f:
            f.write(content)

        # Find header row (contains "PRODUCTO" or "PRINCIPIO")
        raw = pd.read_excel(tmp_path, header=None)
        header_row = None
        for i in range(min(20, len(raw))):
            row_text = " ".join(
                str(v).upper() for v in raw.iloc[i].values if pd.notna(v)
            )
            if "PRODUCTO" in row_text and "FORMA" in row_text:
                header_row = i
                break

        if header_row is None:
            header_row = 11  # fallback

        data = pd.read_excel(tmp_path, header=header_row)
        print(f"  Raw rows: {len(data)}")

        # Standardize column names
        col_map = {}
        for c in data.columns:
            cu = str(c).strip().upper()
            if "NÚMERO" in cu or "NUMERO" in cu:
                col_map[c] = "number"
            elif "PRODUCTO" in cu or "PRINCIPIO" in cu:
                col_map[c] = "product"
            elif "FORMA" in cu:
                col_map[c] = "dosage_form"
            elif "CONCENTRA" in cu:
                col_map[c] = "strength"
            elif "NORMA" in cu:
                col_map[c] = "pharma_code"
            elif "ACTA" in cu:
                col_map[c] = "act_reference"
            elif "IUM" in cu:
                col_map[c] = "ium_code"

        data = data.rename(columns=col_map)

        # Drop rows without a product name
        if "product" in data.columns:
            data = data.dropna(subset=["product"])
            # Filter out "Excluido" rows
            data = data[
                ~data["product"].astype(str).str.contains(
                    "excluido", case=False, na=False
                )
            ]

        records = []
        for _, row in data.iterrows():
            product = str(row.get("product", "")).strip()
            if not product or product.lower() == "nan":
                continue

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": product,
                "active_substance": "",
                "strength": str(row.get("strength", "")).strip() if pd.notna(row.get("strength")) else "",
                "package_size": "",
                "dosage_form": str(row.get("dosage_form", "")).strip() if pd.notna(row.get("dosage_form")) else "",
                "pharma_code": str(row.get("pharma_code", "")).strip() if pd.notna(row.get("pharma_code")) else "",
                "act_reference": str(row.get("act_reference", "")).strip() if pd.notna(row.get("act_reference")) else "",
                "ium_code": str(row.get("ium_code", "")).strip() if pd.notna(row.get("ium_code")) else "",
                "status": "vital_unavailable",
                "scraped_at": datetime.now().isoformat(),
            })

        # Clean up
        try:
            os.remove(tmp_path)
        except OSError:
            pass

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} vital unavailable medicine records scraped")
        return df
