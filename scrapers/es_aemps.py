"""Scraper for Spain AEMPS (Agencia Española de Medicamentos) via CIMA REST API."""

import time
import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class EsAempsScraper(BaseScraper):
    """Scraper for https://cima.aemps.es/cima/rest/psuministro"""

    API_URL = "https://cima.aemps.es/cima/rest/psuministro"
    DRUG_API_URL = "https://cima.aemps.es/cima/rest/medicamento"

    # tipoProblemaSuministro mapping
    TIPO_MAP = {
        1: "Problemas de fabricación",
        2: "Problemas de suministro de materias primas",
        3: "Otros problemas de suministro",
        4: "Problemas de calidad",
        5: "Problemas de suministro (con alternativa)",
        6: "Cese de comercialización temporal",
        7: "Problemas de suministro (sin alternativa, medicamento extranjero)",
    }

    def __init__(self):
        super().__init__(
            country_code="ES",
            country_name="Spain",
            source_name="AEMPS",
            base_url="https://cima.aemps.es",
        )

    def _lookup_substance(self, cn: str) -> str:
        """Look up active substance via CIMA medicines API using código nacional."""
        if not cn:
            return ""
        try:
            resp = requests.get(
                self.DRUG_API_URL,
                params={"cn": cn},
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code != 200:
                return ""
            data = resp.json()
            principios = data.get("principiosActivos", [])
            if principios:
                return ", ".join(
                    p.get("nombre", "").strip()
                    for p in principios
                    if p.get("nombre", "").strip()
                )
        except Exception:
            pass
        return ""

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        # Fetch all records (API supports pagesize)
        params = {"pagesize": 9999}
        response = requests.get(self.API_URL, params=params, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        data = response.json()

        total = data.get("totalFilas", 0)
        results = data.get("resultados", [])
        print(f"  Found {total} records, received {len(results)}")

        # Batch-lookup unique cn codes for active substances
        unique_cns = {str(item.get("cn", "")).strip() for item in results if item.get("cn")}
        print(f"  Looking up active substances for {len(unique_cns)} unique products...")
        cn_substance_map: dict[str, str] = {}
        for i, cn in enumerate(unique_cns):
            cn_substance_map[cn] = self._lookup_substance(cn)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_cns)} lookups done")
            time.sleep(0.1)  # Rate limit
        found = sum(1 for v in cn_substance_map.values() if v)
        print(f"  Substance found for {found}/{len(unique_cns)} products")

        records = []
        for item in results:
            # Dates are epoch milliseconds
            shortage_start = self._epoch_to_date(item.get("fini"))
            estimated_end = self._epoch_to_date(item.get("ffin"))

            tipo = item.get("tipoProblemaSuministro", 0)
            is_active = item.get("activo", True)

            if not is_active:
                status = "resolved"
            elif estimated_end and estimated_end < datetime.now().strftime("%Y-%m-%d"):
                status = "resolved"
            else:
                status = "shortage"

            cn = str(item.get("cn", "")).strip()

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": item.get("nombre", ""),
                "active_substance": cn_substance_map.get(cn, ""),
                "strength": "",
                "package_size": "",
                "product_no": cn,
                "shortage_start": shortage_start,
                "estimated_end": estimated_end,
                "status": status,
                "reason": self.TIPO_MAP.get(tipo, str(tipo)),
                "notes": item.get("observ", ""),
                "scraped_at": datetime.now().isoformat(),
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage records scraped")
        return df

    @staticmethod
    def _epoch_to_date(epoch_ms) -> str | None:
        if not epoch_ms:
            return None
        try:
            return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            return None
