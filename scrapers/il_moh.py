"""Scraper for Israel MOH (Ministry of Health) drug shortage data.

Uses the Israeli Drug Registry API at israeldrugs.health.gov.il to retrieve
drugs that have been cancelled or are in shortage status. The API is an
AngularJS-backed service with POST endpoints under /GovServiceList/IDRServer/.
"""

import re
import time
import requests
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class IlMohScraper(BaseScraper):
    """Scraper for Israeli MOH drug shortage / cancellation data.

    The Israeli Drug Registry does not expose a single dedicated shortage list.
    Instead, we query the advanced-search endpoint (SearchByAdv) for recently
    cancelled drugs (iscanceled flag + date range) and enrich each record with
    detail from GetSpecificDrug when available.

    Drug records from the search contain Hebrew and English names, active
    components, dosage form, registration numbers and pricing.
    """

    API_BASE = "https://israeldrugs.health.gov.il/GovServiceList/IDRServer"
    SEARCH_URL = f"{API_BASE}/SearchByAdv"
    DETAIL_URL = f"{API_BASE}/GetSpecificDrug"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://israeldrugs.health.gov.il",
        "Referer": "https://israeldrugs.health.gov.il/",
    }

    # Maximum pages to iterate through when paginating search results
    MAX_PAGES = 50

    def __init__(self):
        super().__init__(
            country_code="IL",
            country_name="Israel",
            source_name="MOH",
            base_url="https://israeldrugs.health.gov.il",
        )

    def _parse_date(self, val) -> str | None:
        """Parse date values from the API (various formats)."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        val = str(val).strip()
        if not val or val in ("-", "nan", "None", "null"):
            return None

        # Remove time component if present
        val = re.sub(r'T.*$', '', val)
        val = re.sub(r'\s+\d{2}:\d{2}.*$', '', val)

        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%Y%m%d"):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _clean_text(self, val) -> str:
        """Clean a text value, handling None/nan and stripping whitespace."""
        if val is None:
            return ""
        val = str(val).strip()
        if val.lower() in ("nan", "none", "null"):
            return ""
        return val

    def _search_cancelled_drugs(self, page_index: int = 1) -> dict:
        """Query SearchByAdv for cancelled / shortage drugs.

        The SearchByAdv endpoint accepts POST with JSON body and returns
        a dict with 'results' (list of drug dicts) and pagination info.
        We request cancelled drugs by setting date range filters.
        """
        # Build the search payload requesting cancelled drugs
        # Using a broad date window to capture recent cancellations
        today = datetime.now()
        from_date = f"{today.year - 2}-01-01"
        to_date = today.strftime("%Y-%m-%d")

        payload = {
            "val": "",
            "veterinary": False,
            "cytotoxic": False,
            "prescription": False,
            "isGSL": False,
            "healthServices": False,
            "isPeopleMedication": True,
            "fromCanceledDrags": from_date,
            "toCanceledDrags": to_date,
            "fromUpdateInstructions": None,
            "toUpdateInstructions": None,
            "fromNewDrags": None,
            "toNewDrags": None,
            "newDragsDrop": None,
            "pageIndex": page_index,
            "orderBy": 0,
            "types": None,
        }

        resp = requests.post(
            self.SEARCH_URL,
            json=payload,
            headers=self.HEADERS,
            timeout=30,
        )
        resp.raise_for_status()

        # The API sometimes returns an HTML maintenance page instead of JSON
        content_type = resp.headers.get("Content-Type", "")
        if "text/html" in content_type or resp.text.strip().startswith("<"):
            raise RuntimeError(
                "Israeli MOH API returned HTML instead of JSON. "
                "The API may be in maintenance mode. "
                f"Response preview: {resp.text[:200]}"
            )

        try:
            data = resp.json()
        except ValueError as e:
            raise RuntimeError(
                f"Failed to parse JSON from Israeli MOH API: {e}. "
                f"Response preview: {resp.text[:200]}"
            ) from e

        # The API sometimes returns 200 with a .NET error string instead of
        # actual data (e.g. "Object reference not set to an instance of an object.")
        if isinstance(data, str):
            raise RuntimeError(
                f"Israeli MOH API returned an error string instead of data: "
                f"{data[:200]}. The backend service may be unavailable."
            )

        return data

    def _get_drug_detail(self, reg_num: str) -> dict | None:
        """Fetch detailed information for a specific drug by registration number."""
        if not reg_num:
            return None
        try:
            resp = requests.post(
                self.DETAIL_URL,
                json={"dragRegNum": reg_num},
                headers=self.HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                return None
            # Skip HTML error/maintenance pages
            if resp.text.strip().startswith("<"):
                return None
            return resp.json()
        except (ValueError, requests.exceptions.RequestException):
            return None

    def _extract_active_substance(self, detail: dict | None, search_record: dict) -> str:
        """Extract active substance from detail or search record.

        The detail response has 'activeMetirals' (note: MOH spelling).
        The search results have 'activeComponentsDisplayName' and
        'activeComponentsCompareName'.
        """
        # Try detail first
        if detail:
            active = self._clean_text(detail.get("activeMetirals"))
            if active:
                return active

        # Fall back to search record fields
        for field in ("activeComponentsDisplayName", "activeComponentsCompareName"):
            val = self._clean_text(search_record.get(field))
            if val:
                return val

        return ""

    def _extract_strength(self, detail: dict | None, search_record: dict) -> str:
        """Extract dosage strength from the drug name or detail if available.

        The Israeli registry often embeds strength in the product name
        (e.g. 'ACAMOL 500 MG'). We try to parse it from the English name.
        """
        # Try to extract from English drug name (common pattern: NAME STRENGTH UNIT)
        en_name = self._clean_text(
            search_record.get("dragEnName") or
            (detail.get("dragEnName") if detail else "")
        )
        if en_name:
            match = re.search(
                r'(\d+(?:\.\d+)?)\s*(mg|mcg|g|ml|iu|units?|%)\b',
                en_name,
                re.IGNORECASE,
            )
            if match:
                return f"{match.group(1)} {match.group(2).upper()}"

        # Try Hebrew name
        he_name = self._clean_text(
            search_record.get("dragHebName") or
            (detail.get("dragHebName") if detail else "")
        )
        if he_name:
            match = re.search(
                r'(\d+(?:\.\d+)?)\s*(mg|mcg|g|ml|iu|מ"ג|מ"ל|גרם)',
                he_name,
                re.IGNORECASE,
            )
            if match:
                return f"{match.group(1)} {match.group(2)}"

        return ""

    def scrape(self) -> pd.DataFrame:
        """Scrape the Israeli MOH drug registry for cancelled/shortage drugs.

        Paginates through SearchByAdv results for cancelled drugs,
        optionally enriches each record with GetSpecificDrug detail,
        and maps to standard columns.
        """
        print(f"Scraping {self.country_name} ({self.source_name})...")

        all_results = []
        page = 1

        while page <= self.MAX_PAGES:
            print(f"  Fetching page {page}...")
            try:
                data = self._search_cancelled_drugs(page_index=page)
            except requests.exceptions.HTTPError as e:
                if page == 1:
                    raise RuntimeError(
                        f"Failed to fetch shortage data from Israeli MOH API: {e}"
                    ) from e
                print(f"  HTTP error on page {page}, stopping pagination: {e}")
                break
            except requests.exceptions.RequestException as e:
                if page == 1:
                    raise RuntimeError(
                        f"Connection error to Israeli MOH API: {e}"
                    ) from e
                print(f"  Connection error on page {page}, stopping: {e}")
                break

            # The response can be a dict with 'results' key, or a list directly
            if isinstance(data, dict):
                results = data.get("results", [])
            elif isinstance(data, list):
                results = data
            else:
                print(f"  Unexpected response type: {type(data)}")
                break

            if not results:
                print(f"  No more results on page {page}")
                break

            all_results.extend(results)
            print(f"  Got {len(results)} results (total so far: {len(all_results)})")

            # If we got fewer results than a typical page, we've reached the end
            if len(results) < 20:
                break

            page += 1
            time.sleep(0.5)

        print(f"  Total raw results: {len(all_results)}")

        if not all_results:
            print("  WARNING: No results returned from the API. The API may be "
                  "in maintenance mode or the endpoint may have changed.")
            return pd.DataFrame(columns=[
                "country_code", "country_name", "source", "medicine_name",
                "active_substance", "strength", "package_size", "status",
                "shortage_start", "estimated_end", "scraped_at",
            ])

        # Optionally enrich with detail lookups (for active substance)
        detail_cache: dict[str, dict | None] = {}
        unique_reg_nums = {
            self._clean_text(r.get("dragRegNum"))
            for r in all_results
            if self._clean_text(r.get("dragRegNum"))
        }
        print(f"  Looking up details for {len(unique_reg_nums)} unique drugs...")

        for i, reg_num in enumerate(unique_reg_nums):
            detail_cache[reg_num] = self._get_drug_detail(reg_num)
            if (i + 1) % 50 == 0:
                print(f"    ... {i + 1}/{len(unique_reg_nums)} lookups done")
            time.sleep(0.2)

        found = sum(1 for v in detail_cache.values() if v)
        print(f"  Detail found for {found}/{len(unique_reg_nums)} drugs")

        # Build standardized records
        records = []
        now_iso = datetime.now().isoformat()

        for item in all_results:
            reg_num = self._clean_text(item.get("dragRegNum"))
            detail = detail_cache.get(reg_num)

            # Determine medicine name: prefer English, fall back to Hebrew
            en_name = self._clean_text(item.get("dragEnName"))
            he_name = self._clean_text(item.get("dragHebName"))
            medicine_name = en_name if en_name else he_name

            if not medicine_name:
                continue

            # Determine status
            is_cancelled = item.get("iscanceled")
            bitul_date = self._parse_date(
                item.get("bitulDate") or
                (detail.get("bitulDate") if detail else None)
            )

            if is_cancelled:
                status = "cancelled"
            else:
                status = "shortage"

            # Dosage form
            dosage_form = self._clean_text(
                item.get("dosageForm") or
                (detail.get("dosageForm") if detail else "")
            )
            dosage_form_eng = self._clean_text(
                (detail.get("dosageFormEng") if detail else "") or ""
            )

            records.append({
                "country_code": self.country_code,
                "country_name": self.country_name,
                "source": self.source_name,
                "medicine_name": medicine_name,
                "medicine_name_he": he_name,
                "medicine_name_en": en_name,
                "active_substance": self._extract_active_substance(detail, item),
                "strength": self._extract_strength(detail, item),
                "package_size": "",
                "dosage_form": dosage_form_eng if dosage_form_eng else dosage_form,
                "registration_number": reg_num,
                "registration_owner": self._clean_text(
                    item.get("dragRegOwner") or
                    (detail.get("regOwnerName") if detail else "")
                ),
                "status": status,
                "shortage_start": bitul_date,
                "estimated_end": None,
                "cancellation_date": bitul_date,
                "scraped_at": now_iso,
            })

        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} shortage/cancellation records scraped")
        return df
