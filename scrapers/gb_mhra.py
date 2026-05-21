"""Scraper for United Kingdom — MHRA Central Alerting System (CAS) supply alerts (skeleton).

In de UK is **SPS Medicines Supply Tool** alleen toegankelijk voor NHS-emailadressen
(`nhs.net`) — niet publiek bruikbaar voor onze pipeline. De **publieke** route is:

| Bron | URL | Type |
|---|---|---|
| MHRA Central Alerting System (CAS) | https://www.cas.mhra.gov.uk/SearchAlerts.aspx | webform search van alerts |
| GOV.UK drug device alerts | https://www.gov.uk/drug-device-alerts | RSS + HTML listing |
| GOV.UK MI on medicines supply issue notifications | https://www.gov.uk/government/publications/management-information-on-medicines-supply-issue-notifications/management-information-on-medicines-supply-issue-notifications | management info |

Een MSN (Medicine Supply Notification) of NatPSA (National Patient Safety Alert) wordt via
CAS gepubliceerd. CAS is een ASP.NET formulier — voor scraping moet de viewstate worden
afgevangen en de search-form POST gerepliceerd.

Stand van zaken: skeleton. Twee benaderingen mogelijk:

  A) **CAS SearchAlerts** — alle alerts met filter `Alert Type = Medicine Supply Notification`
     of `Issued by = DHSC`. ASP.NET viewstate-handling vereist.
  B) **GOV.UK drug-device-alerts** — eenvoudiger HTML, RSS-feed beschikbaar maar minder
     gestructureerd en lager volume.

Aanbevolen werkstappen:
  1. Beslissen welke route (A levert volledigere data; B is sneller).
  2. Voor route A: in de browser CAS bezoeken, search uitvoeren, viewstate-string kopiëren.
  3. Per alert: detailpagina (`ViewAlert.aspx?AlertID=...`) volgen om medicijnnaam, werkzame
     stof en actie/oorzaak te extraheren.
  4. Cross-check met GOV.UK MI-listing.
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper


class GbMhraScraper(BaseScraper):
    """United Kingdom MHRA CAS medicine supply notifications — skeleton."""

    CAS_SEARCH_URL = "https://www.cas.mhra.gov.uk/SearchAlerts.aspx"
    CAS_ALERT_URL = "https://www.cas.mhra.gov.uk/ViewandAcknowledgment/ViewAlert.aspx"
    GOVUK_ALERTS_URL = "https://www.gov.uk/drug-device-alerts.atom"  # RSS feed

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "text/html,application/xml",
    }

    def __init__(self) -> None:
        super().__init__(
            country_code="GB",
            country_name="United Kingdom",
            source_name="MHRA",
            base_url="https://www.cas.mhra.gov.uk",
        )

    # ─── Optie B: GOV.UK Atom-feed (eenvoudig, lager volume) ────────────────

    def _fetch_govuk_feed(self) -> list[dict]:
        """Haal de drug-device-alerts Atom-feed op en filter op supply-notifications."""
        resp = requests.get(self.GOVUK_ALERTS_URL, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml-xml")

        entries: list[dict] = []
        for entry in soup.find_all("entry"):
            title = entry.find("title").get_text(strip=True) if entry.find("title") else ""
            link_el = entry.find("link")
            link = link_el.get("href", "") if link_el else ""
            updated = entry.find("updated").get_text(strip=True) if entry.find("updated") else ""
            summary = entry.find("summary").get_text(strip=True) if entry.find("summary") else ""

            # Filter op supply-gerelateerde alerts
            blob = f"{title} {summary}".lower()
            if any(k in blob for k in ("supply", "shortage", "msn", "natpsa", "discontin")):
                entries.append({"title": title, "url": link, "updated": updated, "summary": summary})
        return entries

    def _parse_govuk_entry(self, entry: dict) -> dict:
        """Map een Atom-entry naar een standaard shortage-record."""
        title = entry["title"]
        # Heuristiek: "<medicijn>: supply issue / shortage" of "MSN/yyyy/xx: <medicijn> ..."
        med_match = re.search(r"(?:MSN|NatPSA)/\d+/\d+[^:]*:\s*(.+?)(?:\s*-|$)", title)
        if med_match:
            medicine_name = med_match.group(1).strip()
        else:
            medicine_name = title.split(":")[0].strip()

        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "source": self.source_name,
            "medicine_name": medicine_name,
            "active_substance": "",
            "strength": "",
            "package_size": "",
            "atc_code": "",
            "shortage_start": entry.get("updated", "")[:10] or None,
            "estimated_end": None,
            "status": "shortage" if any(k in title.lower() for k in ("supply", "shortage", "discontin")) else "alert",
            "alert_title": title,
            "alert_url": entry.get("url", ""),
            "summary": entry.get("summary", "")[:500],
            "scraped_at": datetime.now().isoformat(),
        }

    # ─── Optie A: CAS search (volledig, maar viewstate-handling nodig) ──────

    def _scrape_cas(self) -> list[dict]:
        """Doe een POST naar CAS SearchAlerts met filter op supply notifications.

        TODO: viewstate / EVENTVALIDATION string afkijken in de browser-network-tab en hier
        invullen. Zonder die strings geeft CAS een 500 of een lege response. Voor nu een
        placeholder die wordt overgeslagen.
        """
        raise NotImplementedError(
            "CAS SearchAlerts.aspx vraagt om __VIEWSTATE/__EVENTVALIDATION tokens. "
            "Implementeer door een browser-flow op te nemen, of vervang door optie B (GOV.UK feed)."
        )

    # ─── Main ───────────────────────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")

        try:
            entries = self._fetch_govuk_feed()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch GOV.UK alerts feed: {e}") from e

        records = [self._parse_govuk_entry(e) for e in entries]
        df = pd.DataFrame(records)
        print(f"  Total: {len(df)} alerts (filtered op supply/shortage keywords)")
        return df
