"""Scraper voor Estland (Ravimiamet) via ravimiregister.ee.

Twee stappen, want de bron splitst de gegevens over twee endpoints:

  1. De WebForms-zoekpagina levert een HTML-tabel met naam, ATC, stof, vorm,
     sterkte en handelsvergunninghouder -- maar GEEN ENKELE DATUM. Wie alleen
     die tabel leest (zoals deze scraper tot september 2026 deed) houdt
     noodgedwongen lege startdatums over.
  2. De datums zitten een niveau dieper, in de ASP.NET page-method
     PublicHomePage.aspx/GetPakend, die per pakket-GUID (vid) 38 velden JSON
     teruggeeft. Die vid staat al in de detaillink van de zoektabel, dus er is
     geen extra navigatie nodig -- alleen een tweede request per rij.

Een rij is een VERPAKKING, niet een geneesmiddel. Hetzelfde middel kan dus
meerdere keren voorkomen met verschillende vids; dat is geen duplicatie.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from scrapers.base_scraper import BaseScraper


class EeRavimiametScraper(BaseScraper):
    """Scraper voor Ravimiamet (Estse geneesmiddelenautoriteit).

    Zoekt op het filter 'Tarneraskusega' (leveringsproblemen) en verrijkt elke
    gevonden verpakking met de tarneraskus-datums uit de GetPakend page-method.
    """

    SEARCH_URL = "https://www.ravimiregister.ee/Default.aspx?pv=HumRavimid.Otsing"
    PAKEND_URL = "https://www.ravimiregister.ee/PublicHomePage.aspx/GetPakend"

    # GUID van de waarde 'Humaanravimid' in het ravimiLiik-keuzevak. Deze staat
    # hard in de HTML van het formulier; hij verandert alleen als de bron zijn
    # stamdata omgooit. Zie _build_search_payload voor waarom hij nodig is.
    HUMAAN_GUID = "d8917a8b-1216-41bc-8249-e3c2dc561ee4"

    # Pauze tussen de GetPakend-requests. Cloudflare zit voor deze site en
    # beantwoordt bursts met HTTP 403 + een "Just a moment..."-pagina; dat is
    # geen blokkade van de bron zelf maar een snelheidslimiet. 0,3 s is tijdens
    # het bouwen genoeg gebleken om er ruim onder te blijven.
    DETAIL_PAUSE = 0.3

    def __init__(self):
        super().__init__(
            country_code="EE",
            country_name="Estonia",
            source_name="RAVIMIAMET",
            base_url="https://www.ravimiregister.ee",
        )
        self.session = requests.Session()
        # VALKUIL: zet hier GEEN eigen Accept-Encoding met 'br'. requests kan
        # brotli alleen uitpakken als het brotli-pakket geinstalleerd is; zonder
        # dat pakket krijg je HTTP 200 met binaire rommel als 'html'. Dat ziet
        # er precies uit als "de site is verbouwd" (0 hidden fields, halve
        # paginagrootte) terwijl er niets aan de hand is. Laat requests zelf
        # onderhandelen over gzip/deflate.
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
        })

    # ------------------------------------------------------------------ #
    # Hulpjes                                                             #
    # ------------------------------------------------------------------ #

    def _post_headers(self) -> dict:
        """Headers die elke POST nodig heeft.

        VALKUIL: zonder Referer (en Origin) antwoordt Cloudflare op de
        zoek-POST met HTTP 403 en een challenge-pagina, terwijl dezelfde GET
        gewoon 200 geeft. Dat leidt snel tot de verkeerde conclusie dat de bron
        dicht zit of inloggegevens vraagt. Hij zit niet dicht; hij wil alleen
        zien dat de POST van zijn eigen pagina komt.
        """
        return {
            "Referer": self.SEARCH_URL,
            "Origin": self.base_url,
        }

    def _post_form(self, data: dict, label: str, pogingen: int = 3):
        """POST naar de zoekpagina, met herstel na een Cloudflare-challenge.

        VALKUIL: Cloudflare gooit er op deze POST WILLEKEURIG een challenge
        tussendoor -- HTTP 403 met de pagina "Just a moment..." en de header
        cf-mitigated: challenge. Het is geen snelheidslimiet en geen blokkade:
        exact dezelfde aanroep lukt een paar seconden later wel. Wie na een
        enkele 403 concludeert dat de bron dicht zit, gooit een werkende
        scraper weg. Gewoon even wachten en opnieuw proberen.

        Opvallend genoeg treft dit ALLEEN de formulier-POSTs naar Default.aspx;
        de GetPakend page-method liep in de test 100 van de 100 keer goed door.
        Ga dus niet de datumophaling vertragen om dit op te lossen.
        """
        for poging in range(1, pogingen + 1):
            try:
                resp = self.session.post(
                    self.SEARCH_URL, data=data, headers=self._post_headers(), timeout=60
                )
            except requests.RequestException as exc:
                # Ook een time-out of verbroken verbinding is het proberen waard. Voorheen
                # vloog die er meteen uit, en omdat de pagineringslus een mislukte pagina als
                # definitief beschouwt, viel de hele Estse set af: "50 van 113 opgehaald,
                # niet weggeschreven". Dat is correct gedrag van die lus, maar het maakte een
                # hikje van een seconde tot een gemiste verversing.
                if poging == pogingen:
                    print(f"  {label}: netwerkfout na {pogingen} pogingen ({type(exc).__name__})")
                    return None
                wacht = 4 * poging
                print(f"  {label}: netwerkfout ({type(exc).__name__}), {wacht}s wachten "
                      f"en opnieuw (poging {poging + 1}/{pogingen})")
                time.sleep(wacht)
                continue
            if resp.status_code == 200:
                return resp
            if resp.status_code != 403:
                resp.raise_for_status()
            if poging < pogingen:
                wacht = 4 * poging
                print(f"  {label}: Cloudflare-challenge (403), {wacht}s wachten "
                      f"en opnieuw (poging {poging + 1}/{pogingen})")
                time.sleep(wacht)
        return None

    def _get_hidden_fields(self, soup) -> dict:
        """Haal de ASP.NET hidden fields (__VIEWSTATE etc.) uit een pagina."""
        data = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name", "")
            if name:
                data[name] = inp.get("value", "")
        return data

    def _build_search_payload(self, soup) -> dict:
        """Bouw de zoek-POST na zoals de browser hem verstuurt.

        Waarom hier drie velden bij moeten, en niet een:

        * tarneraskus = 'Tarneraskusega'
          Het filter kent vier waarden: 'Tarneraskusega' (leveringsproblemen),
          'Turustamine lopetatud' (verkoop beeindigd), 'Molemad' (beide) en
          'Puudub' (geen). Stond ooit op 'Molemad', waardoor beeindigde middelen
          als tekort binnenkwamen en Estland een registerlijst leek. Kies dus
          bewust, en nooit 'Molemad'.

        * ravimiLiik$0 en ainultMyygiloaga
          Dit zijn CHECKBOXES die in de browser AANGEVINKT STAAN als je de
          pagina opent. Een browser stuurt een aangevinkte checkbox altijd mee;
          een POST die je zelf samenstelt uit alleen de hidden fields laat ze
          weg, en de server leest dat als UITGEVINKT. Gevolg: het humaan-filter
          en het geldige-handelsvergunning-filter vallen weg en de resultaatset
          zwelt van 111 naar 155, met veterinaire middelen en tekorten van
          middelen waarvan de vergunning allang verlopen is erbij. Dit is
          dezelfde registervervuiling waarom EE in augustus 2026 van de kaart
          is gehaald -- het filter hierboven was maar de helft van het verhaal.
          Zoek bij een afwijkend aantal dus eerst naar checkboxen met 'checked'
          in het formulier voordat je iets anders gaat zoeken.
        """
        data = self._get_hidden_fields(soup)
        data["ctl04$ctl00$ctl00$Detailotsing$tarneraskus"] = "Tarneraskusega"
        data["ctl04$ctl00$ctl00$Detailotsing$ravimiLiik$0"] = self.HUMAAN_GUID
        data["ctl04$ctl00$ctl00$Detailotsing$ainultMyygiloaga"] = "on"
        data["ctl04$ctl00$ctl00$Detailotsing$search2"] = "Otsi »"
        return data

    def _source_total(self, soup) -> int | None:
        """Lees het totaal dat de bron zelf noemt ('Otsingu tulemusi kokku N').

        Dit is de enige onafhankelijke controle die we hebben op de vraag of de
        paginering alles heeft opgehaald. Zonder zo'n controle kan een scraper
        maandenlang stilletjes een deel van de data leveren.
        """
        text = " ".join(soup.get_text(" ", strip=True).split())
        match = re.search(r"Otsingu tulemusi kokku\s*([0-9]+)", text)
        return int(match.group(1)) if match else None

    def _parse_results_table(self, soup) -> list[dict]:
        """Lees de resultatentabel van een zoekresultatenpagina.

        De koprij is over drie <tr>'s verdeeld (twee daarvan zonder <td>), dus
        rijen met te weinig cellen overslaan is hier geen noodoplossing maar
        noodzakelijk.
        """
        records = []

        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if "Ravimi nimetus" not in headers:
                continue

            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 7:
                    continue

                name = cells[1].get_text(strip=True)
                if not name:
                    continue

                link = cells[1].find("a")
                detail_url, vid = "", ""
                if link and link.get("href"):
                    href = link["href"]
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        detail_url = f"{self.base_url}{href}"
                    elif not href.startswith("javascript"):
                        detail_url = f"{self.base_url}/{href}"
                    # De pakket-GUID uit de detaillink is precies de vid die
                    # GetPakend verwacht; er is geen aparte lookup nodig.
                    match = re.search(r"vid=([0-9a-fA-F-]{36})", href)
                    if match:
                        vid = match.group(1)

                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": name,
                    # ATC staat ALLEEN in deze tabel, niet in de GetPakend-JSON.
                    # Deze kolom is dus de enige bron ervoor -- niet weglaten.
                    "atc_code": cells[2].get_text(strip=True),
                    "active_substance": cells[3].get_text(strip=True),
                    "dosage_form": cells[4].get_text(strip=True),
                    "strength": cells[5].get_text(strip=True),
                    "marketing_auth_holder": cells[6].get_text(strip=True),
                    "vid": vid,
                    "detail_url": detail_url,
                })

            break

        return records

    def _find_next_page_target(self, soup) -> str | None:
        """Zoek het postback-doel achter de knop 'Jargmine' (volgende)."""
        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True) == "Järgmine" and "paging1" in a["href"]:
                match = re.search(r'PostBackOptions\("([^"]+)"', a["href"])
                if match:
                    return match.group(1)
        return None

    # ------------------------------------------------------------------ #
    # Stap 2: datums per verpakking                                       #
    # ------------------------------------------------------------------ #

    def _fetch_pakend(self, vid: str, retry_budget: list[int]) -> dict | None:
        """Haal de 38 JSON-velden van een verpakking op.

        VALKUIL: BEIDE parameters zijn verplicht. Een ASP.NET page-method
        declareert zijn parameters en geeft HTTP 500 met een HTML-foutpagina
        zodra er een mist. Het weglaten van `isEnglish` levert dus precies
        hetzelfde beeld op als een verkeerde parameternaam, waardoor je gaat
        gokken naar de naam van `vid` terwijl die al goed was. Ziet u hier een
        500 met text/html terug: tel eerst de parameters.

        `retry_budget` is een gedeelde teller, zodat een reeks Cloudflare-403's
        de hele run niet over de tijdslimiet van de batchrunner duwt.
        """
        payload = {"vid": vid, "isEnglish": True}
        headers = dict(self._post_headers())
        headers["Content-Type"] = "application/json; charset=utf-8"

        for poging in range(2):
            try:
                resp = self.session.post(
                    self.PAKEND_URL, json=payload, headers=headers, timeout=30
                )
            except requests.RequestException as exc:
                print(f"    vid {vid[:8]}: netwerkfout ({exc})")
                return None

            if resp.status_code == 200 and "json" in (resp.headers.get("Content-Type") or ""):
                try:
                    return resp.json().get("d")
                except ValueError:
                    print(f"    vid {vid[:8]}: antwoord was geen geldige JSON")
                    return None

            # 403 = Cloudflare-snelheidslimiet, niet "endpoint bestaat niet".
            # Even wachten helpt; doorrammen maakt het alleen erger.
            if resp.status_code == 403 and poging == 0 and retry_budget[0] > 0:
                retry_budget[0] -= 1
                time.sleep(3)
                continue

            print(f"    vid {vid[:8]}: HTTP {resp.status_code} ({resp.headers.get('Content-Type')})")
            return None

        return None

    @staticmethod
    def _parse_date(raw: str) -> str:
        """'10.05.2023 00:00:00' -> '2023-05-10'. Onleesbaar of leeg -> ''.

        HARDE REGEL: hier komt NOOIT een fallback naar vandaag. Een lege
        startdatum is informatie ('bron geeft er geen'); de scrapedatum invullen
        maakt er een onware bewering van. AT en DK zijn daar eerder op
        stukgelopen: die toonden de dag van scrapen als begin van het tekort.
        """
        if not raw:
            return ""
        try:
            return datetime.strptime(raw.split(" ")[0], "%d.%m.%Y").date().isoformat()
        except ValueError:
            return ""

    # ------------------------------------------------------------------ #
    # Hoofdroutine                                                        #
    # ------------------------------------------------------------------ #

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        scraped_at = datetime.now().isoformat()
        vandaag = datetime.now().date().isoformat()

        # --- Stap 1a: zoekformulier ophalen en versturen -----------------
        # Bij een challenge halen we het formulier OPNIEUW op in plaats van
        # dezelfde POST te herhalen: de challenge zet een cookie en de
        # __VIEWSTATE hoort bij de pagina waar hij vandaan komt, dus een verse
        # sessie is betrouwbaarder dan doorduwen met oude velden.
        resp2 = None
        for ronde in range(1, 4):
            self.session.cookies.clear()
            resp = self.session.get(self.SEARCH_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            if not self._get_hidden_fields(soup):
                print(f"  Zoekpagina zonder hidden fields (ronde {ronde}); opnieuw")
                time.sleep(5)
                continue
            time.sleep(1)
            resp2 = self._post_form(self._build_search_payload(soup), "zoek-POST")
            if resp2 is not None:
                break
            time.sleep(5)

        if resp2 is None:
            raise RuntimeError(
                "zoek-POST bleef 403 (Cloudflare-challenge) na 3 rondes -- geen data "
                "opgehaald. Liever niets dan een halve lijst wegschrijven."
            )
        soup2 = BeautifulSoup(resp2.text, "lxml")

        bron_totaal = self._source_total(soup2)
        print(f"  Bron meldt zelf: {bron_totaal} resultaten")

        rows = self._parse_results_table(soup2)
        gezien_vids = {r["vid"] for r in rows if r["vid"]}
        print(f"  Pagina 1: {len(rows)} rijen")

        # --- Stap 1b: paginering ----------------------------------------
        # De Jargmine-postback werkt en levert per pagina volledig nieuwe
        # records. Toch een assertie, want dit is de plek waar Denemarken
        # maandenlang misging: daar negeerde de server de paginaparameter en
        # werd 19x dezelfde pagina opgehaald zonder dat iets dat merkte.
        # Nul nieuwe vids op een pagina = stoppen en waarschuwen, niet doorgaan.
        page_num = 2
        max_pages = 20  # ruim genoeg voor 111 rijen van 50; niet nodeloos hoger
        paginering_afgebroken = False
        while page_num <= max_pages:
            next_target = self._find_next_page_target(soup2)
            if not next_target:
                break

            time.sleep(1.5)
            page_data = self._get_hidden_fields(soup2)
            page_data["__EVENTTARGET"] = next_target
            page_data["__EVENTARGUMENT"] = ""

            try:
                resp_page = self._post_form(page_data, f"pagina {page_num}")
            except Exception as exc:
                print(f"  WAARSCHUWING: pagina {page_num} mislukte ({exc})")
                resp_page = None
            if resp_page is None:
                print(f"  WAARSCHUWING: pagina {page_num} niet opgehaald; "
                      f"de set is onvolledig")
                paginering_afgebroken = True
                break
            soup2 = BeautifulSoup(resp_page.text, "lxml")

            page_rows = self._parse_results_table(soup2)
            if not page_rows:
                break

            nieuwe = {r["vid"] for r in page_rows if r["vid"]} - gezien_vids
            if not nieuwe:
                print(f"  WAARSCHUWING: pagina {page_num} bevat 0 nieuwe vids -- de bron "
                      f"geeft waarschijnlijk steeds dezelfde pagina terug. Gestopt.")
                break

            gezien_vids |= nieuwe
            rows.extend(page_rows)
            print(f"  Pagina {page_num}: {len(page_rows)} rijen, {len(nieuwe)} nieuw "
                  f"(totaal {len(rows)})")
            page_num += 1

        # Onafhankelijke controle tegen het getal van de bron zelf. Dit is de
        # enige manier om "we hebben alles" te onderscheiden van "we hebben
        # toevallig niet gemerkt dat er iets ontbrak".
        if bron_totaal is None:
            print("  WAARSCHUWING: kon 'Otsingu tulemusi kokku' niet lezen; "
                  "totaalcontrole overgeslagen")
        elif len(gezien_vids) != bron_totaal:
            print(f"  WAARSCHUWING: {len(gezien_vids)} unieke vids opgehaald, maar de bron "
                  f"meldt {bron_totaal}. Uitzoeken voordat dit als compleet geldt.")
        else:
            print(f"  OK: {len(gezien_vids)} unieke vids == bron-totaal {bron_totaal}")

        if len(rows) != len(gezien_vids):
            print(f"  WAARSCHUWING: {len(rows)} rijen maar {len(gezien_vids)} unieke vids "
                  f"-- dubbele verpakkingen opgehaald?")

        # Bewust HARD stoppen bij een echt gat: een onvolledige CSV overschrijft
        # de goede van gisteren en niemand ziet het. Precies zo leverde de
        # Deense scraper maandenlang 5% van de data zonder dat het opviel.
        # Liever een zichtbare fout in de batchlog dan stille datavernietiging.
        onvolledig = paginering_afgebroken or (
            bron_totaal is not None and len(gezien_vids) < bron_totaal * 0.95
        )
        if onvolledig:
            raise RuntimeError(
                f"onvolledige set: {len(gezien_vids)} van {bron_totaal} meldingen opgehaald"
                f"{' (paginering afgebroken)' if paginering_afgebroken else ''} -- "
                f"niet weggeschreven"
            )

        # --- Stap 2: datums per verpakking ------------------------------
        retry_budget = [10]
        zonder_start = veterinair = gelijk_aan_vandaag = mislukt = 0

        for i, rec in enumerate(rows, 1):
            # Deze velden bestaan altijd, ook als de detailcall faalt; liever
            # leeg dan verzonnen.
            rec["shortage_start"] = ""
            rec["estimated_end"] = ""
            rec["status"] = "shortage"
            rec["package_size"] = ""
            rec["product_no"] = ""
            rec["reason"] = ""
            rec["medicine_type"] = ""
            rec["scraped_at"] = scraped_at

            if not rec["vid"]:
                print(f"    rij {i} ({rec['medicine_name']}): geen vid in de detaillink")
                mislukt += 1
                continue

            if i > 1:
                time.sleep(self.DETAIL_PAUSE)
            d = self._fetch_pakend(rec["vid"], retry_budget)
            if not isinstance(d, dict):
                # Record NIET overslaan: naam, ATC en stof uit de tabel zijn
                # gewoon geldig. Alleen de datums ontbreken, en dat is zichtbaar.
                mislukt += 1
                continue

            rec["shortage_start"] = self._parse_date(d.get("TarneraskusAlgus", ""))
            if not rec["shortage_start"]:
                zonder_start += 1
            elif rec["shortage_start"] == vandaag:
                gelijk_aan_vandaag += 1

            # TarneraskusLopp is een HARDE einddatum die de bron zelf vaststelt.
            # Bij alle lopende meldingen is hij leeg; staat hij gevuld, dan is
            # het tekort echt opgelost en mag hij als einddatum mee.
            eind = self._parse_date(d.get("TarneraskusLopp", ""))
            if eind:
                rec["status"] = "resolved"
                rec["estimated_end"] = eind

            # TarneraskusOletatav blijft BEWUST uit estimated_end. Van de 153
            # gemeten waarden was er geen enkele een echte datum: ongeveer de
            # helft is "ei ole teada" / "tapsustamisel" (onbekend), de rest is
            # vrije tekst op maandniveau in verbogen Estse maandnamen, zoals
            # "eeldatavasti oktoobris 2026" of "eeldatavasti detsembri lopus
            # 2026". Daar een dag in verzinnen is precies de AT/DK-fout. De
            # ruwe tekst gaat naar reason, zodat niets verloren gaat en het
            # dashboard zelf kan besluiten er iets mee te doen -- dan wel
            # zichtbaar, in een eigen kolom, niet stilzwijgend als datum.
            # (TarneraskusTekst is voor elk record de generieke zin "Ravimil on
            # tarneraskus" en voegt niets toe, dus die laten we liggen.)
            rec["reason"] = (d.get("TarneraskusOletatav") or "").strip()

            kogus = (d.get("KogusPakendis") or "").strip()
            lisand = (d.get("PakendiLisand") or "").strip()
            rec["package_size"] = ", ".join(p for p in (kogus, lisand) if p)
            rec["product_no"] = (d.get("Kood") or "").strip()

            # Engelse varianten geven een INN en een vormnaam die over landen
            # heen te matchen zijn; de zoektabel levert alleen Estisch.
            rec["active_substance"] = (d.get("ToimeaineEng") or "").strip() or rec["active_substance"]
            rec["dosage_form"] = (d.get("RavimvormEng") or "").strip() or rec["dosage_form"]

            rec["medicine_type"] = (d.get("RavimiLiikEng") or "").strip()
            if rec["medicine_type"] == "Veterinary medicine":
                veterinair += 1

            # VALKUIL: ViimatiSisseToodud staat er verleidelijk bij, maar is de
            # laatste IMPORTdatum van de verpakking, niet een mutatie op de
            # tekortmelding. Die op last_updated mappen suggereert dat een
            # melding pas is bijgewerkt terwijl er alleen voorraad binnenkwam.
            # Daarom levert deze bron geen last_updated.

        print(f"  Datums opgehaald: {len(rows) - mislukt}/{len(rows)} gelukt, "
              f"{mislukt} mislukt")
        if zonder_start:
            print(f"  LET OP: {zonder_start} records zonder startdatum in de bron "
                  f"(veld blijft leeg, niet aangevuld)")
        if veterinair:
            print(f"  WAARSCHUWING: {veterinair} veterinaire middelen in de set -- "
                  f"het humaan-filter in de zoek-POST werkt niet meer")
        if gelijk_aan_vandaag:
            print(f"  LET OP: {gelijk_aan_vandaag} startdatums vallen op vandaag; "
                  f"controleer of dat echte nieuwe meldingen zijn")

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop(columns=["vid"])
        print(f"  Totaal: {len(df)} tekortmeldingen (verpakkingen)")
        return df
