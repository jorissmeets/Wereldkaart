"""Scraper voor Taiwan — TFDA, open data van het 西藥供應資訊平台 (geneesmiddelvoorziening).

WAT DIT IS
----------
Drie wekelijks bijgewerkte open-datasets van de Taiwanese FDA (data.gov.tw 54504-54506):
  104  經評估有替代藥品品項  lopende tekorten waarvoor een alternatief is aangewezen
  105  經評估無替代藥品品項  lopende tekorten zonder alternatief (vaak leeg: "沒有資料")
  106  藥品已解除短缺品項      opgeheven tekorten
Velden: nummer, 公告更新時間 (laatste bijwerking), productnaam (Chinees + Engels),
vergunningsnummer en 供應狀態 -- een vrije tekst met reden, verwachte hervatting en het
aangewezen alternatief. Er staat GEEN werkzame stof, ATC of startdatum in.

De stof komt uit de vergunningendataset van dezelfde FDA (export 36, ~66.500 vergunningen,
zip van ~10 MB): vergunningsnummer -> 主成分略述 (Engelse stofnaam), 英文品名, 申請商名稱.
Daarna leidt de gewone ATC-verrijking de code af uit die Engelse stofnaam. ";;" scheidt
meerdere stoffen; dat wordt "+", zodat de verrijking een combinatie herkent en die bewust
ongecodeerd laat.

WAAROM NIET DSMS
----------------
De oude scraper las dsms.fda.gov.tw; die weigert elke verbinding van buiten Taiwan en wat
overbleef waren persberichtkoppen uit 2015/2016. Daarom stond TW tot 14-11-2026 niet op de
kaart. Deze open-datasets zijn wel bereikbaar en vallen onder de Taiwanese open-
overheidslicentie (政府資料開放授權條款-第1版).

DATUMS EN STATUS
----------------
- shortage_start blijft LEEG: de bron kent geen startdatum, en 公告更新時間 is een
  bijwerkdatum. Die als start invullen is precies de AT/DK-fout (scrapedatum als start).
- last_updated = 公告更新時間. build_data behandelt TW als momentopname (zoals CZ en JP):
  wat in 104/105 staat is NU een tekort, hoe oud de laatste bijwerking ook is.
- estimated_end uit de tekst: "預計115年4月恢復供應" -> 2027-04-30 (Taiwanese jaartelling +
  1911, laatste dag van de maand; kwartaal en halfjaar naar hun laatste dag).
- 106: status resolved, actual_end = 公告更新時間.

Het alternatief in 供應狀態 gaat mee in `notes`, maar wordt nergens gebruikt.
"""

import io
import json
import re
import zipfile
from calendar import monthrange
from datetime import datetime

import pandas as pd
import requests

from scrapers.base_scraper import BaseScraper


class TwTfdaScraper(BaseScraper):
    EXPORT = "https://data.fda.gov.tw/data/opendata/export/{n}/{fmt}"
    LOPEND = (104, 105)
    OPGELOST = 106
    VERGUNNINGEN = 36

    # Reden uit de vrije tekst, in vaste categorieën die cause_mapping_v2.csv kent.
    # Volgorde telt: "廠商停止供應" moet vóór het algemene "供應".
    REDENEN = [
        ("廠商停止供應", "廠商停止供應"),
        ("GMP", "GMP問題"),
        ("藥證", "藥證問題"),
        ("原物料", "原物料問題"), ("原料", "原物料問題"),
        ("生產", "生產問題"), ("製程", "生產問題"),
        ("成本", "成本因素"),
        ("需求", "需求增加"),
        ("供應", "供應問題"), ("輸入", "供應問題"),
    ]

    def __init__(self):
        super().__init__(
            country_code="TW",
            country_name="Taiwan",
            source_name="TFDA",
            base_url="https://data.fda.gov.tw",
        )
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0"

    def _json(self, n: int) -> list[dict]:
        r = self.session.get(self.EXPORT.format(n=n, fmt="json"), timeout=60)
        r.raise_for_status()
        rijen = json.loads(r.content.decode("utf-8-sig"))
        # Een lege dataset komt terug als één rij met 編號 "沒有資料".
        return [x for x in rijen if x.get("許可證字號")]

    def _vergunningen(self) -> dict[str, dict]:
        r = self.session.get(self.EXPORT.format(n=self.VERGUNNINGEN, fmt="csv"), timeout=120)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            naam = next(n for n in z.namelist() if n.endswith(".csv"))
            df = pd.read_csv(z.open(naam), dtype=str, encoding="utf-8-sig", keep_default_na=False,
                             usecols=["許可證字號", "英文品名", "劑型", "主成分略述", "申請商名稱"])
        return {x["許可證字號"]: x for x in df.to_dict("records")}

    @staticmethod
    def _datum(s: str) -> str | None:
        m = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", (s or "").strip())
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else None

    @staticmethod
    def _verwacht_einde(tekst: str) -> str | None:
        m = re.search(r"預計(\d{3})年(第([一二三四])季|([上下])半年|(\d{1,2})月|底)", tekst or "")
        if not m:
            return None
        jaar = int(m.group(1)) + 1911
        if m.group(3):
            maand = {"一": 3, "二": 6, "三": 9, "四": 12}[m.group(3)]
        elif m.group(4):
            maand = 6 if m.group(4) == "上" else 12
        elif m.group(5):
            maand = int(m.group(5))
        else:
            maand = 12
        if not 1 <= maand <= 12:
            return None
        return f"{jaar}-{maand:02d}-{monthrange(jaar, maand)[1]:02d}"

    def _reden(self, tekst: str) -> str:
        m = re.search(r"因(?:為)?([^，,。；]{2,24})", tekst or "")
        fragment = m.group(1) if m else ""
        return next((cat for sleutel, cat in self.REDENEN if sleutel in fragment), "")

    def scrape(self) -> pd.DataFrame:
        print(f"Scraping {self.country_name} ({self.source_name})...")
        vergunningen = self._vergunningen()
        print(f"  Vergunningen: {len(vergunningen)}")

        bronnen = [(n, "shortage") for n in self.LOPEND] + [(self.OPGELOST, "resolved")]
        records, zonder_vergunning = [], 0
        nu = datetime.now().isoformat()
        for n, status in bronnen:
            rijen = self._json(n)
            print(f"  Dataset {n}: {len(rijen)} rijen ({status})")
            for x in rijen:
                lic = x["許可證字號"].strip()
                v = vergunningen.get(lic)
                if v is None:
                    zonder_vergunning += 1
                    v = {}
                naam_zh, _, naam_en = (x.get("中文品名") or "").partition("\n")
                tekst = (x.get("供應狀態") or "").strip()
                bijgewerkt = self._datum(x.get("公告更新時間"))
                records.append({
                    "country_code": self.country_code,
                    "country_name": self.country_name,
                    "source": self.source_name,
                    "medicine_name": naam_en.strip() or v.get("英文品名", "") or naam_zh.strip(),
                    "medicine_name_zh": naam_zh.strip(),
                    "active_substance": (v.get("主成分略述") or "").replace(";;", " + ").strip(" +"),
                    "strength": "",
                    "package_size": "",
                    "product_no": lic,
                    "atc_code": "",
                    "marketing_auth_holder": v.get("申請商名稱", ""),
                    "shortage_start": None,
                    "estimated_end": self._verwacht_einde(tekst) if status == "shortage" else None,
                    "actual_end": bijgewerkt if status == "resolved" else None,
                    "status": status,
                    "reason": self._reden(tekst),
                    "last_updated": bijgewerkt,
                    "notes": tekst,
                    "dataset": n,
                    "scraped_at": nu,
                })
        df = pd.DataFrame(records)
        if zonder_vergunning:
            print(f"  LET OP: {zonder_vergunning} rijen met een vergunningsnummer dat niet in dataset "
                  f"{self.VERGUNNINGEN} staat; die hebben geen stof en dus geen ATC")
        print(f"  Total: {len(df)} records ({df['status'].value_counts().to_dict()})")
        return df
