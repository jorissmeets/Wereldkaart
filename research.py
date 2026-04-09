"""Research script to discover shortage data endpoints for all EU medicine agencies."""

import requests
from bs4 import BeautifulSoup
import json
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json",
}

def test_url(url, method="GET", json_body=None, label=""):
    """Test a URL and return status + content preview."""
    try:
        if method == "POST":
            r = requests.post(url, json=json_body, headers={**HEADERS, "Content-Type": "application/json"}, timeout=15, allow_redirects=True)
        else:
            r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        content_type = r.headers.get("Content-Type", "")
        preview = r.text[:500] if r.status_code == 200 else ""
        is_json = "json" in content_type or (preview.startswith("{") or preview.startswith("["))
        is_excel = "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type
        return {
            "url": url,
            "label": label,
            "status": r.status_code,
            "content_type": content_type[:80],
            "is_json": is_json,
            "is_excel": is_excel,
            "size": len(r.content),
            "preview": preview[:300] if not is_excel else f"[Excel file, {len(r.content)} bytes]",
            "final_url": r.url,
        }
    except Exception as e:
        return {"url": url, "label": label, "status": "ERROR", "error": str(e)[:100]}


sites = {
    # === Austria (AT) - BASG ===
    "AT_BASG_search": ("POST", "https://medikamente.basg.gv.at/api/v1/drug-shortage/search", {"page": 0, "size": 2}),
    "AT_BASG_export": ("GET", "https://medikamente.basg.gv.at/api/v1/drug-shortage/export", None),

    # === Belgium (BE) - FAMHP ===
    "BE_FAMHP_page": ("GET", "https://www.famhp.be/nl/MENSELIJK_gebruik/geneesmiddelen/geneesmiddelen/onbeschikbaarheden", None),
    "BE_FAMHP_api": ("GET", "https://geneesmiddelendb.fagg-afmps.be/api/v1/indisponibilities?language=nl", None),

    # === Bulgaria (BG) - BDA ===
    "BG_BDA_page": ("GET", "https://www.bda.bg/bg/component/content/article?id=1471", None),

    # === Croatia (HR) - HALMED ===
    "HR_HALMED_page": ("GET", "https://www.halmed.hr/Lijekovi/Informacije-o-lijekovima/Nestasice-lijekova/", None),

    # === Czech Republic (CZ) - SUKL ===
    "CZ_SUKL_api": ("GET", "https://www.sukl.cz/modules/medication/search_shortages.php?lang=en", None),
    "CZ_SUKL_page": ("GET", "https://prehledy.sukl.cz/prehled_leciv_702.html", None),

    # === Denmark (DK) - LMST ===
    "DK_LMST_page": ("GET", "https://laegemiddelstyrelsen.dk/da/special/forsyningsvanskeligheder/", None),
    "DK_LMST_api": ("GET", "https://laegemiddelstyrelsen.dk/da/special/forsyningsvanskeligheder/LMS_drug_shortage_list_export_excel/", None),

    # === Estonia (EE) - Ravimiamet ===
    "EE_RAVI_page": ("GET", "https://www.ravimiamet.ee/tarnehaired", None),
    "EE_RAVI_page2": ("GET", "https://www.ravimiamet.ee/ravimite-tarnehaired", None),

    # === Finland (FI) - FIMEA ===
    "FI_FIMEA_page": ("GET", "https://www.fimea.fi/saatavuushairiot", None),
    "FI_FIMEA_api": ("GET", "https://fimea.fi/api/jsonws/saatavuushairiot", None),

    # === France (FR) - ANSM ===
    "FR_ANSM_page": ("GET", "https://ansm.sante.fr/disponibilites-des-produits-de-sante/medicaments", None),
    "FR_ANSM_api": ("GET", "https://data.ansm.sante.fr/api/explore/v2.1/catalog/datasets", None),

    # === Germany (DE) - BfArM ===
    "DE_BFARM_api": ("GET", "https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml", None),
    "DE_BFARM_export": ("GET", "https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/api/exportExcel", None),

    # === Greece (GR) - EOF ===
    "GR_EOF_page": ("GET", "https://www.eof.gr/web/guest/ellipseis", None),

    # === Hungary (HU) - OGYEI ===
    "HU_OGYEI_page": ("GET", "https://www.ogyei.gov.hu/gyogyszerhianyok", None),
    "HU_OGYEI_api": ("GET", "https://ogyei.gov.hu/api/drug_shortages", None),

    # === Ireland (IE) - HPRA ===
    "IE_HPRA_page": ("GET", "https://www.hpra.ie/homepage/medicines/medicine-shortages", None),
    "IE_HPRA_api": ("GET", "https://www.hpra.ie/img/uploaded/swedocuments/Medicines-Shortages-List.xlsx", None),

    # === Italy (IT) - AIFA ===
    "IT_AIFA_page": ("GET", "https://www.aifa.gov.it/web/guest/carenze-e-indisponibilita", None),
    "IT_AIFA_api": ("GET", "https://api.aifa.gov.it/v1/carenze", None),

    # === Lithuania (LT) - VVKT ===
    "LT_VVKT_page": ("GET", "https://vvkt.lt/vaistiniu-preparatu-tiekimo-sutrikimai/", None),

    # === Netherlands (NL) - CBG-MEB / Farmanco ===
    "NL_CBG_page": ("GET", "https://www.cbg-meb.nl/onderwerpen/medicijntekorten", None),
    "NL_FARMANCO": ("GET", "https://www.farmanco.knmp.nl/", None),
    "NL_FARMANCO_API": ("GET", "https://www.farmanco.knmp.nl/api/v1/tekorten", None),

    # === Portugal (PT) - INFARMED ===
    "PT_INFARMED_page": ("GET", "https://www.infarmed.pt/web/infarmed/indisponibilidades", None),
    "PT_INFARMED_api": ("GET", "https://extranet.infarmed.pt/INFMED-fo/pesquisa-avancada-result.xhtml", None),

    # === Romania (RO) - ANM ===
    "RO_ANM_page": ("GET", "https://www.anm.ro/medicamente-de-uz-uman/deficit-de-medicamente/", None),

    # === Slovakia (SK) - SUKL ===
    "SK_SUKL_page": ("GET", "https://www.sukl.sk/hlavna-stranka/slovenska-verzia/pomocne-stranky/detail-lieku/?page_id=4222", None),

    # === Slovenia (SI) - JAZMP ===
    "SI_JAZMP_page": ("GET", "https://www.jazmp.si/humana-zdravila/informacije-o-zdravilih/motnje-pri-oskrbi-z-zdravili/", None),

    # === Spain (ES) - AEMPS/CIMA ===
    "ES_AEMPS_page": ("GET", "https://cima.aemps.es/cima/publico/listadesabastecimiento.html", None),
    "ES_AEMPS_api": ("GET", "https://cima.aemps.es/cima/rest/desabastecimiento", None),

    # === Sweden (SE) - Lakemedelsverket ===
    "SE_LV_page": ("GET", "https://www.lakemedelsverket.se/sv/behandling-och-forskrivning/lakemedel-som-ar-restnoterade", None),

    # === Iceland (IS) - IMA ===
    "IS_IMA_page": ("GET", "https://www.ima.is/medicines/shortages/", None),

    # === Norway (NO) - SLV/Legemiddelverket ===
    "NO_SLV_page": ("GET", "https://legemiddelverket.no/mangel-pa-legemidler", None),
    "NO_SLV_api": ("GET", "https://legemiddelverket.no/api/mangellisten", None),

    # === EU - EMA ===
    "EU_EMA_page": ("GET", "https://www.ema.europa.eu/en/human-regulatory-overview/post-authorisation/availability-medicines/shortages-catalogue", None),
    "EU_EMA_api": ("GET", "https://www.ema.europa.eu/api/shortages", None),
}

print("=" * 80)
print("MEDICINE SHORTAGE WEBSITE RESEARCH")
print("=" * 80)

results_by_country = {}

for key, (method, url, body) in sites.items():
    country = key.split("_")[0]
    result = test_url(url, method=method, json_body=body, label=key)
    if country not in results_by_country:
        results_by_country[country] = []
    results_by_country[country].append(result)

for country, results in sorted(results_by_country.items()):
    print(f"\n{'='*60}")
    print(f"  {country}")
    print(f"{'='*60}")
    for r in results:
        status = r.get("status", "?")
        label = r.get("label", "")
        url = r.get("url", "")
        ct = r.get("content_type", "")
        is_json = r.get("is_json", False)
        is_excel = r.get("is_excel", False)
        size = r.get("size", 0)
        final_url = r.get("final_url", url)
        error = r.get("error", "")

        status_icon = "OK" if status == 200 else "XX" if isinstance(status, int) else "!!"
        data_type = ""
        if is_excel:
            data_type = " [EXCEL]"
        elif is_json:
            data_type = " [JSON]"

        redirected = f" -> {final_url}" if final_url != url else ""

        print(f"  {status_icon} [{status}] {label}: {size:,} bytes {ct[:40]}{data_type}")
        if redirected:
            print(f"         Redirected: {final_url[:80]}")
        if error:
            print(f"         Error: {error}")
        if is_json and r.get("preview"):
            print(f"         Preview: {r['preview'][:200]}")
        if is_excel:
            print(f"         {r.get('preview', '')}")
