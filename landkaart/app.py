"""
Streamlit-app: wereldkaart van tekortmeldingen per ATC-code.
Laadt alle CSV-bestanden uit de output-map en toont per gekozen ATC in welke landen die ATC gemeld is.
"""

import pandas as pd
from pathlib import Path
import streamlit as st
import plotly.express as px

# Pad naar de output-map (één niveau boven landkaart)
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

# Plotly choropleth gebruikt ISO-3 (drie letters), onze data heeft ISO-2 → omzetten
ISO2_TO_ISO3 = {
    "AT": "AUT", "AU": "AUS", "BE": "BEL", "CA": "CAN", "CH": "CHE", "CO": "COL",
    "CZ": "CZE", "DE": "DEU", "ES": "ESP", "FI": "FIN", "FR": "FRA", "GR": "GRC",
    "HR": "HRV", "IE": "IRL", "IS": "ISL", "IT": "ITA", "KR": "KOR", "LV": "LVA",
    "MY": "MYS", "NL": "NLD", "NO": "NOR", "RO": "ROU", "SE": "SWE", "SK": "SVK",
    "TR": "TUR", "US": "USA", "GB": "GBR", "UK": "GBR", "PT": "PRT",
    "DK": "DNK", "PL": "POL", "HU": "HUN", "BG": "BGR", "EE": "EST", "LT": "LTU",
    "SI": "SVN", "JP": "JPN", "IN": "IND", "BR": "BRA", "MX": "MEX", "ZA": "ZAF",
}

# Kolomnamen die we zoeken voor ATC (verschillende bronnen)
ATC_COLUMNS = ["atc_code", "atc_level1", "Atc Code"]
COUNTRY_CODE_COL = "country_code"
COUNTRY_NAME_COL = "country_name"


def load_all_shortage_data() -> pd.DataFrame:
    """Leest alle *_shortage_*.csv uit output en combineert rijen met ATC + land."""
    if not OUTPUT_DIR.exists():
        return pd.DataFrame()

    rows = []
    for p in OUTPUT_DIR.glob("*_shortage_*.csv"):
        try:
            df = pd.read_csv(p, encoding="utf-8", on_bad_lines="skip", low_memory=False)
        except Exception:
            df = pd.read_csv(p, encoding="latin-1", on_bad_lines="skip", low_memory=False)

        atc_col = None
        for c in ATC_COLUMNS:
            if c in df.columns:
                atc_col = c
                break
        if atc_col is None:
            continue

        if COUNTRY_CODE_COL not in df.columns:
            continue

        for _, row in df.iterrows():
            atc = row.get(atc_col)
            if pd.isna(atc) or str(atc).strip() == "":
                continue
            atc = str(atc).strip()
            cc = row.get(COUNTRY_CODE_COL)
            if pd.isna(cc) or str(cc).strip() == "":
                continue
            cc = str(cc).strip().upper()
            cn = row.get(COUNTRY_NAME_COL, cc)
            if pd.isna(cn):
                cn = cc
            rows.append({"atc": atc, "country_code": cc, "country_name": str(cn).strip()})

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).drop_duplicates()
    return out


def get_atc_country_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Per ATC en land één rij (geschikt voor choropleth: land + aantal meldingen)."""
    return df.groupby(["atc", "country_code"], as_index=False).agg(
        country_name=("country_name", "first"),
        count=("country_code", "count"),
    )


def main():
    st.set_page_config(page_title="ATC per land – Tekortmeldingen", layout="wide")
    st.title("Waar is een ATC-code gemeld?")
    st.caption("Data uit de output-map. Selecteer een ATC-code om op de kaart te zien in welke landen deze gemeld is.")

    df = load_all_shortage_data()
    if df.empty:
        st.warning(f"Geen data gevonden. Zet CSV-bestanden in: `{OUTPUT_DIR}`")
        return

    matrix = get_atc_country_matrix(df)
    atc_list = sorted(matrix["atc"].unique().tolist())

    if not atc_list:
        st.warning("Geen ATC-codes in de data.")
        return

    chosen = st.selectbox(
        "Kies een ATC-code (of atc_level1)",
        options=atc_list,
        index=0,
    )

    # Landen waar deze ATC gemeld is
    subset = matrix[matrix["atc"] == chosen].copy()
    subset["gemeld"] = 1

    # Plotly choropleth gebruikt ISO-3 (drie letters); onze data is ISO-2 → omzetten
    subset["country_iso3"] = subset["country_code"].map(
        lambda x: ISO2_TO_ISO3.get(str(x).upper() if pd.notna(x) else "", None)
    )
    subset = subset.dropna(subset=["country_iso3"])  # EU e.d. vallen af
    if subset.empty:
        st.info("Geen landen met ISO-code voor deze ATC (bijv. alleen EU).")
        return

    fig = px.choropleth(
        subset,
        locations="country_iso3",
        locationmode="ISO-3",
        color="gemeld",
        hover_name="country_name",
        hover_data={"country_code": True, "count": True, "gemeld": False},
        color_continuous_scale=[[0, "rgb(220,220,220)"], [1, "rgb(26, 115, 232)"]],  # grijs → blauw
        range_color=(0.5, 1),
        scope="world",
        labels={"count": "Aantal meldingen"},
    )
    fig.update_layout(
        coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=30, b=0),
        title=f"Landen waar ATC « {chosen} » gemeld is",
        geo=dict(bgcolor="rgba(245,245,245,0.8)", lakecolor="white"),
    )
    fig.update_traces(showlegend=False)

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Tabel: landen voor gekozen ATC"):
        st.dataframe(
            subset[["country_code", "country_name", "count"]].sort_values("country_code"),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
