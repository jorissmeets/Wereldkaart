# Landkaart – ATC per land

Streamlit-app om per ATC-code te zien **in welke landen** die ATC gemeld is (op basis van de tekortmeldingen in de `output`-map).

## Starten

Vanuit de **projectroot** (Nieuwe Scraper):

```bash
streamlit run landkaart/app.py
```

Of vanuit de map `landkaart`:

```bash
streamlit run app.py
```

De app leest alle `*_shortage_*.csv` uit de map `output` en toont een wereldkaart: kies een ATC-code in de dropdown om de landen waar die ATC gemeld is te zien.

## Vereisten

- `streamlit`, `plotly` en `pandas` (staan in de hoofdmap `requirements.txt`).
