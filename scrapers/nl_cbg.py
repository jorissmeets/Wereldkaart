"""DEPRECATED — geen publieke CBG-bron beschikbaar.

Deze scraper is gestopt vóór werkelijk gebruik. Conclusie uit onderzoek (mei 2026):

- Het Meldpunt geneesmiddelentekorten en -defecten (meldpuntgeneesmiddelentekortendefecten.nl)
  is een inzendingsportaal voor handelsvergunninghouders en fabrikanten — er is géén publieke
  meldingenlijst toegankelijk zonder credentials.
- Farmanco (KNMP) is gesloten achter login.
- De cbg-meb.nl pagina onder /onderwerpen/medicijntekorten levert geen gestructureerde
  meldingenlijst die zinvol gescrapet kan worden voor onze use case.

Voor NL blijft `nl_igj.py` (IGJ-vrijstellingsbesluiten) de gebruikte bron, met de bekende
beperking dat dit een reactieve subset is van de echte tekortmeldingen.

Niet importeren in `scrapers/__init__.py`.
"""

from scrapers.base_scraper import BaseScraper  # noqa: F401 — gehouden zodat imports niet breken
