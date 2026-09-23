# Volgorde van tekorten over landen — data en valkuilen

**De vraag.** Trekken tekorten in een vaste volgorde over landen? Als Duitsland stelselmatig
kort voor Slovenië in tekort raakt, kan een Duitse melding als vroegsignaal dienen.

Deze map bevat de data om dat te onderzoeken. Hij bevat met opzet **geen conclusie**, want de
eerlijke samenvatting is: *met de huidige gegevens is die vraag niet betrouwbaar te
beantwoorden, maar vanaf nu bouwen we wel de meting op die het wél kan.*

Lees deze pagina voordat je een grafiek maakt. De valkuilen hieronder zijn geen slagen om de
arm — ze zijn de reden dat de eerste naïeve analyse uitkwam op *"Slowakije loopt 1.696 dagen
voor op Griekenland"*, oftewel vier en een half jaar.

---

## Wat er ligt

| bestand | wat het is |
|---|---|
| `dekking.csv` | **lees dit eerst.** Per land: hoe ver het register teruggaat, hoeveel gebeurtenissen, of het in het analysevenster meedoet |
| `gebeurtenissen.csv` | één rij per molecuul × land: de vroegste startdatum, met vlaggen |
| `paren.csv` | één rij per molecuul × landenpaar, met het verschil in dagen |
| `landenparen.csv` | samengevat per landenpaar — **naïef, niet voor conclusies** |
| `landenparen_venster.csv` | idem, maar beperkt tot een gemeenschappelijk venster |
| `panel.csv` | **het instrument voor de toekomst**, zie onder |

Herbouwen: `uv run --python 3.13 --with pandas python bouw_volgorde_dataset.py`
Het venster is verzetbaar: `VOLGORDE_VENSTER=2026-01-01 uv run … python bouw_volgorde_dataset.py`

---

## Drie redenen waarom de retrospectieve analyse wankel is

### 1. De datums meten niet hetzelfde

Dit is de zwaarste. `shortage_start` betekent per land iets anders: soms de dag dat de
levering feitelijk stokte, soms de dag dat de vergunninghouder het meldde, soms de dag dat de
toezichthouder publiceerde. De EU-richtlijn eist melding van een staking **twee maanden
vooraf**; lidstaten hebben dat verschillend aangescherpt.

Een land met een strengere voorafmeldplicht loopt daardoor per definitie voor — en dat zegt
niets over waar het tekort vandaan komt. **Dan meet je meldplicht, geen besmetting.**

Wat per land bekend is, staat in `../../datum_betekenis.json` en in de kolom
`datum_betekenis`. Gebruik `beide_vergelijkbaar` in `paren.csv` om alleen paren te houden
waarvan beide datums hetzelfde meten.

### 2. De registers lopen dertien jaar uiteen

Uit `dekking.csv`:

| land | vroegste melding | | land | vroegste melding |
|---|---|---|---|---|
| IJsland | 2000-01-01 | | Griekenland | 2024-05-13 |
| Slowakije | 2004-03-31 | | Tsjechië | 2024-08-02 |
| Hongarije | 2007-03-08 | | Maleisië | 2024-12-31 |

Elk land met een lang register "loopt voor" op elk land met een kort register, puur omdat het
langer bestaat. Dat is linker-censurering, geen signaal.

`landenparen_venster.csv` corrigeert hiervoor: alleen gebeurtenissen die **beide** landen
hadden kunnen zien. Landen waarvan het register ná de vensterstart begint, doen niet mee.

### 3. Sommige registers bewaren, andere verversen

Ook binnen een venster blijft er vertekening. IJsland houdt oude meldingen in de lijst
(mediane meldingsdatum januari 2025), België toont vooral de actuele stand (mediaan juni
2026). IJsland "loopt" daardoor op bijna alles voor, met medianen van 200 tot 280 dagen.

Een golf van 280 dagen is geen golf. Kijk altijd naar `spreiding_dagen`: bij deze paren is
die net zo groot als de mediaan, en dan is er geen patroon maar ruis.

---

## Hoe je er wél iets mee kunt

**Begin bij `dekking.csv`.** Kies zelf een venster waarin de landen die je vergelijkt
allemaal al bestonden.

**Filter in `paren.csv`** op `beide_vergelijkbaar = ja` en `een_voor_venster = nee`.

**Eis genoeg gedeelde moleculen.** De drempel staat op tien; onder de dertig zou ik niets
beweren. Kolom `gedeelde_moleculen`.

**Kijk naar spreiding, niet alleen naar de mediaan.** Een paar met mediaan 14 dagen en
spreiding 200 dagen is ruis. De kwartielen staan erbij.

**Onthoud dat gelijktijdigheid geen volgorde is.** Valt één Europese fabriek uit, dan worden
alle landen tegelijk geraakt; dat A drie dagen eerder meldde betekent niets. Het onderscheid
tussen "A veroorzaakt B" en "A en B delen een oorzaak" is met deze data niet te maken.

---

## Het panel: de meting die het wél kan

`panel.csv` legt vast wanneer **wij** een molecuul voor het eerst in een land zagen. Dat is
onafhankelijk van meldplicht en registerouderdom, en wordt voor elk land op precies dezelfde
manier bepaald: de dag dat het in onze verversing verscheen.

Elke wekelijkse run voegt een waarneming toe. Na een paar maanden is er een echt panel.

**Twee dingen om te weten.**

De nulmeting van 23-09-2026 bevat 10.515 combinaties met vlag `vanaf_begin = ja`. Die zeggen
niets over volgorde — die tekorten liepen al toen we begonnen te kijken. **Sluit ze uit.**
Alleen rijen met `vanaf_begin = nee` zijn waarnemingen.

En de resolutie is een week. Een golf die in drie dagen over Europa trekt, zie je hiermee
niet. Voor iets trager dan een week werkt het wel.

---

## Wat ik zou onderzoeken als ik jou was

1. **Toets eerst of er überhaupt iets te vinden is.** Schud de landcodes per molecuul door
   elkaar en herbereken. Komt de "sterkste" volgorde ook uit ruis, dan is er geen patroon.
2. **Kijk naar één oorzaak tegelijk.** `rc` in `data.json` geeft de reden. Een productieprobleem
   in één fabriek verspreidt anders dan een vraagpiek, en ze door elkaar halen middelt beide weg.
3. **Toets tegen iets wat je al weet.** Duitsland en Nederland delen veel
   vergunninghouders; als daar geen verband te zien is, is het instrument te stomp — niet de
   wereld te ongestructureerd.
4. **Vraag Farmanco erbij.** Nederland staat niet op de kaart (bewust: de kaart gaat over het
   buitenland), maar `farmanco_eml.json` en `sfk_verloop.json` bevatten de Nederlandse kant.
   Voor een vroegsignaal is dat juist de kant die je wilt voorspellen.

---

*Gebouwd 23-09-2026. Vragen over de herkomst van een veld: zie de docstring bovenaan
`bouw_volgorde_dataset.py` en `bouw_panel.py`.*
