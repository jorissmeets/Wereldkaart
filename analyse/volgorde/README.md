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
| `landenparen_streng.csv` | **de enige tabel waar ik iets op zou baseren**: venster én beide datums een feitelijke start |
| `aangekondigd.csv` | tekorten met een startdatum in de toekomst — het enige vooruitkijkende |
| `panel.csv` | **het instrument voor de toekomst**, zie onder |

Herbouwen: `uv run --python 3.13 --with pandas python bouw_volgorde_dataset.py`
Het venster is verzetbaar: `VOLGORDE_VENSTER=2026-01-01 uv run … python bouw_volgorde_dataset.py`

---

## Wat de data nu laat zien: geen golf

Ik heb het met de strengst mogelijke filter bekeken — alleen landenparen waarvan **beide**
datums een feitelijke start meten, binnen een gemeenschappelijk venster. Dat zijn 3.356
molecuulparen over 13 landen.

De verdeling van de vertraging tussen twee landen:

| binnen | aandeel |
|---|---|
| 7 dagen | 5% |
| 14 dagen | 9% |
| 30 dagen | 16% |
| 60 dagen | 28% |
| 90 dagen | 38% |

Mediaan 133 dagen, kwartielen 52 tot 249.

**Als tekorten in een golf over landen trokken, zou je een piek bij korte vertraging zien.**
Die is er niet; de verdeling is vlak. En in élk "sterk" landenpaar uit
`landenparen_streng.csv` is de spreiding groter dan de mediaan — Noorwegen loopt op bijna
alles voor met medianen van 90 tot 160 dagen en spreidingen van 158 tot 192.

Dat betekent één van drie dingen, en welke weet ik niet:
1. landen raken grotendeels onafhankelijk van elkaar in tekort;
2. er is wél een golf, maar onze datums zijn te grof om hem te zien;
3. de golf zit in een deelverzameling (één oorzaakstype, één productvorm) die in het
   gemiddelde wegvalt.

Punt 3 is wat ik zelf als eerste zou onderzoeken.

---

## Drie redenen waarom de retrospectieve analyse wankel is

### 1. De datums meten niet hetzelfde

Dit is de zwaarste. `shortage_start` betekent per land iets anders: soms de dag dat de
levering feitelijk stokte, soms de dag dat de vergunninghouder het meldde, soms de dag dat de
toezichthouder publiceerde. De EU-richtlijn eist melding van een staking **twee maanden
vooraf**; lidstaten hebben dat verschillend aangescherpt.

Een land met een strengere voorafmeldplicht loopt daardoor per definitie voor — en dat zegt
niets over waar het tekort vandaan komt. **Dan meet je meldplicht, geen besmetting.**

Dit is uitgezocht per land, in de brondocumentatie en de wetgeving. Uitkomst: **van de 27
landen meten er 14 iets anders dan de feitelijke start.**

| betekenis | landen |
|---|---|
| feitelijke start | AT BE DE EE FI FR GR HR HU LV NO SI SK (13) |
| verwachte/geraamde start | AU IE IS IT MY SE (6) |
| publicatiedatum | CA CZ DK US (4) |
| meldingsdatum | ES RO (2) |
| registratiedatum | JP (1) |
| onbekend | SA (1) |

Het Verenigd Koninkrijk stond hier eerder bij en is op 24-09 van de kaart gehaald; het
datumonderzoek voor GB staat nog wel in `datum_betekenis.json`, met een vlag.

Alles staat met citaat en bron in `../../datum_betekenis.json`.

Let op één spanning die ik er niet uit heb gepoetst: **IJsland** staat als "vergelijkbaar"
maar meet `Áætlað upphaf` — een *geraamd* begin. En juist IJsland loopt in de ruwe data op
bijna alles voor. Gebruik daarom liever de kolom `beide_feitelijk` dan
`beide_vergelijkbaar`; die is strenger en houdt dit soort gevallen buiten de deur.

`landenparen_streng.csv` past dat filter al toe.

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

## Aangekondigde tekorten: het enige dat vooruitkijkt

Los van de volgordevraag zit er iets simpelers in de data. **695 meldingen hebben een
startdatum in de TOEKOMST** — leveranciers die aankondigen dat ze straks niet meer kunnen
leveren. Mediaan 31 dagen vooruit, over 382 moleculen.

Wie kondigt aan: Italië 196 moleculen, Zweden 100, Australië 62, Slowakije 30, Noorwegen 26.
De meeste landen doen het niet of nauwelijks, dus het signaal komt uit een handvol registers.

### Een sterk verband, maar pas op met de richting

Moleculen met een aankondiging zijn veel vaker breed in tekort dan moleculen zonder:

| | mét aankondiging | zonder |
|---|---|---|
| actief in ≥3 landen | 77% | 26% |
| actief in ≥5 landen | 62% | 14% |
| actief in ≥8 landen | **45%** | **5%** |

Gemiddeld aantal landen waar het nú loopt: 7,2 tegen 2,2.

**Dat is een verband, geen voorspelling.** De richting is met deze momentopname niet vast te
stellen, en de meest waarschijnlijke verklaring is de omgekeerde van wat je hoopt: een
molecuul dat al in acht landen op is, wordt nu eenmaal eerder in een negende aangekondigd.
Dan is de aankondiging een *achterlopende* indicator van een probleem dat al breed is, niet
een vooruitlopende.

### Wat wél vooruitkijkt

Slechts **36 moleculen** zijn aangekondigd én nergens al actief. Dat is de enige groep waar
de aankondiging nieuws is in plaats van een echo. Ze staan in `aangekondigd.csv`.

Of die 36 daadwerkelijk uitbreken is met een momentopname niet te toetsen — daar is het panel
voor. Zodra dat een paar maanden loopt, is dit de eerste vraag om te stellen: **van de
moleculen die ergens werden aangekondigd terwijl ze nergens liepen, hoeveel braken er
werkelijk uit, en waar het eerst?** Dat is een echte toets op een echt vroegsignaal, en veel
directer te beantwoorden dan de golfvraag.

---

## Het panel: de meting die het wél kan

`panel.csv` legt vast wanneer **wij** een molecuul voor het eerst in een land zagen. Dat is
onafhankelijk van meldplicht en registerouderdom, en wordt voor elk land op precies dezelfde
manier bepaald: de dag dat het in onze verversing verscheen.

Elke wekelijkse run voegt een waarneming toe. Na een paar maanden is er een echt panel.

**Twee dingen om te weten.**

De nulmeting van 23-09-2026 bevat 10.445 combinaties met vlag `vanaf_begin = ja`. Die zeggen
niets over volgorde — die tekorten liepen al toen we begonnen te kijken. **Sluit ze uit.**
Alleen rijen met `vanaf_begin = nee` zijn waarnemingen.

Datzelfde geldt voor een land dat nieuw in de data komt: dat levert in één keer honderden
'nieuwe' combinaties op die in werkelijkheid al liepen. `bouw_panel.py` vangt dat sinds 24-09
af — een nieuw land krijgt `vanaf_begin = ja`. Het Verenigd Koninkrijk liet zien waarom: 70
regels kwamen als verse verschijningen binnen terwijl er alleen een bron was bijgekomen.

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
4. **Kijk naar de aangekondigde tekorten** (zie hieronder). Dat is het enige echt
   vooruitkijkende dat in deze data zit, en het is nog niet getoetst.

---

*Gebouwd 23-09-2026. Vragen over de herkomst van een veld: zie de docstring bovenaan
`bouw_volgorde_dataset.py` en `bouw_panel.py`.*
