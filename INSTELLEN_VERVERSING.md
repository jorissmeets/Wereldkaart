# Automatische verversing instellen

Het LCG wil de data elke twee dagen vers, met **donderdagochtend** als belangrijkste moment
(geneesmiddelbespreking) en **dinsdag** als uiterste deadline zodat er nog tijd is om een
mislukte run te repareren.

## De regel

Open de crontab met `crontab -e` en plak:

```cron
# LCG-tekortendashboard: verversen op zondag, dinsdag en donderdag om 04:12
12 4 * * 0,2,4 cd /Users/karkara/Documents/LCG/Landkaart && /bin/bash ververs_alles.sh >> logs/cron.log 2>&1
```

Donderdag 04:12 levert verse data voor de ochtendbespreking; dinsdag is het vangnet; zondag
houdt de tweedaagse cadans erin.

Publiceren gebeurt **niet** automatisch. `ververs_alles.sh` zet pas live met `--deploy`, en dat
is met opzet: een half mislukte scrape mag nooit vanzelf de live kaart overschrijven. Wil je het
toch volautomatisch, zet dan `--deploy` achter het scriptpad — maar lees dan eerst de sectie
"Wat er mis kan gaan".

## Voorwaarden

Zonder deze drie draait de verversing niet:

1. **De Mac moet wakker zijn.** Cron draait niet als de laptop slaapt. Zet in Systeeminstellingen
   → Batterij → Opties een wektijd, of gebruik `caffeinate`. Alternatief is `launchd`, dat een
   gemiste run inhaalt zodra de machine weer aan gaat — zeg het als je dat wilt, dan bouw ik het om.
2. **OpenAI-tegoed.** De PRK-koppeling gebruikt o4-mini. Op 21-09 raakte het tegoed halverwege
   de run op, waardoor vijf landen geen koppelingen kregen.
3. **Tijd.** Een volledige run duurt twee tot vier uur; Slowakije alleen al ~55 minuten en de
   PRK-koppeling van Japan is de traagste stap.

## Wat er mis kan gaan

De merge met de vorige live-data vangt uitval per land op, zodat een kapotte scraper geen land
van de kaart laat verdwijnen. Dat vangnet verbergt echter ook stilstand: een bron die al weken
niets nieuws levert, ziet er in de cijfers hetzelfde uit als een bron die werkt.

Kijk daarom na elke run in `logs/ververs_<datum>.log` naar de regel onder "### 7. Controle".
Die meldt landen met minder dan tien records — dat is vrijwel altijd een kapotte scraper.
