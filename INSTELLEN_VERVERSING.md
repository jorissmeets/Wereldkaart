# Automatische verversing

De kaart ververst zichzelf **elke woensdag om 04:12** en publiceert alleen als de uitkomst
de controle doorstaat. Er is niets aan te zetten; hieronder staat wat er draait, wat je moet
weten, en hoe je het stopt.

## Wat er draait

| onderdeel | wat het doet |
|---|---|
| `~/Library/LaunchAgents/com.lcg.tekortendashboard.plist` | de wekker: woensdag 04:12 |
| `ververs_bewaakt.sh` | draait de verversing, toetst de uitkomst, publiceert of niet |
| `ververs_alles.sh` | de eigenlijke pijplijn (scrapen, verrijken, koppelen, bouwen) |
| `logs/laatste_run.txt` | één regel met de uitslag van de laatste run |
| `logs/bewaakt_<datum>.log` | het volledige verslag |

**launchd en niet cron.** Cron slaat een run zonder melding over als de Mac op dat moment
sliep. launchd onthoudt een gemiste afspraak en draait hem alsnog zodra de machine wakker
wordt. Bij een taak die één keer per week gaat is dat het verschil tussen een week oude kaart
en géén kaart.

**Woensdag en niet donderdag.** De geneesmiddelbespreking is donderdagochtend. Door woensdag
te draaien blijft er een hele werkdag over om een mislukte run te repareren.

## De rem

Publiceren gaat automatisch, maar alleen als de nieuwe kaart geloofwaardiger is dan de
huidige. Dat is een andere vraag dan "is de scrape geslaagd" — een scraper die stilletjes de
helft van een land laat vallen meldt namelijk netjes succes. Dat is deze zomer negen keer
gebeurd. De toets kijkt daarom naar de uitkomst en vergelijkt die met wat er nu live staat:

1. geen land verdwenen;
2. geen land dat meer dan de helft van zijn records verliest;
3. totaal niet meer dan een kwart afwijkend, omhoog noch omlaag;
4. actieve meldingen niet onder 60% of boven 140% van het huidige aantal;
5. **geen registerval**: geen land waarvan het *aandeel* actieve meldingen meer dan 30
   procentpunt omhoog springt. Dit is de fout die het vaakst is voorgekomen — 8.215 normaal
   geleverde Japanse producten die als tekort telden, 207 leverbare Oostenrijkse verpakkingen
   — en die een globale marge niet vangt;
6. geen land met minder dan tien records;
7. geen duplicaten (die zijn er eerder 17.088 geweest);
8. geen uitgesloten land terug op de kaart, Zwitserland voorop (licentievoorbehoud).

Faalt er één, dan blijft de oude kaart gewoon staan, komt er een melding in het
Berichtencentrum met de reden, en staat die reden in `logs/laatste_run.txt`.

De toets is getest op nagebootste storingen: een verdwenen land, een gehalveerd land, 900
duplicaten, Zwitserland dat terugkomt, een land dat naar vier records zakt, en twee varianten
van de registerval. Alle zes werden tegengehouden; normale schommelingen gingen door.

## Wat jij moet doen

**Drie dingen, waarvan er twee eenmalig zijn.**

1. **De Mac moet om 04:12 aan staan.** Dit is de meest waarschijnlijke reden dat er niets
   gebeurt. launchd haalt een gemiste run in zodra de machine wakker wordt, dus als je de
   laptop woensdagochtend opent draait hij alsnog — maar dan tijdens je werkdag, en hij duurt
   twee tot vier uur. Beter is de Mac zichzelf laten wekken:

   ```
   sudo pmset repeat wakeorpoweron W 04:05:00
   ```

   (`W` is woensdag. Vraagt je wachtwoord; dat kan ik niet voor je doen.) Controleren met
   `pmset -g sched`.

2. **Eén keer handmatig starten terwijl je erbij zit.** macOS vraagt de eerste keer mogelijk
   toestemming voor toegang tot je Documenten-map. Om 04:12 ziet niemand dat venster en blijft
   de run hangen. Dus:

   ```
   launchctl start com.lcg.tekortendashboard
   ```

   Laat staan, kijk na afloop in `logs/laatste_run.txt`. Dit is meteen de eerste volledige
   test: `ververs_alles.sh` heeft nog nooit als geheel gedraaid, alleen de stappen los.

3. **OpenAI-tegoed in de gaten houden.** De PRK-koppeling gebruikt o4-mini. Op 21-09 raakte
   het tegoed halverwege een run op, waardoor vijf landen geen koppelingen kregen. Dat is
   precies het soort storing dat de rem hierboven *niet* vangt: de kaart blijft geloofwaardig,
   hij wordt alleen stiller.

## Bedienen

```bash
launchctl list | grep lcg                 # draait hij?
launchctl start com.lcg.tekortendashboard # nu meteen een keer
cat logs/laatste_run.txt                  # uitslag laatste run
tail -f logs/bewaakt_$(date +%F).log      # meekijken tijdens een run

bash ververs_bewaakt.sh --droog           # alles doen behalve publiceren

launchctl bootout gui/$(id -u)/com.lcg.tekortendashboard   # stoppen
```

Na het stoppen komt hij pas terug met `launchctl bootstrap gui/$(id -u)
~/Library/LaunchAgents/com.lcg.tekortendashboard.plist`.

## Wat de rem niet vangt

Stilstand. Het vangnet in `_merge_live.py` houdt een land op de kaart als zijn scraper faalt,
zodat er niets verdwijnt. Daardoor ziet een bron die al weken niets nieuws levert er in de
cijfers hetzelfde uit als een bron die werkt. Een kwartaalcontrole op de bijwerkdatums per
land blijft dus nodig; die is niet te automatiseren met de vergelijking die hier staat.
