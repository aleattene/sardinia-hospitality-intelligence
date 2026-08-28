# Sardinia Hospitality Intelligence: Report Esecutivo

> **Analisi data-driven della domanda turistica e dell'offerta ricettiva nelle province sarde**
> Dati: open data di fonte ISTAT, 2018-2024 · Data di analisi: aprile 2026

> **Rigenerare le figure:** tutti i grafici di questo report sono prodotti da
> [`notebooks/01_eda_demand_supply.ipynb`](../../notebooks/01_eda_demand_supply.ipynb).
> Eseguire `jupyter nbconvert --to notebook --execute notebooks/01_eda_demand_supply.ipynb --inplace`
> per ricostruire i PNG in `reports/figures/` (EN) e `reports/figures/it/` (IT).

---

## Sintesi esecutiva

Il mercato turistico della Sardegna ha completato il pieno recupero post-pandemia e oggi
supera i livelli pre-pandemia di circa il **25%**, raggiungendo **4,44 milioni di arrivi
nel 2024**. La crescita è distribuita in modo disomogeneo tra le province, e un
disallineamento persistente tra concentrazione della domanda e capacità ricettiva crea
al tempo stesso rischi e opportunità azionabili.

**Cinque numeri chiave:**

| Metrica | Valore |
|---------|--------|
| Arrivi totali 2024 | ~4,44 M (+25% sul 2019) |
| Massima pressione sull'occupazione | Nuoro: 55,4% |
| Segmento in crescita più rapida | Sassari / Affitti brevi (+38,7% YoY) |
| Provincia meno stagionale | Cagliari (indice di stagionalità 0,13) |
| Prima priorità di espansione | Nuoro (punteggio composito 0,74) |

**Risultati principali:**
- **Il gap di offerta è reale ma non uniforme.** Nuoro e Cagliari registrano una
  pressione significativa sull'occupazione; Oristano ha capacità disponibile ma una
  domanda debole per riempirla.
- **Gli affitti brevi stanno ridisegnando il mercato.** L'extra-alberghiero cresce a un
  ritmo 3-4 volte superiore agli hotel tradizionali ed è ormai il primo motore di
  crescita in ogni provincia.
- **La concentrazione stagionale è il rischio strutturale.** Oltre metà delle presenze
  annue si concentra in una finestra estiva di 3 mesi; le strategie di allungamento
  della stagione possono sbloccare ricavi annuali in province come Cagliari.

---

## 1. Dati e metodologia

### Fonti dati

| Dataset | Fonte | Copertura |
|---------|-------|-----------|
| Flussi turistici (`raw_tourism_flows`) | ISTAT: Movimento clienti negli esercizi ricettivi | Provincia × mese × anno × tipo struttura × provenienza |
| Capacità ricettiva (`raw_accommodation_capacity`) | ISTAT: Capacità degli esercizi ricettivi | Provincia × anno × tipo struttura |

Entrambi i dataset sono open data di fonte ISTAT, scaricati dal portale open data
dell'Osservatorio del Turismo della Regione Sardegna (nessuna autenticazione richiesta).
Copertura: **2018-2024** per le cinque province sarde: Cagliari, Sassari, Nuoro,
Sud Sardegna, Oristano.

### Pipeline

```
CSV (ISTAT) → DuckDB (tabelle raw)
           → views SQL (aggregazioni, segmentazioni, trend YoY)
           → queries SQL (punteggio di priorità, indice di stagionalità, ranking di crescita)
           → export CSV → notebook Jupyter (EDA, visualizzazioni)
```

Tutte le trasformazioni sono espresse in file SQL puri (`sql/views/`, `sql/queries/`)
ed eseguite su un'istanza DuckDB locale: nessun server richiesto.

### Metrica chiave: occupancy proxy

In assenza di dati a livello di prenotazione, l'occupazione è approssimata come:

```
occupancy_proxy (%) = total_nights / (total_beds × 365) × 100
```

La metrica misura l'utilizzo dello stock di posti letto nell'ipotesi che tutti i letti
siano disponibili ogni giorno dell'anno. Sottostima l'occupazione reale in alta stagione
(i letti non sono disponibili in modo uniforme tutto l'anno), ma fornisce un indicatore
coerente e confrontabile tra province e anni.

---

## 2. Ripresa della domanda (2018-2024)

![Trend della domanda totale in Sardegna](../figures/it/fig_01_demand_trend.png)

Il turismo sardo ha subito un **crollo di circa il 60% degli arrivi nel 2020** a causa
della pandemia da COVID-19, seguito da un forte rimbalzo nel 2021-2022. Nel 2023 la
regione era già tornata ai livelli del 2019; nel 2024 li ha superati con margine.

**Fotografia 2024 per provincia:**

![Arrivi e presenze per provincia, 2024](../figures/it/fig_02_demand_by_province.png)

| Provincia | Arrivi 2024 | Presenze 2024 |
|-----------|-------------|---------------|
| Sassari | I più alti | Le più alte |
| Cagliari | 2° | 2° |
| Sud Sardegna | 3° | 3° |
| Nuoro | 4° | 4° |
| Oristano | I più bassi | Le più basse |

**Traiettorie provinciali:**

![Trend degli arrivi per provincia (2018-2024)](../figures/it/fig_03_demand_trend_province.png)

Sassari ha guidato la ripresa, aggiungendo circa **+2,15 milioni di arrivi** rispetto
alla propria base 2019, trainata dalla forte crescita dell'extra-alberghiero e da
un'elevata domanda internazionale. Oristano ha mostrato la ripresa più debole e i
volumi assoluti più bassi lungo tutto il periodo.

---

## 3. Gap domanda-offerta

### Occupancy proxy: fotografia 2024

![Occupancy proxy per provincia, 2024](../figures/it/fig_04_occupancy_proxy.png)

Nel 2024 nessuna provincia ha raggiunto un livello di saturazione (tipicamente definito
come occupazione >70%), il che segnala spazio di espansione ricettiva su tutta l'isola.
La pressione è però concentrata:

| Provincia | Occupancy proxy (2024) | Interpretazione |
|-----------|------------------------|-----------------|
| Nuoro | **55,4%** | Vincolo più stretto: espansione giustificata |
| Cagliari | **51,3%** | Pressione moderata: espansione selettiva percorribile |
| Sud Sardegna | **49,4%** | Vicina alla soglia: monitorare il trend |
| Sassari | ~48% | Crescita rapida: attenzione a irrigidimenti a breve |
| Oristano | **43,0%** | Ampio margine inutilizzato: prima serve attivare la domanda |

### Trend dell'occupazione nel tempo

![Occupancy proxy per provincia (2018-2024)](../figures/it/fig_05_occupancy_trend.png)

La serie storica mostra un trend crescente costante post-2020 per Nuoro e Cagliari,
segno che la domanda si è ripresa più in fretta di quanto sia stata aggiunta capacità.
L'occupazione di Oristano è rimasta piatta: la capacità esiste ma la domanda non la
sta attivando.

---

## 4. Stagionalità

### Concentrazione mensile

![Distribuzione mensile delle presenze per provincia, 2024 (%)](../figures/it/fig_06_seasonality_heatmap.png)

In tutte le province il turismo è dominato da una **finestra estiva ristretta**. Luglio
e agosto da soli valgono la maggioranza delle presenze annue. La heatmap mostra
un'attività quasi nulla tra novembre e febbraio nella maggior parte delle province, in
particolare Nuoro e Sud Sardegna.

### Indice di stagionalità

![Indice di stagionalità e quota del mese di picco](../figures/it/fig_07_seasonality_index.png)

L'indice di stagionalità è calcolato come concentrazione alla Herfindahl delle quote mensili:

```
seasonality_index = Σ (month_share²)    su tutti i 12 mesi
```

Una distribuzione perfettamente uniforme sull'anno vale 0,0833 (1/12); una stagione
interamente concentrata in un solo mese vale 1,0. Valori più alti indicano un rischio
stagionale maggiore.

| Provincia | Indice di stagionalità | Quota mese di picco | Quota top 3 mesi |
|-----------|------------------------|---------------------|------------------|
| Cagliari | **0,13** | **20%** | ~52% |
| Sassari | ~0,15 | ~23% | ~57% |
| Sud Sardegna | ~0,19 | ~28% | ~64% |
| Nuoro | ~0,19 | ~28% | ~66% |
| Oristano | ~0,16 | ~24% | ~58% |

**Cagliari si distingue** come la provincia meno stagionale (indice 0,13, mese di picco
al 20% delle presenze annue): la candidata più forte per strategie annuali come turismo
congressuale, eventi culturali e pacchetti fuori stagione.

---

## 5. Segmentazione dei turisti

### Provenienza: italiani vs internazionali

![Arrivi italiani vs internazionali per provincia, 2024](../figures/it/fig_08_origin_domestic_intl.png)

I turisti internazionali rappresentano un segmento consistente e strategicamente
prezioso in tutte le province. Esprimono tipicamente una spesa per soggiorno più alta
e una minore elasticità al prezzo rispetto ai turisti italiani.

| Provincia | Quota internazionale (2024) |
|-----------|------------------------------|
| Sassari | **59,5%**: la più diversificata a livello internazionale |
| Nuoro | ~55% |
| Cagliari | ~50% |
| Sud Sardegna | **41%**: la più orientata al mercato domestico |

### Principali mercati di provenienza

![Top 10 paesi di provenienza, Sardegna 2024](../figures/it/fig_09_origin_top_countries.png)

I mercati europei dominano gli arrivi internazionali. **Germania, Francia e Regno
Unito** sono i primi tre paesi di provenienza, a conferma dell'importanza dei
collegamenti con il Nord Europa (rotte aeree, traghetti) per l'economia turistica
dell'isola.

---

## 6. Tendenze dell'offerta ricettiva

### Mix di mercato per provincia

![Mix ricettivo per provincia, 2024 (% degli arrivi)](../figures/it/fig_10_accommodation_mix.png)

Gli affitti brevi (appartamenti privati, B&B, case vacanza) rappresentano ormai una
quota significativa degli arrivi in ogni provincia. La loro traiettoria di crescita
supera ampiamente quella degli hotel tradizionali.

### Durata media del soggiorno

![Durata media del soggiorno per tipo di struttura, 2024](../figures/it/fig_11_avg_stay_length.png)

Gli ospiti degli affitti brevi soggiornano in media più a lungo degli ospiti degli
hotel, un pattern osservato in modo coerente in tutte le province. Segnala un profilo
di turista qualitativamente diverso: viaggiatori in modalità vacanza piuttosto che
ospiti di passaggio o business.

### Crescita anno su anno per segmento

| Provincia | Tipo di struttura | Crescita arrivi YoY (2023-2024) |
|-----------|-------------------|--------------------------------|
| Sassari | Affitti brevi | **+38,7%** |
| Nuoro | Affitti brevi | **+32,5%** |
| Sud Sardegna | Affitti brevi | **+31,1%** |
| Cagliari | Affitti brevi | ~+25% |
| Oristano | Affitti brevi | ~+20% |
| Cagliari | Hotel | +13% |
| Sassari | Hotel | +8% |
| Oristano | Hotel | **-6,3%** |

Il boom degli affitti brevi è un cambiamento strutturale dell'intero mercato. La
contrazione degli hotel a Oristano (-6,3%) riflette la combinazione di domanda debole
e spiazzamento competitivo da parte di formati ricettivi più flessibili.

---

## 7. Priorità di espansione

### Modello del punteggio di priorità

Per ordinare le province per attrattività di espansione si calcola un **punteggio
composito di priorità** da tre componenti a pesi uguali, ciascuna normalizzata min-max
nell'intervallo [0, 1]:

| Componente | Proxy di | Peso |
|------------|----------|------|
| Occupancy proxy | Vincolo attuale dell'offerta | 1/3 |
| Crescita arrivi YoY | Slancio del mercato | 1/3 |
| Quota internazionale | Potenziale del segmento premium | 1/3 |

```
priority_score = mean(occupancy_norm, yoy_growth_norm, intl_share_norm)
```

### Classifica delle province

![Punteggio di priorità per provincia](../figures/it/fig_12_priority_score.png)

| Posizione | Provincia | Punteggio | Occupazione | Crescita YoY | Quota intl |
|-----------|-----------|-----------|-------------|--------------|------------|
| 1 | **Nuoro** | 0,74 | 55,4% | Alta | ~55% |
| 2 | **Sassari** | 0,72 | ~48% | La più alta | 59,5% |
| 3 | Cagliari | 0,58 | 51,3% | Moderata | ~50% |
| 4 | Sud Sardegna | 0,50 | 49,4% | Moderata | 41% |
| 5 | Oristano | 0,06 | 43,0% | Bassa | Bassa |

### Segmenti a maggiore crescita

![Segmenti a maggiore crescita, arrivi YoY (%)](../figures/it/fig_13_growth_segments.png)

### Mappa di posizionamento strategico

![Posizionamento delle province: occupazione vs crescita YoY](../figures/it/fig_14_bubble_chart.png)

Il bubble chart colloca ogni provincia su una griglia bidimensionale: **pressione
sull'occupazione** (asse x) e **slancio di crescita** (asse y). La dimensione della
bolla è proporzionale alla quota internazionale. Le province nel **quadrante in alto a
destra** (alta occupazione + crescita rapida) rappresentano i casi di espansione più
urgenti.

- **Nuoro e Sassari** occupano il quadrante in alto a destra: entrambe vincolate sulla
  capacità e in rapida crescita.
- **Cagliari** si colloca vicino alle mediane: posizione equilibrata, adatta a
  un'espansione costante e misurata.
- **Oristano** cade nel quadrante in basso a sinistra: lo stimolo della domanda deve
  precedere l'investimento in offerta.

---

## 8. Raccomandazioni

### Nuoro: priorità alta

Nuoro mostra la pressione sull'occupazione più alta (55,4%) e una forte domanda
internazionale (~55%). Il mercato sta assorbendo nuova capacità e gli affitti brevi
crescono rapidamente.

- **Espandere la capacità ricettiva**, in particolare nei formati extra-alberghieri
  (agriturismi, boutique rental) allineati al posizionamento su natura e cultura.
- **Puntare sui segmenti internazionali premium**: mercati mitteleuropei (Germania,
  Svizzera, Austria) con esperienze di trekking, cicloturismo e cultura locale autentica.
- **Investire in infrastrutture** (accessibilità, trasporto locale) per ridurre le
  frizioni per i visitatori internazionali in arrivo dai gateway di Cagliari e Olbia.

### Sassari: priorità alta

Sassari guida la crescita YoY (+38,7% per gli affitti brevi) e ha la quota
internazionale più alta (59,5%) tra tutte le province.

- **Capitalizzare lo slancio**: accelerare l'infrastruttura degli affitti brevi e
  semplificare i processi autorizzativi per far emergere l'offerta.
- **Rafforzare il marketing internazionale** su Regno Unito, Germania e Francia: i tre
  mercati di provenienza già più coinvolti.
- **Sviluppare offerte di mezza stagione** (eventi culturali in primavera, enogastronomia
  in autunno) per ridurre la dipendenza dal picco estivo.

### Cagliari: priorità medio-alta

Cagliari combina una pressione moderata sull'occupazione (51,3%) con la **stagionalità
più bassa** di tutte le province: la piattaforma migliore per strategie annuali.

- **Investire in infrastrutture congressuali e MICE** (Meetings, Incentives,
  Conferences, Events) per attrarre viaggi business fuori dalla stagione estiva.
- **Espandere l'offerta di boutique hotel e strutture di design** per intercettare il
  segmento in crescita dei city break dalle città europee.
- **Sfruttare l'indice di stagionalità più basso** per negoziare con le compagnie aeree
  il mantenimento delle rotte tutto l'anno, riducendo il crollo invernale degli orari.

### Sud Sardegna: priorità media

Sud Sardegna ha una base turistica sbilanciata sul mercato domestico (41%
internazionale) e un'occupazione moderata (49,4%). La crescita c'è, ma sotto il ritmo
di Nuoro e Sassari.

- **Migliorare la visibilità internazionale**: marketing mirato in Francia e Germania
  per il turismo balneare, coerente con la geografia costiera della provincia.
- **Monitorare il tetto di occupazione**: se la traiettoria di crescita attuale
  continua, i vincoli di offerta emergeranno entro 2-3 anni.
- **Investire in infrastrutture costiere sostenibili** per proteggere l'attrattività di
  lungo periodo della destinazione per il segmento internazionale premium.

### Oristano: priorità bassa (fase di attivazione della domanda)

Con un'occupazione al 43,0%, gli hotel in contrazione (-6,3% YoY) e il punteggio di
priorità più basso (0,06), Oristano dovrebbe concentrarsi sullo **stimolare la domanda
prima di aggiungere offerta**.

- **Attivare il turismo di nicchia**: l'ecosistema lagunare (Stagno di Cabras),
  l'archeologia (Tharros) e il birdwatching nelle zone umide sono asset differenzianti
  poco valorizzati dal marketing.
- **Collaborare con i tour operator** per creare itinerari curati che combinino
  Oristano con le province a maggior traffico (gite giornaliere da Cagliari, circuiti
  di più giorni con Nuoro).
- **Rinviare gli investimenti ricettivi su larga scala** finché le metriche di domanda
  (arrivi, occupazione) non mostrino una ripresa sostenuta sopra il 48-50% di
  occupancy proxy.

---

## Appendice

### Fonti dati

| Fonte | URL |
|-------|-----|
| Osservatorio del Turismo, Regione Sardegna: open data (fonte ISTAT) | [osservatorio.sardegnaturismo.it](https://osservatorio.sardegnaturismo.it/it/open-data) |
| ISTAT: Movimento clienti e Capacità ricettiva (fonte statistica) | [dati.istat.it](https://dati.istat.it) |

### Definizioni dei campi

| Campo | Definizione |
|-------|-------------|
| `arrivals` | Numero di ospiti che effettuano il check-in nelle strutture nel periodo di riferimento |
| `nights` | Presenze totali (notti per ospite) nel periodo di riferimento |
| `occupancy_proxy` | `(total_nights / (total_beds × 365)) × 100` |
| `seasonality_index` | Concentrazione alla Herfindahl delle 12 quote mensili di presenze: `Σ(month_share²)` |
| `priority_score` | Media a pesi uguali di occupazione, crescita YoY e quota internazionale, normalizzate min-max |

### Avvertenze

- L'**occupancy proxy** è una stima per difetto dell'occupazione in alta stagione:
  assume che tutti i letti siano disponibili 365 giorni l'anno.
- I **dati di capacità 2021** sono privi del dettaglio provinciale nella fonte ISTAT:
  i record interessati sono esclusi dai calcoli del gap domanda-offerta per quell'anno.
- **Cambio di schema 2022-2023:** ISTAT ha rimosso il campo `origin_macro` dai dati sui
  flussi turistici, rendendo necessaria una regola di classificazione per mantenere la
  segmentazione italiani/internazionali.
- **Perimetro provinciale:** l'analisi copre le cinque province sarde attuali. La
  provincia del Sud Sardegna è nata nel 2016 dalla fusione di parti di Cagliari e
  Carbonia-Iglesias; i dati storici precedenti al 2018 non sono direttamente
  confrontabili.
