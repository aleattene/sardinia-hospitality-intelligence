# Sardinia Hospitality Intelligence <a href="#"><img src="https://github.githubassets.com/images/icons/emoji/unicode/1f1ee-1f1f9.png?v8" width="28" alt="Versione italiana"/></a> <a href="../README.md"><img src="https://github.githubassets.com/images/icons/emoji/unicode/1f1ec-1f1e7.png?v8" width="28" alt="English version"/></a>

![Test & Coverage](https://github.com/aleattene/sardinia-hospitality-intelligence/actions/workflows/test.yml/badge.svg)
![Lint & Format](https://github.com/aleattene/sardinia-hospitality-intelligence/actions/workflows/lint.yml/badge.svg)
[![codecov](https://codecov.io/gh/aleattene/sardinia-hospitality-intelligence/graph/badge.svg?token=1TXMAP8EU8)](https://codecov.io/gh/aleattene/sardinia-hospitality-intelligence)
![Python](https://img.shields.io/badge/Python-3.13-blue)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Analysis-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytical%20DB-yellow)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange)
![Matplotlib](https://img.shields.io/badge/Matplotlib-Visualization-green)
![License](https://img.shields.io/badge/License-MIT-blue)
![Last Commit](https://img.shields.io/github/last-commit/aleattene/sardinia-hospitality-intelligence)

> Sette annate di dati e cinque province: 
> - dove la domanda preme sull'offerta,
> - dove conviene espandersi per primi.

---

Progetto di **Data Analysis** end-to-end che mappa la domanda turistica e l'offerta
ricettiva delle province sarde (open data di fonte ISTAT), individuando gap geografici
e stagionali a supporto di decisioni di espansione data-driven nel settore ricettivo.

---

<br/>

## Le tre tappe dell'analisi

1. **Dimensione della domanda**: quanto vale ogni mercato provinciale e come questo respira
   nell'anno. *"Chi sono i turisti? Qual è il profilo stagionale?"*


2. **Gap domanda-offerta**: dove la capacità ricettiva non tiene il passo degli arrivi,
   misurato con un occupancy proxy per provincia. *"Dove lo squilibrio è più ampio?"*


3. **Direzione**: dove puntano insieme pressione sull'occupazione, crescita e apertura
   internazionale, riassunte in un punteggio composito di priorità. *"Quali segmenti
   corrono più veloci? Dove conviene espandersi per primi?"*

Il notebook ([`01_eda_demand_supply.ipynb`](../notebooks/01_eda_demand_supply.ipynb))
percorre le tre tappe a profondità EDA. 

La roadmap approfondisce la terza tappa: analisi statistica e forecasting della domanda sulle annate più recenti del 
portale.

---

<br/>

## Risultati principali

> *Basati su dati 2018-2024 per le cinque province sarde. L'aggiornamento con
> l'annata 2025 è previsto per l'autunno 2026, quello con l'annata 2026 per il primo
> trimestre del 2027.* 

### Ripresa della domanda

La Sardegna ha assorbito un crollo di circa il 56% degli arrivi nel 2020, è poi rimbalzata con forza nel 2021-2022 sino
a raggiungere nel 2024 **circa 4,44 milioni di arrivi**: ovvero il **25% sopra i livelli pre-pandemia del 2019**
(la sola provincia di Sassari vale 2,15 milioni di arrivi, quasi metà del totale regionale).

### Gap domanda-offerta

Tutte le province restano lontane dalla saturazione, perché la domanda si comprime nell'estate, ma la pressione
relativa è disomogenea.
**Nuoro** mostra il vincolo di offerta più stretto (**occupancy proxy 15,2%**, pari a 55 notti annuali vendute per 
posto letto), seguita da **Cagliari** (14,1%) e **Sud Sardegna** (13,5%).
**Oristano** chiude la classifica (11,8%): capacità disponibile ma domanda debole.

### Stagionalità

Il turismo è fortemente concentrato in estate.
I 3 mesi di punta valgono il **52-66% delle presenze annue** a seconda della provincia.
**Cagliari è la meno stagionale**: il suo mese di picco vale il 20% delle presenze annue, il potenziale più alto per strategie 
destagionalizzate.
L'indice di stagionalità sintetizza questa concentrazione su una scala che va da
0,08 (presenze distribuite uniformemente in tutti i mesi) a 1 (tutte le presenze in un
solo mese): 0,13 per Cagliari contro lo 0,19 circa di **Sud Sardegna** e **Nuoro**, le
più concentrate.

### Provenienza dei turisti

I turisti internazionali rappresentano una quota rilevante in ogni provincia, dal **41% (Sud Sardegna)** al 
**59,5% (Sassari)**.
**Sassari** e **Nuoro** attraggono la domanda internazionale più diversificata: un asset per un posizionamento premium.

### Segmenti in crescita più rapida

**Gli affitti brevi sono il motore di crescita dominante** in tutte le province. YoY 2023-2024:
- +38,7% **Sassari** 
- +32,5% **Nuoro**
- +31,1% **Sud Sardegna**

Gli hotel crescono più moderatamente (+3-13%), con quelli di **Oristano** in contrazione (-6,3%).

### Priorità di espansione

Il punteggio composito di priorità è la media di tre leve, ciascuna normalizzata su
scala 0-1 rispetto alle altre province: pressione sull'occupazione, crescita YoY e
quota internazionale. Un punteggio pari a 1 indicherebbe la provincia migliore su tutte
e tre le leve, un punteggio pari a 0 la peggiore su tutte. L'ordinamento risultante:

| Posizione | Provincia | Punteggio di priorità |
|-----------|-----------|-----------------------|
| 1 | Nuoro | 0,74 |
| 2 | Sassari | 0,72 |
| 3 | Cagliari | 0,58 |
| 4 | Sud Sardegna | 0,50 |
| 5 | Oristano | 0,06 |

Si può quindi osservare che:
- **Nuoro** guida per pressione sull'occupazione e quota internazionale
- **Sassari** si distingue per slancio di crescita e apertura internazionale
- **Oristano** risulta ultima o quasi su ogni leva, da cui il punteggio vicino allo zero

---

<br/>

## Le figure chiave

<br/>

![Punteggio di priorità di espansione per provincia](../reports/figures/it/fig_15_choropleth_priority_score.png)

<br/>

![Distribuzione mensile delle presenze per provincia](../reports/figures/it/fig_06_seasonality_heatmap.png)

<br/>

![Posizionamento delle province: occupazione vs crescita YoY](../reports/figures/it/fig_14_bubble_chart.png)

<br/>

Il percorso analitico completo, figura per figura, è disponibile nel [notebook EDA](../notebooks/01_eda_demand_supply.ipynb).

I risultati commentati, insieme alle raccomandazioni operative, sono invece disponibili nel [report esecutivo](../reports/it/REPORT.md).

---

<br/>

## Dashboard

E' anche disponibile una dashboard interattiva, costruita con **Looker Studio**, che offre una vista live e filtrabile di 
tutti gli indicatori chiave.

**[Apri la dashboard](https://lookerstudio.google.com/s/v2XX9XVY8Zk)**

#### Flusso dei dati: dalla pipeline alla dashboard

```text
DuckDB (database analitico)
  └── step_03_export.py
        ├── file CSV (locali, sempre)
        └── Google Sheets (opt-in, su richiesta esplicita)
              └── Looker Studio (connettore live, auto-refresh)
```

La pipeline esporta le tabelle analitiche in CSV per default. 

Quando `PUSH_TO_SHEETS=true` è impostato esplicitamente, gli stessi dati vengono anche inviati a Google Sheets, 
che Looker Studio legge come sorgente dati live.

E' necessario anche `GOOGLE_SHEETS_SPREADSHEET_ID`, valorizzato con l'ID dello spreadsheet di destinazione.

La credenziale del service account Google vive solo nel portachiavi di sistema e viene letta a runtime tramite la 
libreria `keyring`, che usa il credential store nativo di ogni sistema operativo (Keychain su macOS, Credential 
Manager su Windows, Secret Service su Linux): zero credenziali su disco o in variabili d'ambiente.

Si tenga comunque presente che il push è un'operazione opzionale, riservata a chi mantiene la dashboard: per
riprodurre l'analisi non è necessario alcun account Google.

---

<br/>

## Perimetro di analisi

- **Unità di analisi:** provincia (province sarde)
- **Dimensioni:** 
  - geografica (provincia)
  - tipo di struttura
  - provenienza (italiani/internazionali)
  - temporale (anno e mese)
- **KPI principali:**

| KPI | Formula | Interpretazione |
|-----|---------|-----------------|
| Occupancy Proxy | `nights / (beds × 365) × 100` | Tasso di occupazione dei posti letto (%): presenze per letto nell'anno |
| Gap domanda-offerta | `beds - arrivals` | Stima assoluta di sotto/sovra-offerta |
| Punteggio di priorità | `(occupancy_norm + yoy_norm + intl_share_norm) / 3` | Priorità composita di espansione a pesi uguali (0-1) |

---

<br/>

## Note di metodo e limiti dichiarati

- **Occupancy proxy e non tasso di occupazione reale.** La formula assume letti disponibili 365 giorni l'anno: 
sottostima quindi l'occupazione effettiva nei mesi di punta e va letta come misura di intensità d'uso, più utile al 
confronto tra province che in valore assoluto.


- **Granularità della capacità non uniforme.** Le annate 2018-2019 riportano la capacità ricettiva con dettaglio 
mensile, dal 2020 il dato è annuale: la serie dell'occupancy proxy copre quindi il 2020 e il 2022-2024 (il 2021 è privo 
del dettaglio provinciale nella fonte). Il punto è tracciato nel backlog tecnico.


- **Nomi provincia non uniformi tra annate.** I file di origine scrivono la stessa provincia in varianti diverse 
(prefissi descrittivi e perfino codifiche Unicode differenti per "Città metropolitana di Cagliari"). Il notebook li 
armonizza in modo generico (normalizzazione NFC, pulizia degli spazi, rimozione del prefisso) invece di enumerare le 
varianti, in modo che le annate future possano integrarsi con più facilità nella pipeline e nell'analisi.


- **Provenienza a granularità variabile.** Nelle annate 2023-2024 la macro-classificazione della provenienza non è 
presente nei file di origine: la ripartizione italiani/internazionali è ricostruita a partire dal dettaglio per paese.


- **Assetto amministrativo del periodo osservato.** Le cinque province analizzate riflettono la riforma del 2016 
(Sud Sardegna, città metropolitana di Cagliari). I confini usati per le mappe sono geodati pubblici versionati 
in `data_sample/geo/`.

---

<br/>

## Fonti dati

L'analisi usa due dataset open data di fonte ISTAT, pubblicati dal 
[portale open data dell'Osservatorio del Turismo della Regione Sardegna](https://osservatorio.sardegnaturismo.it/it/open-data):

| Fonte | Descrizione | Granularità |
|-------|-------------|-------------|
| **Movimento clienti** | Arrivi e presenze dei turisti negli esercizi ricettivi | Provincia × mese × anno × tipo × provenienza |
| **Capacità ricettiva** | Capacità degli esercizi (strutture, posti letto, camere) | Provincia × anno × tipo |

> **Privacy by design:** i dati sono già aggregati alla raccolta.
> Nessun dato personale (PII) viene trattato o memorizzato.

---

<br/>

## Stato del progetto

- [x] **Milestone 01**: analisi domanda-offerta end-to-end (pipeline ETL, notebook EDA
  con 16 figure EN/IT, README e report esecutivo EN/IT, test con coverage 93%, CI,
  dashboard interattiva Looker Studio)
- [ ] **Milestone 02**: aggiornamento dei dati (annata 2025 in autunno 2026, annata
  2026 nel primo trimestre 2027) e riallineamento della dashboard
- [ ] **Milestone 03**: analisi statistica e forecasting della domanda

---

<br/>

## Struttura del progetto

```text
project_root/
├── run_pipeline.py                    # Orchestratore ETL: da CSV a DuckDB a export
├── requirements.in                    # Dipendenze runtime (pip-tools)
├── requirements-test.in               # Runtime + pytest (usato dalla CI)
├── requirements-dev.in                # Set completo per lo sviluppo locale
├── requirements*.txt                  # Generati da pip-compile (versioni pinnate)
├── pyproject.toml                     # Config black, pytest, coverage
├── it/
│   └── README.md                      # Questa pagina (versione italiana)
├── src/
│   ├── config.py                      # Configurazione centralizzata (variabili d'ambiente)
│   ├── utils/                         # Utility condivise (logging, helper DB, runtime)
│   ├── sheets/                        # Push Google Sheets (auth keyring, gspread)
│   └── pipeline/
│       ├── step_01_ingest.py          # CSV ISTAT in tabelle raw DuckDB
│       ├── step_02_transform.py       # Views SQL e tabelle aggregate
│       └── step_03_export.py          # Da DuckDB a CSV + push opzionale a Google Sheets
├── sql/
│   ├── schema.sql                     # DDL: tabelle raw
│   ├── views/                         # Views analitiche (domanda, offerta, gap, stagionalità...)
│   └── queries/                       # Query materializzate (priority score, ranking...)
├── data/                              # Directory dati (gitignored)
│   ├── raw/                           # CSV ISTAT originali
│   ├── db/                            # File DuckDB
│   └── analysis/                      # CSV di output per il notebook
├── data_sample/                       # Dati di esempio schema-conformi (committati)
│   └── geo/                           # Dati geografici di riferimento: confini delle province sarde (GeoJSON)
├── notebooks/
│   └── 01_eda_demand_supply.ipynb     # Notebook di analisi esplorativa
├── reports/
│   ├── REPORT.md                      # Report esecutivo (EN)
│   ├── it/
│   │   └── REPORT.md                  # Report esecutivo (IT)
│   └── figures/                       # Figure EN generate dal notebook (in figures/it/ le versioni IT)
└── tests/
    ├── conftest.py                    # Fixture condivise e DuckDB in-memory
    ├── test_smoke.py                  # Smoke test leggeri (senza variabili d'ambiente)
    ├── test_pipeline.py               # Test unitari e di integrazione per pipeline e utility
    └── test_sql_views.py              # Views e query SQL testate su DuckDB con data_sample
```

---

<br/>

## Stack

| Componente | Tecnologia                                       |
|------------|--------------------------------------------------|
| Linguaggio | Python 3.13                                      |
| Database analitico | DuckDB                                           |
| Manipolazione dati | NumPy, Pandas                                    |
| Visualizzazione | Matplotlib, Seaborn                              |
| Notebook | Jupyter                                          |
| Visualizzazione geografica | GeoPandas                                        |
| Integrazione Google Sheets | gspread, keyring (credential store di sistema)   |
| Testing | pytest, pytest-cov                               |
| Dashboard | Looker Studio (connettore live su Google Sheets) |

---

<br/>

## Riproducibilità

L'intera analisi gira in locale: nessuna chiamata remota, nessuna credenziale richiesta. 
Prerequisiti: Git e Python 3.13+.

**1. Clonare il repository ed entrare nella cartella**

```bash
git clone https://github.com/aleattene/sardinia-hospitality-intelligence.git
cd sardinia-hospitality-intelligence
```

**2. Creare e attivare l'ambiente virtuale**: un'installazione Python isolata e dedicata al progetto, così le 
dipendenze non toccano il sistema.

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows (PowerShell):

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

**3. Installare le dipendenze**: versioni pinnate gestite con pip-tools
(`pip-compile` serve solo quando si modificano i file `.in`).

```bash
pip install pip-tools
pip-sync requirements-dev.txt
```

**4. Installare l'hook pre-commit**: attiva nbstripout, che ripulisce in
automatico gli output dei notebook a ogni commit.

```bash
pre-commit install
```

**5. Configurare l'ambiente**: i valori di default sono sufficienti.

macOS / Linux:

```bash
cp .env.example .env
```

Windows (PowerShell):

```powershell
Copy-Item .env.example .env
```

**6. Scaricare i dati**: i CSV del portale open data (link nella sezione Fonti dati) vanno collocati in `data/raw/`. 
Senza questi file la pipeline non parte: `data_sample/` serve ai test, non all'analisi.

**7. Eseguire pipeline, test e notebook**

```bash
python -m run_pipeline
pytest
jupyter notebook notebooks/01_eda_demand_supply.ipynb
```

L'esecuzione completa del notebook rigenera anche i grafici del report in
`reports/figures/` (EN) e `reports/figures/it/` (IT). Per farlo senza aprire
l'interfaccia Jupyter:

```bash
jupyter nbconvert --to notebook --execute notebooks/01_eda_demand_supply.ipynb --inplace
```

> Nota: la pipeline **non effettua chiamate remote**: elabora i CSV presenti in `data/raw/`, scaricati dal portale 
> open data dell'Osservatorio del Turismo della Regione Sardegna.

---

<br/>

### Autore:
[Alessandro Attene](https://www.linkedin.com/in/aleattene)

#### Licenza:
[MIT](../LICENSE)
