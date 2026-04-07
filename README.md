# Sardinia Hospitality Intelligence

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

An end-to-end **Data Analysis** project that maps the tourism demand and accommodation supply
across Sardinian provinces using ISTAT open data — identifying geographic and seasonal gaps
to support data-driven expansion decisions in the hospitality sector.

---

## Business Questions

This analysis addresses five key questions for hospitality operators and investors in Sardinia:

1. **Where is the supply-demand gap largest?** Which provinces show the highest imbalance between tourist arrivals and accommodation capacity?
2. **What is the seasonality profile?** How does demand distribute across months, and which provinces are least seasonal?
3. **Who are the tourists?** How do Italian and international visitors differ by province and accommodation type?
4. **Which segments are growing fastest?** Which accommodation types and tourist origins show the strongest year-over-year growth?
5. **Where should operators expand first?** Which provinces score highest on a composite expansion priority index?

---

## Key Findings

> Based on ISTAT data 2018–2024 across five Sardinian provinces.

### Demand recovery
Sardinia absorbed a ~60% collapse in arrivals during 2020, rebounded strongly through 2021–2022, and by 2024 reached **~4.44M arrivals** — roughly **25% above 2019 pre-pandemic levels** (+2.15M in Sassari alone).

### Supply-demand gap
All provinces remain below full occupancy, but pressure is uneven.
Nuoro shows the tightest supply constraint (**55.4% occupancy proxy**), followed by Cagliari (51.3%) and Sud Sardegna (49.4%).
Oristano sits furthest from saturation (43.0%) — indicating available capacity but weak demand pull.

### Seasonality
Tourism is strongly concentrated in summer.
The top 3 months account for **52–66% of annual overnight stays** depending on province.
**Cagliari is the least seasonal** (peak-month share: 20%, index: 0.13) — highest potential for year-round strategies.
Sud Sardegna and Nuoro are the most concentrated (index ≈ 0.19).

### Tourist origin
International tourists represent a significant share everywhere, ranging from **41% (Sud Sardegna)** to **59.5% (Sassari)**.
Sassari and Nuoro attract the most internationally diverse demand — an asset for premium positioning.

### Fastest-growing segments
**Short-term rentals are the dominant growth engine** across all provinces (YoY 2023→2024: +38.7% Sassari, +32.5% Nuoro, +31.1% Sud Sardegna).
Hotels grew more modestly (+3–13%), with Oristano hotels contracting (–6.3%).

### Expansion targeting
The composite priority score (occupancy + YoY growth + international share) ranks provinces as:

| Rank | Province | Priority Score |
|------|----------|---------------|
| 1 | Nuoro | 0.74 |
| 2 | Sassari | 0.72 |
| 3 | Cagliari | 0.58 |
| 4 | Sud Sardegna | 0.50 |
| 5 | Oristano | 0.06 |

**Nuoro** leads on occupancy pressure and international share; **Sassari** on growth momentum and international openness.

---

## Analysis Scope

- **Unit of analysis:** province (Sardinian provinces)
- **Dimensions:** geographic (province), accommodation type, tourist origin (Italian / international), temporal (year + month)
- **Core KPIs:**

| KPI | Formula | Interpretation |
|-----|---------|----------------|
| Occupancy Proxy | `nights / (beds × 365) × 100` | Bed occupancy rate (%) — nights used per bed over the year |
| Supply-Demand Gap | `beds - arrivals` | Absolute under/over-supply estimate |
| Priority Score | `(occupancy_norm + yoy_norm + intl_share_norm) / 3` | Equal-weight composite expansion priority (0–1) |

---

## Expected Outputs

| Output | Description |
|--------|-------------|
| Supply-demand gap ranking | By province, with occupancy proxy (%), arrivals, nights, and beds |
| Expansion priority ranking | Provinces scored on 3 equal-weight components: occupancy pressure, YoY growth, international share |
| Seasonality profile | Monthly demand distribution per province, Herfindahl-style concentration index |
| Tourist origin segmentation | Italian vs international breakdown by province |
| Year-over-year growth | Top growing segments by accommodation type and province |
| Geographic visualization | Choropleth map of coverage across Sardinian provinces |
| Interactive dashboard | Looker Studio Dashboard *(coming soon)* |

---

## Data Sources

The analysis uses two ISTAT open data sources:

| Source | Description | Granularity |
|--------|-------------|-------------|
| **Movimento clienti** | Tourist arrivals and nights spent in accommodation facilities | Province × month × year × type × origin |
| **Capacità ricettiva** | Accommodation capacity (facilities, beds, rooms) | Province × year × type |

> **Privacy by design:** ISTAT data is already aggregated at collection time.
> No Personally Identifiable Information (PII) is processed or stored.

---

## Project Structure

```text
project_root/
├── run_pipeline.py                    # ETL orchestrator: CSV → DuckDB → export
├── requirements.in                    # Top-level dependencies
├── requirements.txt                   # Pinned, generated by pip-compile
├── pyproject.toml                     # black, pytest, coverage config
├── src/
│   ├── config.py                      # Centralized configuration (env vars)
│   ├── utils/                         # Shared utilities (logging, DB helpers, runtime)
│   └── pipeline/
│       ├── step_01_ingest.py          # ISTAT CSV → raw tables in DuckDB
│       ├── step_02_transform.py       # SQL views and aggregate tables
│       └── step_03_export.py          # DuckDB → CSV for notebook and dashboard
├── sql/
│   ├── schema.sql                     # DDL: raw tables
│   ├── views/                         # Analytical views (demand, supply, gap, seasonality…)
│   └── queries/                       # Materialized queries (priority score, rankings…)
├── data/                              # Git-ignored data directory
│   ├── raw/                           # Original ISTAT CSV files
│   ├── db/                            # DuckDB file
│   └── analysis/                      # CSV output for notebook
├── data_sample/                       # Schema-conformant sample data (committed)
├── notebooks/
│   └── 01_eda_demand_supply.ipynb     # Exploratory analysis notebook
├── reports/
│   ├── REPORT.md                      # Executive report
│   └── figures/                       # Charts generated by notebook
└── tests/
    ├── test_pipeline.py
    └── test_sql_views.py              # SQL queries tested on DuckDB with data_sample
```

---

## Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.13 |
| Analytical DB | DuckDB |
| Data manipulation | Pandas, NumPy |
| Visualization | Matplotlib, Seaborn |
| Notebook | Jupyter |
| Geographic visualization | GeoPandas |
| Dashboard | Looker Studio |

---

## Reproducibility

```bash
# 1. Install dependencies
pip install pip-tools
pip-compile requirements.in
pip-sync requirements.txt

# 1b. Install pre-commit hooks (strips notebook outputs before each commit)
pre-commit install

# 2. Configure environment
cp .env.example .env
# No values required to run the default pipeline (processes local data only).
# Set FETCH_ISTAT_DATA=true only when downloading fresh data from ISTAT.

# 3. Run the pipeline (processes existing local data — no remote calls)
python -m run_pipeline

# 3b. Run with fresh ISTAT data download
FETCH_ISTAT_DATA=true python -m run_pipeline

# 4. Run the EDA notebook
jupyter notebook notebooks/01_eda_demand_supply.ipynb
```

> By default the pipeline performs **no remote calls** — it processes existing raw data.
> Set `FETCH_ISTAT_DATA=true` to trigger a fresh download from ISTAT.

---

## Report

> Coming soon — will be published upon completion of the EDA notebook.

- [Executive Report](reports/REPORT.md)
- [EDA Notebook](notebooks/01_eda_demand_supply.ipynb)

---

## Author

Alessandro Attene
