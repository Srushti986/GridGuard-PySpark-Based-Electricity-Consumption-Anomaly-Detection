# ⚡ GridGuard
**Electricity Consumption Anomaly Detection and Visualization System Using PySpark**

## 1. Abstract
GridGuard is a project that uses PySpark to analyse household electricity consumption and identify unusual changes in usage. It processes electricity readings, cleans invalid data, studies consumption patterns, and detects readings that are significantly different from a household's previous usage.

The project uses Apache Spark for data processing, Hadoop HDFS for storage, and Streamlit with Plotly to display the results through an interactive dashboard.

It also provides simple explanations for detected anomalies, making it easier to understand why a particular reading was flagged. The system can run using local storage or HDFS, depending on the available setup.

## 2. Problem statement
Consumption data is large and noisy, with gaps and invalid readings. Manually spotting unusual usage is impractical. A reproducible, explainable pipeline is needed that handles bad data safely and avoids look-ahead leakage.

## 3. Objectives
Detect unusually high/low consumption · identify hourly/daily patterns · find recurring peaks · compare readings with historical baselines · explain each flag · visualise results · demonstrate HDFS storage + Spark processing.

## 4. Technologies
Python · Apache Spark/PySpark (DataFrames, Spark SQL) · Hadoop HDFS (Docker Compose, `apache/hadoop`) · Streamlit · Plotly · NumPy/pandas (small dashboard-level tasks and the sample generator) · Jupyter/Colab. 

## 5. Dataset
UCI *Individual Household Electric Power Consumption* — https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption (one household, 1-minute readings, Dec 2006–Nov 2010, `?` marks missing values). Field meanings and units: see [`data/README.md`](data/README.md). Primary metric: `Global_active_power` (kW). The dataset has **no meter IDs and no anomaly labels**; none are invented, so detection accuracy cannot be measured.

`data/sample_electricity.csv` is a **synthetic**, 10-day test file with deliberately injected problems — never treat its results as real.

## 6. Architecture
```
 UCI CSV ──► HDFS /gridguard/raw ──► PySpark clean ──► hourly + daily aggregation
                                                   └─► rolling z-score detection ──► peak analysis
 HDFS /gridguard/processed + /results  ──►  Streamlit dashboard (reads results only)
 (or local outputs/ in standalone mode)
```
Code layout: `src/` holds all logic; `app.py` only displays pre-computed tables; `run_pipeline.py` orchestrates.

## 7. Processing workflow
1. Read raw file as strings (explicit schema). 2. Parse `d/M/yyyy HH:mm:ss` timestamps (UTC session zone → no DST artefacts). 3. `?`, text, NaN, negative or implausible values → `NULL` (never 0, never imputed). 4. Drop unparseable timestamps and duplicates. 5. Hourly mean of valid minutes; hours with < 30 valid minutes become `NULL`. 6. Baseline + z-score. 7. Daily table, peak tables, run summary. 8. Write + verify.

## 8. Anomaly detection method
For hour *h* of date *d*, the baseline uses the **same hour-of-day on the previous 14 days (d-14 … d-1)** — a Spark `RANGE BETWEEN 14 PRECEDING AND 1 PRECEDING` window. The evaluated reading is never part of its own baseline (no look-ahead leakage), and gaps don't stretch the window.

```
z = (observed_kw − baseline_mean) / baseline_std        (sample std, n−1)
flag if |z| ≥ 3        (configurable)
```
| Situation | Status | Result |
|---|---|---|
| observation missing/negative/invalid | `missing_observation` | not scored |
| fewer than 5 prior points | `insufficient_history` | not scored |
| std null or ≈ 0 (constant history) | `zero_variance` | not scored (z undefined) |
| otherwise | `ok` | scored |

Severity (relative to threshold): Moderate (+0…1), Strong (+1…2), Extreme (≥ +2). Output fields: timestamp, observed kW, historical mean, historical std, z-score, direction (High/Low), severity, explanation. A flag is **a statistical deviation only** — it does not prove theft, equipment failure or a fault.

## 9. HDFS layout
```
/gridguard/raw/                                   original CSV
/gridguard/processed/hourly_consumption/          hourly features + baseline + z-score for every hour
/gridguard/processed/daily_consumption/           (addition) daily summary
/gridguard/processed/pipeline_summary/            (addition) one-row run metadata incl. the storage mode actually used
/gridguard/results/detected_anomalies/            flagged hours only
/gridguard/results/peak_analysis/top_periods/
/gridguard/results/peak_analysis/peak_hour_frequency/
```
Local standalone mode mirrors the same layout under `outputs/` (and is labelled "Local filesystem – NOT HDFS" everywhere). Setup: [`hadoop/README.md`](hadoop/README.md).

## 10. Installation
Requirements: Python 3.9–3.12, **Java 11 or 17**, ~4 GB RAM.
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                   # optional; edit settings
```

## 11. Run locally
```bash
python data_download.py            # download + extract the real dataset into data/
python run_pipeline.py             # standalone mode (local outputs/)
python run_pipeline.py --sample    # quick test with the synthetic file
streamlit run app.py               # dashboard at http://localhost:8501
python -m pytest tests/            # unit tests (Spark tests need PySpark + Java)
```
With HDFS (after `hadoop/README.md`): `export GRIDGUARD_HDFS_ENABLED=true && python run_pipeline.py --hdfs`. If the NameNode is unreachable the pipeline **falls back to local storage and prints why**.
Useful flags: `--threshold 2.5`, `--baseline-days 21`, `--min-history 7`, `--output-dir PATH`, `--local`.

Settings (env vars / `.env`): see [`.env.example`](.env.example).

## 12. Google Colab
Open `GridGuard_Colab.ipynb` in Colab → Runtime ▸ Run all. Default is **Mode B (standalone)**; Mode A needs a NameNode reachable from Google's cloud. Step-by-step: [`COLAB_SETUP.md`](COLAB_SETUP.md). Download the exported ZIP from the last cells and unzip it into `outputs/` to view results in the dashboard.

## 13. Dashboard
Pages: **Overview** (records, average, maximum, anomalies, share, daily trend) · **Consumption Analytics** (date filter, hourly/daily, time-of-day, weekly, heatmap) · **Anomaly Investigation** (filters, sortable table, baseline-vs-observed chart, explanation) · **Peak Demand** · **Data and Storage** (active backend, schema, row counts, HDFS paths only when genuinely connected). The sidebar shows processing and storage status. If outputs are missing, the app says processing is not complete.

Screenshots (add your own after running):
| Overview | Anomaly Investigation |
|---|---|
| `docs/screenshots/overview.png` *(placeholder)* | `docs/screenshots/anomalies.png` *(placeholder)* |
| Peak Demand | Data and Storage |
| `docs/screenshots/peaks.png` *(placeholder)* | `docs/screenshots/storage.png` *(placeholder)* |

## 14. Output files
Each table is a folder containing a headered CSV part file and `_SUCCESS`. `hourly_consumption` (all hours, scored), `daily_consumption`, `detected_anomalies` (flagged, strongest first), `top_periods`, `peak_hour_frequency`, `pipeline_summary` (record counts, settings, `storage_mode`, `copy_location`, timestamp).

## 15. Limitations
Single household/metric; no ground truth (no precision/recall); z-scores assume a fairly stable, roughly symmetric same-hour history — real consumption is skewed and seasonal; weekday/weekend split is optional (`GRIDGUARD_BASELINE_BY_DAY_TYPE`) and needs longer baselines; constant baselines are "not evaluated"; ~2 M rows is modest for "big data" (Spark runs in local mode); the Docker HDFS is single-node, without authentication; HDFS and Spark paths untested by the generator of this repo.

## 16. Future scope
Multi-meter data and partitioned Parquet; robust statistics (median/MAD, seasonal decomposition); weekday-aware and holiday-aware baselines; streaming ingestion; alerting; authentication/Kerberos for HDFS; a Spark cluster instead of local mode.

## 17. Conclusion
GridGuard shows an end-to-end, explainable big-data workflow - distributed storage, Spark processing, leakage-free statistical detection, and an interactive dashboard - with explicit, honest handling of storage mode and data quality.
