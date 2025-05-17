# PLE Triage Dashboard

A visualization tool for **Post‑Labor Economics – Labor‑Collapse Triage** that identifies geographic areas experiencing significant labor market stress.

## Purpose
This dashboard maps and analyzes employment data across US counties to detect regions experiencing labor market disruption or decline. It serves as an early warning system to identify where economic interventions might be needed, regardless of whether the causes are technological automation, economic restructuring, or other factors.

## Quick‑start
```bash
pip install -r requirements.txt
python build_triage.py      # builds triage.duckdb & triage.parquet
streamlit run dashboard_triage.py
```

## Key Metrics
* Prime-age Employment-Population Ratio
* Average Weekly Hours

## Stack
* Python 3.11, pandas, duckdb
* Streamlit + PyDeck for local / Netlify‑deployable front‑end
* GitHub Actions nightly ETL