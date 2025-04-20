# PLE_Triage_Dashboard

Prototype dashboard for **Post‑Labor Economics – Labor‑Collapse Triage**.

## Quick‑start
```bash
pip install -r requirements.txt
python build_triage.py      # builds triage.duckdb & triage.parquet
streamlit run dashboard_triage.py
```

## Stack
* Python 3.11, pandas, duckdb
* Streamlit + PyDeck for local / Netlify‑deployable front‑end
* GitHub Actions nightly ETL
