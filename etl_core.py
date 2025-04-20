"""Shared ETL utilities for Post‑Labor dashboards.
Requires: pandas, duckdb, pyarrow, requests, tqdm
"""
import duckdb, pandas as pd, requests, io, os, pathlib, datetime
from pathlib import Path
DATA_DIR = Path(__file__).parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

def fetch_csv(url: str, fname: str) -> Path:
    """Download a CSV if not cached; return local path."""
    dest = DATA_DIR / fname
    if not dest.exists():
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        dest.write_bytes(r.content)
    return dest

def standardise_fips(df, col='fips'):
    """Ensure 5‑digit zero‑padded county FIPS as str."""
    df[col] = df[col].astype(str).str.zfill(5)
    return df

def zscore(series):
    return (series - series.mean())/series.std()

# -- Add more helpers as the project grows --
