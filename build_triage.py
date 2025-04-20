"""Driver script: builds triage.duckdb & triage.parquet
Usage: python build_triage.py
"""
import duckdb, pandas as pd, yaml, json, etl_core, datetime, pathlib
from pathlib import Path
SPEC = yaml.safe_load(Path('triage_spec.yaml').read_text())

con = duckdb.connect('triage.duckdb')
con.execute('PRAGMA memory_limit="1GB"')

# Placeholder demo table
df_demo = pd.DataFrame({'fips':['01001','01003'], 'year':[2024,2024], 'prime_epop':[0.79,0.82]})
con.register('demo', df_demo)
con.execute('CREATE OR REPLACE TABLE triage AS SELECT * FROM demo')
con.execute("COPY (SELECT * FROM triage) TO 'triage.parquet' (FORMAT 'parquet')")
print('Wrote triage.duckdb & triage.parquet')
