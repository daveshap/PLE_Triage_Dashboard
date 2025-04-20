import streamlit as st, duckdb, pandas as pd, pydeck as pdk, json, pathlib
st.set_page_config(page_title='Post‑Labor Triage Dashboard', layout='wide')
DB = pathlib.Path(__file__).parent / 'triage.duckdb'
if not DB.exists():
    st.error('Run build_triage.py first')
    st.stop()

con = duckdb.connect(DB.as_posix(), read_only=True)
df = con.table('triage').to_df()

st.title('Labor‑Collapse Triage (prototype)')
st.write('Showing demo data; replace with full ETL output.')

# Dummy map
INITIAL_VIEW = dict(latitude=37.8, longitude=-96, zoom=3)
df['dummy']=1
layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position="[ -100 + (dummy*5), 38 ]",
    get_radius=10000,
    get_fill_color=[255,0,0,140],
)
st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=INITIAL_VIEW))
