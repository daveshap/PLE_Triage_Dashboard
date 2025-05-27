import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
import numpy as np
import pathlib
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Post‑Labor Triage Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .title-container {
        background-color: #1E3A8A;
        padding: 1.5rem;
        border-radius: 5px;
        margin-bottom: 1.5rem;
        color: white;
    }
    .title-text {
        font-size: 2.2rem;
        font-weight: bold;
        margin: 0;
    }
    .subtitle-text {
        font-size: 1rem;
        opacity: 0.9;
        margin: 0.5rem 0 0 0;
    }
    .metric-card {
        background-color: white;
        border-radius: 5px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
    .high-stress {
        color: #DC2626;
    }
    .medium-stress {
        color: #F59E0B;
    }
    .low-stress {
        color: #10B981;
    }
    .footer {
        text-align: center;
        margin-top: 2rem;
        font-size: 0.8rem;
        color: #6B7280;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #E5E7EB;
    }
</style>
""", unsafe_allow_html=True)


def get_county_coordinates(fips):
    """Generate approximate coordinates for US counties based on FIPS codes"""
    if not fips or len(str(fips)) != 5:
        return 39.0, -98.0  # Center of US

    # Convert to string and get state/county codes
    fips_str = str(fips).zfill(5)
    state_code = int(fips_str[:2])
    county_code = int(fips_str[2:])

    # Approximate state center coordinates
    state_coords = {
        1: (32.7, -86.8),  # Alabama
        2: (64.0, -153.0),  # Alaska
        4: (34.2, -111.5),  # Arizona
        5: (35.2, -92.4),  # Arkansas
        6: (36.7, -119.7),  # California
        8: (39.0, -105.5),  # Colorado
        9: (41.6, -72.7),  # Connecticut
        10: (39.0, -75.5),  # Delaware
        11: (38.9, -77.0),  # DC
        12: (27.8, -81.7),  # Florida
        13: (32.9, -83.2),  # Georgia
        15: (21.1, -157.8),  # Hawaii
        16: (44.2, -114.5),  # Idaho
        17: (40.3, -89.0),  # Illinois
        18: (39.8, -86.1),  # Indiana
        19: (42.0, -93.2),  # Iowa
        20: (38.5, -96.7),  # Kansas
        21: (37.7, -84.9),  # Kentucky
        22: (31.1, -91.8),  # Louisiana
        23: (44.6, -69.8),  # Maine
        24: (39.0, -76.8),  # Maryland
        25: (42.2, -71.5),  # Massachusetts
        26: (43.3, -84.5),  # Michigan
        27: (45.7, -93.9),  # Minnesota
        28: (32.7, -89.7),  # Mississippi
        29: (38.4, -92.2),  # Missouri
        30: (47.0, -110.0),  # Montana
        31: (41.1, -98.0),  # Nebraska
        32: (38.4, -117.0),  # Nevada
        33: (43.4, -71.5),  # New Hampshire
        34: (40.3, -74.5),  # New Jersey
        35: (34.8, -106.2),  # New Mexico
        36: (42.1, -74.9),  # New York
        37: (35.6, -79.0),  # North Carolina
        38: (47.5, -99.8),  # North Dakota
        39: (40.3, -82.8),  # Ohio
        40: (35.6, -96.9),  # Oklahoma
        41: (44.5, -122.0),  # Oregon
        42: (40.5, -77.5),  # Pennsylvania
        44: (41.7, -71.5),  # Rhode Island
        45: (33.8, -80.9),  # South Carolina
        46: (44.2, -99.8),  # South Dakota
        47: (35.7, -86.0),  # Tennessee
        48: (31.0, -97.5),  # Texas
        49: (40.1, -111.9),  # Utah
        50: (44.0, -72.7),  # Vermont
        51: (37.7, -78.2),  # Virginia
        53: (47.3, -121.0),  # Washington
        54: (38.4, -80.9),  # West Virginia
        55: (44.3, -89.6),  # Wisconsin
        56: (42.7, -107.3),  # Wyoming
    }

    # Get base coordinates for the state
    base_lat, base_lng = state_coords.get(state_code, (39.0, -98.0))

    # Add variation based on county code to spread counties within state
    lat_offset = (county_code % 20 - 10) * 0.15
    lng_offset = ((county_code // 20) % 20 - 10) * 0.2

    return base_lat + lat_offset, base_lng + lng_offset


# Database connection
DB = pathlib.Path(__file__).parent / 'triage.duckdb'
if not DB.exists():
    st.error('Database not found. Please run build_triage.py first.')
    st.stop()

try:
    con = duckdb.connect(DB.as_posix(), read_only=True)
    df = con.table('triage').to_df()
    con.close()
except Exception as e:
    st.error(f"Error connecting to database: {str(e)}")
    st.stop()

# Add coordinates to all counties
df['latitude'] = df['fips'].apply(lambda x: get_county_coordinates(x)[0])
df['longitude'] = df['fips'].apply(lambda x: get_county_coordinates(x)[1])

# Create header
st.markdown(f"""
<div class="title-container">
    <h1 class="title-text">Post-Labor Economics: Economic Agency Dashboard</h1>
    <p class="subtitle-text">Wage dependency analysis across {len(df):,} US Counties</p>
</div>
""", unsafe_allow_html=True)

# Check if we have data
if len(df) < 10:
    st.error("Insufficient data. Please run build_triage.py to generate complete data.")
    st.stop()

# Sidebar controls
with st.sidebar:
    st.header("Controls")

    # Year selection
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Year", options=available_years, index=len(available_years) - 1)

    # Thresholds
    st.markdown("### Economic Agency Thresholds")
    st.caption("Wage ratio = wages / (wages + property + transfers)")

    high_threshold = st.slider("High Stress (wage ratio below)", 0.20, 0.50, 0.35, 0.01)
    medium_threshold = st.slider("Medium Stress (wage ratio below)", high_threshold, 0.70, 0.50, 0.01)

    # Filters
    stress_options = ["High Stress", "Medium Stress", "Low Stress"]
    selected_stress = st.multiselect("Show Stress Levels", stress_options, default=stress_options)

    st.markdown("---")
    st.markdown(f"""
    **Economic Agency Index**

    Higher wage ratios indicate more economic agency and self-sufficiency.

    **{len(df):,} counties** analyzed using BEA personal income data.
    """)

# Filter data
year_data = df[df['year'] == selected_year].copy()


# Add stress classification
def classify_stress(wage_ratio, high_thresh, medium_thresh):
    if wage_ratio < high_thresh:
        return 'High'
    elif wage_ratio < medium_thresh:
        return 'Medium'
    else:
        return 'Low'


year_data['stress_level'] = year_data['prime_epop'].apply(
    lambda x: classify_stress(x, high_threshold, medium_threshold)
)

# Apply filters
stress_filter = []
if "High Stress" in selected_stress:
    stress_filter.append('High')
if "Medium Stress" in selected_stress:
    stress_filter.append('Medium')
if "Low Stress" in selected_stress:
    stress_filter.append('Low')

filtered_data = year_data[year_data['stress_level'].isin(stress_filter)]

# Key metrics
col1, col2, col3 = st.columns(3)

high_stress_counties = year_data[year_data['stress_level'] == 'High']
overall_avg = year_data['prime_epop'].mean()
high_stress_avg = high_stress_counties['prime_epop'].mean() if len(high_stress_counties) > 0 else 0

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{len(high_stress_counties):,}</div>
        <div class="metric-label">High Stress<br/>Counties</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value medium-stress">{overall_avg:.3f}</div>
        <div class="metric-label">Average<br/>Wage Ratio</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    if len(high_stress_counties) > 0:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value high-stress">{high_stress_avg:.3f}</div>
            <div class="metric-label">High Stress<br/>Avg Ratio</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">—</div>
            <div class="metric-label">High Stress<br/>Avg Ratio</div>
        </div>
        """, unsafe_allow_html=True)

# Map
st.markdown('<div class="section-header">Economic Agency by County</div>', unsafe_allow_html=True)

if len(filtered_data) > 0:
    # Prepare map data
    map_data = filtered_data.copy()
    map_data['radius'] = 6000

    # Colors by stress level
    map_data['color'] = map_data['stress_level'].map({
        'High': [220, 38, 38, 160],  # Red
        'Medium': [245, 158, 11, 160],  # Orange
        'Low': [16, 185, 129, 160]  # Green
    })

    # Map view
    view_state = pdk.ViewState(
        latitude=39.0,
        longitude=-98.0,
        zoom=4,
        pitch=0
    )

    # Map layer
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_data,
        get_position=["longitude", "latitude"],
        get_radius="radius",
        get_fill_color="color",
        pickable=True,
        opacity=0.7,
        stroked=False,
        filled=True,
    )

    # Create deck
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "{county_name}\nWage Ratio: {prime_epop:.3f}\nStress: {stress_level}"},
        map_style="light"
    )

    st.pydeck_chart(deck)

    # Legend
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f'🔴 High Stress (< {high_threshold})', unsafe_allow_html=True)
    with col2:
        st.markdown(f'🟠 Medium Stress ({high_threshold}-{medium_threshold})', unsafe_allow_html=True)
    with col3:
        st.markdown(f'🟢 Low Stress (> {medium_threshold})', unsafe_allow_html=True)

    st.caption(f"Showing {len(filtered_data):,} of {len(year_data):,} counties")

else:
    st.warning("No counties match the current filters.")

# Top high-stress counties
if len(high_stress_counties) > 0:
    st.markdown('<div class="section-header">Counties Requiring Attention</div>', unsafe_allow_html=True)

    top_counties = high_stress_counties.nsmallest(10, 'prime_epop')

    display_df = top_counties[['county_name', 'fips', 'prime_epop']].copy()
    display_df.columns = ['County', 'FIPS', 'Wage Ratio']
    display_df['Wage Ratio'] = display_df['Wage Ratio'].apply(lambda x: f"{x:.3f}")

    st.dataframe(display_df, use_container_width=True, hide_index=True)

# Export
st.markdown('<div class="section-header">Export Data</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

with col1:
    csv_data = filtered_data[['fips', 'county_name', 'prime_epop', 'stress_level']].to_csv(index=False)
    st.download_button(
        label=f"Download Filtered Data ({len(filtered_data):,} counties)",
        data=csv_data,
        file_name=f"economic_agency_index_{selected_year}.csv",
        mime="text/csv"
    )

with col2:
    if len(high_stress_counties) > 0:
        high_stress_csv = high_stress_counties[['fips', 'county_name', 'prime_epop']].to_csv(index=False)
        st.download_button(
            label=f"Download High-Risk Counties ({len(high_stress_counties):,})",
            data=high_stress_csv,
            file_name=f"high_risk_counties_{selected_year}.csv",
            mime="text/csv"
        )

# Footer
st.markdown(f"""
<div class="footer">
    <p>Post-Labor Economics Dashboard | Economic Agency Index | {len(df):,} US Counties</p>
    <p>Data: Bureau of Economic Analysis CAINC4 | Updated: {datetime.now().strftime('%Y-%m-%d')}</p>
</div>
""", unsafe_allow_html=True)