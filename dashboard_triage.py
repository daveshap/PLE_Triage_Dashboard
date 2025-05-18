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

# Database connection
DB = pathlib.Path(__file__).parent / 'triage.duckdb'
if not DB.exists():
    st.error('Database not found. Please run build_triage.py first to generate the required data.')
    st.stop()

try:
    con = duckdb.connect(DB.as_posix(), read_only=True)

    # Check what tables exist in the database
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [table[0] for table in tables]

    if 'triage' not in table_names:
        st.error(f"Table 'triage' not found in database. Available tables: {', '.join(table_names)}")
        st.stop()

    # Get the schema of the triage table - using PRAGMA which is more reliable
    schema_info = con.execute("PRAGMA table_info(triage)").fetchall()

    # Get the real data from triage.duckdb
    df = con.table('triage').to_df()

except Exception as e:
    st.error(f"Error connecting to database: {str(e)}")
    st.stop()

# Create header
st.markdown("""
<div class="title-container">
    <h1 class="title-text">Post-Labor Economics: Labor-Collapse Triage Dashboard</h1>
    <p class="subtitle-text">Identifying geographic areas experiencing significant labor market stress</p>
</div>
""", unsafe_allow_html=True)

# Data information section - hidden by default
with st.expander("Database Information", expanded=False):
    st.markdown("### Database Overview")
    st.write(f"Available tables: {table_names}")

    st.markdown("### Table Schema")
    # Create a properly formatted schema dataframe
    schema_df = pd.DataFrame(
        [(col[1], col[2], "PRIMARY KEY" if col[5] else "") for col in schema_info],
        columns=["Column", "Type", "Constraints"]
    )
    st.dataframe(schema_df)

    st.markdown("### Data Statistics")
    st.write(f"Total records: {len(df)}")
    st.write(f"Unique years: {sorted(df['year'].unique())}")
    st.write(f"Unique FIPS codes: {sorted(df['fips'].unique())}")

    st.markdown("### Raw Data Preview")
    st.dataframe(df)

# Check if we have sufficient data
if len(df) < 1:
    st.error("No data found in the database. Please run build_triage.py to generate data.")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("Dashboard Controls")

    # Year selection
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Select Year", options=available_years, index=len(available_years) - 1)

    # Determine stress level thresholds
    high_stress_threshold = 0.75
    medium_stress_threshold = 0.80

    # Allow user to adjust thresholds
    st.markdown("### Stress Level Thresholds")
    custom_high = st.slider("High Stress Threshold (E-POP below)", 0.60, 0.80, high_stress_threshold, 0.01)
    custom_medium = st.slider("Medium Stress Threshold (E-POP below)", custom_high, 0.90, medium_stress_threshold, 0.01)

    # Filter by stress level
    stress_options = ["High Stress", "Medium Stress", "Low Stress"]
    selected_stress = st.multiselect(
        "Filter by Stress Level",
        stress_options,
        default=stress_options
    )

    st.markdown("---")

    # About section
    st.markdown("""
    ### About This Dashboard

    This tool visualizes labor market stress indicators across US counties, 
    serving as an early warning system for areas experiencing disruption from 
    automation, economic shifts, or other factors.

    **Data Sources:** Bureau of Labor Statistics
    """)

    st.markdown("---")
    st.warning("Limited data detected. This is a demo dataset with only 2 counties.")
    st.info("Run a full ETL process with real data to see more counties.")

# Filter data for selected year
year_data = df[df['year'] == selected_year].copy()

# Notify if no data for selected year
if len(year_data) == 0:
    st.warning(f"No data available for the selected year {selected_year}.")
    st.stop()

# Add county information - hardcoded for the two counties we know exist
county_info = {
    '01001': {'name': 'Autauga County, AL', 'latitude': 32.5, 'longitude': -86.5},
    '01003': {'name': 'Baldwin County, AL', 'latitude': 30.7, 'longitude': -87.7}
}

# Add county information to the data
year_data['county_name'] = year_data['fips'].apply(lambda x: county_info.get(x, {}).get('name', f'County {x}'))
year_data['latitude'] = year_data['fips'].apply(lambda x: county_info.get(x, {}).get('latitude', 0))
year_data['longitude'] = year_data['fips'].apply(lambda x: county_info.get(x, {}).get('longitude', 0))


# Add stress level classification
def get_stress_level(epop, high_thresh, medium_thresh):
    if epop < high_thresh:
        return 'High'
    elif epop < medium_thresh:
        return 'Medium'
    else:
        return 'Low'


year_data['stress_level'] = year_data['prime_epop'].apply(
    lambda x: get_stress_level(x, custom_high, custom_medium)
)

# Filter by selected stress levels
stress_filter = []
if "High Stress" in selected_stress:
    stress_filter.append('High')
if "Medium Stress" in selected_stress:
    stress_filter.append('Medium')
if "Low Stress" in selected_stress:
    stress_filter.append('Low')

filtered_data = year_data[year_data['stress_level'].isin(stress_filter)]

# Main dashboard area - Key Metrics
col1, col2, col3 = st.columns(3)

# Calculate metrics
high_stress_counties = year_data[year_data['stress_level'] == 'High']
high_stress_avg = high_stress_counties['prime_epop'].mean() if not high_stress_counties.empty else 0
overall_avg = year_data['prime_epop'].mean()
counties_needing_attention = len(high_stress_counties)

# Display metrics
with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value high-stress">{high_stress_avg:.2f}</div>
        <div class="metric-label">Average Prime E-POP<br/>High-Risk Counties</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value medium-stress">{overall_avg:.2f}</div>
        <div class="metric-label">Overall Average<br/>Prime E-POP</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{counties_needing_attention}</div>
        <div class="metric-label">Counties Requiring<br/>Intervention</div>
    </div>
    """, unsafe_allow_html=True)

# Map Section
st.markdown('<div class="section-header">Geographic Distribution of Labor Market Stress</div>', unsafe_allow_html=True)

# Display data source warning
st.warning("""
This dashboard is showing a demo dataset with just two Alabama counties.
For a full view of all US counties, a complete ETL process needs to be implemented.
""")

# Prepare map data
map_data = filtered_data.copy()
map_data['radius'] = 35000  # Set a standard radius for visibility

# Set colors based on stress level
map_data['color'] = map_data['stress_level'].map({
    'High': [220, 38, 38, 160],  # Red with alpha
    'Medium': [245, 158, 11, 160],  # Orange with alpha
    'Low': [16, 185, 129, 160]  # Green with alpha
})

# Create map view focused on Alabama
view_state = pdk.ViewState(
    latitude=31.6,
    longitude=-87.1,
    zoom=6.5,
    pitch=0
)

# Create scatter plot layer
scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_data,
    get_position=["longitude", "latitude"],
    get_radius="radius",
    get_fill_color="color",
    pickable=True,
    opacity=0.8,
    stroked=True,
    filled=True,
)

# Create deck
deck = pdk.Deck(
    layers=[scatter_layer],
    initial_view_state=view_state,
    tooltip={"text": "{county_name}\nFIPS: {fips}\nPrime E-POP: {prime_epop}\nStress Level: {stress_level}"},
    map_style="light"
)

# Display map
st.pydeck_chart(deck)

# Map legend
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f'<span style="color:#DC2626">●</span> High Stress (E-POP < {custom_high})', unsafe_allow_html=True)
with col2:
    st.markdown(f'<span style="color:#F59E0B">●</span> Medium Stress (E-POP {custom_high}-{custom_medium})',
                unsafe_allow_html=True)
with col3:
    st.markdown(f'<span style="color:#10B981">●</span> Low Stress (E-POP > {custom_medium})', unsafe_allow_html=True)

# County Rankings section
st.markdown('<div class="section-header">Counties Requiring Attention</div>', unsafe_allow_html=True)

# Get high-stress counties
top_counties = high_stress_counties.sort_values('prime_epop').head(10)

if not top_counties.empty:
    # Display counties table
    st.markdown("**Counties with High Labor Market Stress**")

    # Format the table display
    display_df = top_counties[['county_name', 'fips', 'prime_epop', 'stress_level']].copy()
    display_df.columns = ['County', 'FIPS Code', 'Prime E-POP', 'Stress Level']

    st.dataframe(
        display_df,
        use_container_width=True
    )
else:
    st.info("No high-stress counties found with current thresholds.")

# Download section
st.markdown('<div class="section-header">Download Data</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)
with col1:
    st.download_button(
        label="Download Full Dataset (CSV)",
        data=filtered_data.to_csv(index=False),
        file_name=f"labor_triage_data_{selected_year}.csv",
        mime="text/csv"
    )

with col2:
    if not high_stress_counties.empty:
        st.download_button(
            label="Download High-Risk Counties Only (CSV)",
            data=high_stress_counties.to_csv(index=False),
            file_name=f"high_risk_counties_{selected_year}.csv",
            mime="text/csv"
        )
    else:
        st.button("Download High-Risk Counties Only (CSV)", disabled=True)

# Implementation Notes section
st.markdown('<div class="section-header">Implementation Notes</div>', unsafe_allow_html=True)
st.markdown("""
This dashboard currently displays a demo dataset with just two Alabama counties. To expand this to a full-scale monitoring tool:

1. **Complete ETL Implementation**: Implement the data extraction and transformation logic specified in triage_spec.yaml
2. **Add County Metadata**: The dashboard is ready to show county names and accurate geographic coordinates
3. **Time Series Analysis**: Add historical trend visualization for E-POP and weekly hours
4. **Performance Optimization**: For larger datasets, implement data clustering and aggregation at different zoom levels

The architecture is in place for a scalable solution when more comprehensive data becomes available.
""")

# Footer
st.markdown(f"""
<div class="footer">
    <p>Post-Labor Triage Dashboard | Last Updated: {datetime.now().strftime('%Y-%m-%d')} | Data Sources: BLS, US Census Bureau</p>
    <p>This dashboard is part of Post-Labor Economics research by David Shapiro</p>
</div>
""", unsafe_allow_html=True)