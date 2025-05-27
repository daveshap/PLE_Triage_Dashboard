"""Build triage.duckdb with Economic Agency Index data from BEA CAINC4"""
import duckdb
import pandas as pd
import yaml
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def load_and_process_cainc4():
    """Load CAINC4 data and calculate Economic Agency Index"""

    # Path to the all areas CSV file
    csv_path = Path('data/CAINC4__ALL_AREAS_1969_2023.csv')

    if not csv_path.exists():
        logger.error(f"Data file not found: {csv_path}")
        logger.error("Please run download_bea_data.py first!")
        return None

    logger.info("Loading CAINC4 data...")

    # Read the CSV file
    df = pd.read_csv(csv_path, encoding='latin-1', low_memory=False)
    logger.info(f"Loaded {len(df)} rows")

    # Clean up GeoFIPS - remove quotes and spaces
    df['GeoFIPS_clean'] = df['GeoFIPS'].str.strip().str.strip('"')

    # Filter for county data only (5-digit FIPS codes that don't end in 000)
    df['is_county'] = (df['GeoFIPS_clean'].str.match(r'^\d{5}$') &
                       ~df['GeoFIPS_clean'].str.endswith('000'))
    county_data = df[df['is_county']].copy()

    logger.info(f"Found {len(county_data['GeoFIPS_clean'].unique())} counties")

    # Get the three components we need
    wages = county_data[county_data['LineCode'] == 50].copy()
    property_income = county_data[county_data['LineCode'] == 46].copy()
    transfers = county_data[county_data['LineCode'] == 47].copy()

    logger.info(f"Wages data: {len(wages)} rows")
    logger.info(f"Property income data: {len(property_income)} rows")
    logger.info(f"Transfers data: {len(transfers)} rows")

    # Get the most recent year data (2023)
    year_col = '2023'

    # Reshape data for easier processing
    def get_component_data(df, component_name):
        result = df[['GeoFIPS_clean', 'GeoName', year_col]].copy()
        result.columns = ['fips', 'county_name', component_name]
        # Convert to numeric, handling (NA) and other text values
        result[component_name] = pd.to_numeric(
            result[component_name].astype(str).str.replace('(NA)', '0').str.replace('(D)', '0'),
            errors='coerce'
        )
        result[component_name] = result[component_name].fillna(0)
        return result.set_index('fips')

    wages_2023 = get_component_data(wages, 'wages')
    property_2023 = get_component_data(property_income, 'property')
    transfers_2023 = get_component_data(transfers, 'transfers')

    logger.info(f"Sample wages data: {wages_2023.head()}")

    # Combine all components
    eai_data = wages_2023.join(property_2023[['property']], how='outer').join(transfers_2023[['transfers']], how='outer')

    # Fill any remaining NaN values with 0
    eai_data = eai_data.fillna(0)

    # Calculate Economic Agency Index components
    eai_data['total_income'] = eai_data['wages'] + eai_data['property'] + eai_data['transfers']

    # Calculate wage ratio (simplified EAI for MVP)
    # This represents economic agency - higher wage ratio = more agency
    eai_data['wage_ratio'] = eai_data['wages'] / eai_data['total_income']
    eai_data['wage_ratio'] = eai_data['wage_ratio'].fillna(0)

    # Also calculate property ratio for future use
    eai_data['property_ratio'] = eai_data['property'] / eai_data['total_income']
    eai_data['property_ratio'] = eai_data['property_ratio'].fillna(0)

    # Reset index to have fips as a column
    eai_data = eai_data.reset_index()

    # Add year column
    eai_data['year'] = 2023

    # Use wage_ratio as our primary metric (compatible with existing dashboard)
    eai_data['prime_epop'] = eai_data['wage_ratio']

    # Select final columns and remove invalid/missing data
    final_data = eai_data[['fips', 'year', 'prime_epop', 'county_name', 'wages', 'property', 'transfers']].copy()

    # Remove rows where total income is 0 or very small
    final_data = final_data[final_data['wages'] + final_data['property'] + final_data['transfers'] > 1000]

    # Remove any remaining NaN values
    final_data = final_data.dropna()

    logger.info(f"Processed {len(final_data)} counties with complete data")
    logger.info(f"Sample data:\n{final_data.head()}")

    return final_data

def main():
    """Main ETL process"""
    logger.info("Starting Economic Agency Index ETL process...")

    # Load and process data
    data = load_and_process_cainc4()

    if data is None or len(data) == 0:
        logger.error("Failed to load data or no valid counties found")
        return

    # Create DuckDB database
    con = duckdb.connect('triage.duckdb')
    con.execute('PRAGMA memory_limit="1GB"')

    # Create table with the data
    con.register('eai_data', data)
    con.execute('''
        CREATE OR REPLACE TABLE triage AS 
        SELECT 
            fips,
            year,
            prime_epop,
            county_name,
            wages,
            property,
            transfers
        FROM eai_data
    ''')

    # Also save as parquet
    con.execute("COPY (SELECT * FROM triage) TO 'triage.parquet' (FORMAT 'parquet')")

    # Show summary statistics
    stats = con.execute('''
        SELECT 
            COUNT(*) as total_counties,
            AVG(prime_epop) as avg_wage_ratio,
            MIN(prime_epop) as min_wage_ratio,
            MAX(prime_epop) as max_wage_ratio
        FROM triage
    ''').fetchone()

    logger.info(f"Database created successfully!")
    logger.info(f"Total counties: {stats[0]}")
    logger.info(f"Average wage ratio: {stats[1]:.3f}")
    logger.info(f"Min wage ratio: {stats[2]:.3f}")
    logger.info(f"Max wage ratio: {stats[3]:.3f}")

    con.close()
    logger.info("Wrote triage.duckdb & triage.parquet")

if __name__ == "__main__":
    main()