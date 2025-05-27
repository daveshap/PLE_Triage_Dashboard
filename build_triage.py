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
    df = pd.read_csv(csv_path, encoding='latin-1')
    logger.info(f"Loaded {len(df)} rows")

    # Filter for county data only (FIPS codes ending in 3 digits that aren't 000)
    # This excludes states, metros, and US total
    df['is_county'] = df['GeoFIPS'].str.match(r'^\d{5}$') & ~df['GeoFIPS'].str.endswith('000')
    county_data = df[df['is_county']].copy()

    logger.info(f"Found {len(county_data['GeoFIPS'].unique())} counties")

    # Get the three components we need
    wages = county_data[county_data['LineCode'] == 50].copy()
    property_income = county_data[county_data['LineCode'] == 46].copy()
    transfers = county_data[county_data['LineCode'] == 47].copy()

    logger.info(f"Wages data: {len(wages)} rows")
    logger.info(f"Property income data: {len(property_income)} rows")
    logger.info(f"Transfers data: {len(transfers)} rows")

    # Get the most recent year data (2023)
    year = '2023'

    # Reshape data for easier processing
    def get_component_data(df, component_name):
        result = df[['GeoFIPS', 'GeoName', year]].copy()
        result.columns = ['fips', 'county_name', component_name]
        result[component_name] = pd.to_numeric(result[component_name].str.replace('(NA)', '0'), errors='coerce')
        return result.set_index('fips')

    wages_2023 = get_component_data(wages, 'wages')
    property_2023 = get_component_data(property_income, 'property')
    transfers_2023 = get_component_data(transfers, 'transfers')

    # Combine all components
    eai_data = wages_2023.join(property_2023[['property']], how='outer').join(transfers_2023[['transfers']], how='outer')

    # Calculate Economic Agency Index
    # For now, let's use a simple ratio approach
    # Higher wages and property income = good (more agency)
    # Higher transfers = bad (less agency, more dependency)
    eai_data['total_income'] = eai_data['wages'] + eai_data['property'] + eai_data['transfers']

    # Calculate wage ratio (simplified EAI for MVP)
    # This represents the proportion of income from wages vs transfers
    eai_data['wage_ratio'] = eai_data['wages'] / eai_data['total_income']
    eai_data['wage_ratio'] = eai_data['wage_ratio'].fillna(0)

    # Reset index to have fips as a column
    eai_data = eai_data.reset_index()

    # Add year column
    eai_data['year'] = 2023

    # For MVP, use wage_ratio as our primary metric (similar to employment-population ratio)
    # We'll call it prime_epop to keep compatibility with existing dashboard
    eai_data['prime_epop'] = eai_data['wage_ratio']

    # Select final columns
    final_data = eai_data[['fips', 'year', 'prime_epop', 'county_name', 'wages', 'property', 'transfers']].copy()

    # Remove any rows with missing data
    final_data = final_data.dropna()

    logger.info(f"Processed {len(final_data)} counties with complete data")

    return final_data

def main():
    """Main ETL process"""
    logger.info("Starting Economic Agency Index ETL process...")

    # Load and process data
    data = load_and_process_cainc4()

    if data is None:
        logger.error("Failed to load data")
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