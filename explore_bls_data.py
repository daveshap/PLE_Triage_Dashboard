"""BLS Data Explorer for Post-Labor Triage Dashboard
Explores the structure of BLS data files to assist in ETL development
"""
import requests
import pandas as pd
import yaml
import os
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_and_explore_bls_file(url, local_filename):
    """Download and explore a BLS data file to understand its structure"""
    logger.info(f"Exploring BLS data from {url}")

    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    file_path = data_dir / local_filename

    # Download the file if it doesn't exist
    if not file_path.exists():
        logger.info(f"Downloading file to {file_path}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        file_path.write_bytes(r.content)

    # First, try to determine the file format
    with open(file_path, 'r') as f:
        sample = f.read(2000)  # Read first 2000 chars

    # Check delimiter without using backslash in f-string expression
    tab_char = '\t'  # Define tab character outside the f-string
    delimiter = tab_char if tab_char in sample else ','
    logger.info(f"Detected delimiter: {'tab' if delimiter == tab_char else 'comma'}")

    # Try to read the file
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, nrows=1000, low_memory=False)

        # Basic file information
        logger.info(f"File successfully loaded. Preview of structure:")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Sample rows: {len(df)}")

        # Check for key columns
        for col in ['series_id', 'year', 'value', 'period']:
            logger.info(f"Column '{col}' exists: {col in df.columns}")

        # If series_id exists, analyze its structure
        if 'series_id' in df.columns:
            series_samples = df['series_id'].unique()[:10]
            logger.info(f"Sample series IDs: {series_samples}")

            # Try to find patterns in series IDs
            logger.info("Analyzing series ID patterns...")
            has_lau = any(s.startswith('LAU') for s in df['series_id'].dropna().astype(str))
            if has_lau:
                # Local Area Unemployment Statistics
                logger.info("Found LAU series (Local Area Unemployment)")

                # Try to extract county identifiers
                has_fips = df['series_id'].astype(str).str.contains(r'\d{5}').any()
                if has_fips:
                    logger.info("Series contains 5-digit codes (likely FIPS)")

            has_ces = any(s.startswith('CES') for s in df['series_id'].dropna().astype(str))
            if has_ces:
                # Current Employment Statistics
                logger.info("Found CES series (Current Employment Statistics)")

        # Check for date/period information
        if 'year' in df.columns:
            years = df['year'].unique()
            logger.info(f"Years in data: {sorted(years)}")

        if 'period' in df.columns:
            periods = df['period'].unique()
            logger.info(f"Periods in data: {sorted(periods)}")

        # Sample data rows
        logger.info("\nSample data rows:")
        print(df.head(5))

        return df

    except Exception as e:
        logger.error(f"Error exploring file: {str(e)}")
        return None

def main():
    """Main exploration process"""
    try:
        # Load the ETL specification
        spec = yaml.safe_load(Path('triage_spec.yaml').read_text())

        # Explore each KPI data source
        for kpi in spec['kpis']:
            logger.info(f"\n{'='*50}")
            logger.info(f"Exploring data for KPI: {kpi['name']}")
            logger.info(f"{'='*50}")

            df = fetch_and_explore_bls_file(kpi['source_url'], kpi['local_csv'])

            logger.info(f"\nCompleted exploration for {kpi['name']}")
            logger.info(f"{'='*50}\n")

    except Exception as e:
        logger.error(f"Exploration failed: {str(e)}")

if __name__ == "__main__":
    main()