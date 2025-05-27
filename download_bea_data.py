"""Download BEA CAINC4 data for Economic Agency Index calculation"""
import requests
import zipfile
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def download_bea_data():
    """Download and extract BEA CAINC4 county income data"""

    # Create data directory
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)

    # BEA CAINC4 URL
    url = "https://apps.bea.gov/regional/zip/CAINC4.zip"
    zip_path = data_dir / "CAINC4.zip"

    # Download the ZIP file
    logger.info(f"Downloading BEA CAINC4 data from {url}")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        # Save ZIP file
        zip_path.write_bytes(response.content)
        logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.1f} MB")

        # Extract ZIP file
        logger.info("Extracting ZIP file...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)

        # Find the CSV file (it's usually named CAINC4.csv)
        csv_files = list(data_dir.glob("CAINC4*.csv"))
        if csv_files:
            logger.info(f"Found CSV file: {csv_files[0].name}")
            return csv_files[0]
        else:
            logger.error("No CAINC4 CSV file found in ZIP")
            return None

    except Exception as e:
        logger.error(f"Error downloading data: {e}")
        return None


def explore_cainc4_structure(csv_path):
    """Explore the structure of CAINC4 data"""
    logger.info(f"Exploring CAINC4 data structure...")

    # Read first few rows to understand structure
    df = pd.read_csv(csv_path, nrows=1000, encoding='latin-1')

    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Shape: {df.shape}")

    # Check for LineCode column
    if 'LineCode' in df.columns:
        line_codes = df['LineCode'].unique()
        logger.info(f"Found LineCode values: {sorted(line_codes)}")

        # Check for our needed line codes
        needed_codes = [50, 46, 47]  # Wages, Property, Transfers
        for code in needed_codes:
            if code in line_codes:
                logger.info(f"✓ Found LineCode {code}")
            else:
                logger.warning(f"✗ Missing LineCode {code}")

    return df


if __name__ == "__main__":
    csv_path = download_bea_data()
    if csv_path:
        explore_cainc4_structure(csv_path)