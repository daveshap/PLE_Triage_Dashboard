"""Download official US county coordinates from Census Bureau Gazetteer Files"""
import requests
import zipfile
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def download_county_coordinates():
    """Download and process Census Bureau county coordinates"""

    # Create data directory
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)

    # Census Bureau Gazetteer file URL (2024 version)
    url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip"
    zip_path = data_dir / "county_coordinates.zip"
    csv_path = data_dir / "county_coordinates.csv"

    # Check if we already have the processed file
    if csv_path.exists():
        logger.info(f"County coordinates file already exists: {csv_path}")
        return csv_path

    try:
        logger.info(f"Downloading county coordinates from Census Bureau...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        # Save ZIP file
        zip_path.write_bytes(response.content)
        logger.info(f"Downloaded {len(response.content) / 1024:.1f} KB")

        # Extract ZIP file
        logger.info("Extracting coordinates file...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find the text file inside
            txt_files = [f for f in zip_ref.namelist() if f.endswith('.txt')]
            if not txt_files:
                raise ValueError("No text file found in ZIP")

            # Extract the main file
            txt_file = txt_files[0]
            with zip_ref.open(txt_file) as f:
                content = f.read().decode('utf-8')

        # Parse the tab-delimited file
        logger.info("Processing coordinate data...")
        lines = content.strip().split('\n')

        # Parse header
        header = lines[0].split('\t')
        logger.info(f"Columns: {header}")

        # Parse data rows
        data = []
        for line in lines[1:]:
            row = line.split('\t')
            if len(row) == len(header):
                data.append(row)

        # Create DataFrame
        df = pd.DataFrame(data, columns=header)

        # Clean and standardize the data
        # GEOID is the 5-digit FIPS code we need
        df['fips'] = df['GEOID'].str.zfill(5)
        df['latitude'] = pd.to_numeric(df['INTPTLAT'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['INTPTLONG'], errors='coerce')
        df['county_name'] = df['NAME']

        # Select only the columns we need
        coords_df = df[['fips', 'county_name', 'latitude', 'longitude']].copy()

        # Remove any rows with missing coordinates
        coords_df = coords_df.dropna(subset=['latitude', 'longitude'])

        # Save processed coordinates
        coords_df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(coords_df)} county coordinates to {csv_path}")

        # Show sample data
        logger.info("Sample coordinates:")
        print(coords_df.head())

        # Clean up ZIP file
        zip_path.unlink()

        return csv_path

    except Exception as e:
        logger.error(f"Error downloading county coordinates: {e}")

        # Fallback: try the more generic URL
        fallback_url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/Gaz_counties_national.zip"
        logger.info(f"Trying fallback URL: {fallback_url}")

        try:
            response = requests.get(fallback_url, timeout=60)
            response.raise_for_status()

            zip_path.write_bytes(response.content)
            logger.info("Downloaded fallback file successfully")

            # Extract and process (same logic as above)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                txt_files = [f for f in zip_ref.namelist() if f.endswith('.txt')]
                if txt_files:
                    txt_file = txt_files[0]
                    with zip_ref.open(txt_file) as f:
                        content = f.read().decode('utf-8')

                    lines = content.strip().split('\n')
                    header = lines[0].split('\t')

                    data = []
                    for line in lines[1:]:
                        row = line.split('\t')
                        if len(row) == len(header):
                            data.append(row)

                    df = pd.DataFrame(data, columns=header)
                    df['fips'] = df['GEOID'].str.zfill(5)
                    df['latitude'] = pd.to_numeric(df['INTPTLAT'], errors='coerce')
                    df['longitude'] = pd.to_numeric(df['INTPTLONG'], errors='coerce')
                    df['county_name'] = df['NAME']

                    coords_df = df[['fips', 'county_name', 'latitude', 'longitude']].copy()
                    coords_df = coords_df.dropna(subset=['latitude', 'longitude'])

                    coords_df.to_csv(csv_path, index=False)
                    logger.info(f"Saved {len(coords_df)} county coordinates to {csv_path}")

                    zip_path.unlink()
                    return csv_path

        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")
            return None


def test_coordinates():
    """Test the coordinate data"""
    csv_path = Path('data/county_coordinates.csv')
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} counties")
        logger.info("Sample data:")
        print(df.head())

        # Check a few known counties
        known_counties = {
            '01001': 'Autauga County, Alabama',
            '06037': 'Los Angeles County, California',
            '48201': 'Harris County, Texas',
            '36061': 'New York County, New York'
        }

        for fips, name in known_counties.items():
            county = df[df['fips'] == fips]
            if not county.empty:
                lat, lng = county.iloc[0]['latitude'], county.iloc[0]['longitude']
                logger.info(f"{name}: {lat:.3f}, {lng:.3f}")
    else:
        logger.error("County coordinates file not found")


if __name__ == "__main__":
    coord_file = download_county_coordinates()
    if coord_file:
        test_coordinates()
    else:
        logger.error("Failed to download county coordinates")