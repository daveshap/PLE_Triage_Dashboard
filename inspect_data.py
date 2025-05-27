import pandas as pd
from pathlib import Path

# Load the data and examine GeoFIPS structure
csv_path = Path('data/CAINC4__ALL_AREAS_1969_2023.csv')
if not csv_path.exists():
    # Try the Alaska file first
    csv_path = Path('data/CAINC4_AK_1969_2023.csv')

print(f"Reading from: {csv_path}")
df = pd.read_csv(csv_path, encoding='latin-1', low_memory=False)

print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Look at unique GeoFIPS values
print(f"\nSample GeoFIPS values:")
geo_fips = df['GeoFIPS'].unique()
print(f"First 20 GeoFIPS: {geo_fips[:20]}")
print(f"Total unique GeoFIPS: {len(geo_fips)}")

# Look for county-like patterns
county_patterns = [fips for fips in geo_fips if str(fips).isdigit() and len(str(fips)) == 5]
print(f"\n5-digit numeric GeoFIPS (likely counties): {len(county_patterns)}")
print(f"Sample county FIPS: {county_patterns[:10]}")

# Check what LineCode 50 (wages) looks like
wages_data = df[df['LineCode'] == 50]
print(f"\nWages data (LineCode 50): {len(wages_data)} rows")
if len(wages_data) > 0:
    print(f"Sample wages GeoFIPS: {wages_data['GeoFIPS'].unique()[:10]}")