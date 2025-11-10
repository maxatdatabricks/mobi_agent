# Mobi Vancouver Bike Share API

Tools for downloading and processing Vancouver's Mobi bike share data.

## Quick Start

```bash
# Install
uv pip install -e ".[dev]"

# Run notebook (Databricks or Jupyter)
# Open 01_data.ipynb

# Run tests
uv run pytest
```

## What's Included

- **GBFS API Client**: Real-time station data from `https://gbfs.kappa.fifteen.eu/gbfs/2.2/mobi/en/`
- **Data Downloader**: Downloads all historic trip CSVs from Google Drive (2018-2025)
- **Data Processor**: Standardizes schemas and combines into Parquet
- **Notebook**: `01_data.ipynb` - Complete data download pipeline for Databricks

## Data

### Trip Data
- **Source**: https://www.mobibikes.ca/en/system-data
- **Coverage**: January 2018 - Present (~7.6M trips)
- **Format**: Monthly CSV files → Combined Parquet

### Station Data
- **Source**: GBFS API (real-time)
- **Coverage**: 262 active stations
- **Format**: JSON → Parquet/CSV

## Usage

### From Notebook (Recommended)

Open `01_data.ipynb` in Databricks or Jupyter and run all cells.

### From Code

```python
from mobi import (
    GBFSClient,
    download_all_trip_data,
    combine_trip_data,
    save_to_parquet,
    fetch_station_info_from_gbfs,
)

# Download trips
files = download_all_trip_data("./data/raw")
trips = combine_trip_data(files)
save_to_parquet(trips, "./data/mobi_trips.parquet")

# Get stations
stations = fetch_station_info_from_gbfs()
print(f"{len(stations)} stations")
```

## API Reference

### GBFS Client

```python
from mobi import GBFSClient

client = GBFSClient()
system = client.get_system_information()  # System info
stations = client.get_station_information()  # All stations
status = client.get_station_status()  # Real-time availability
```

### Data Download

```python
from mobi import download_all_trip_data

# Download all monthly CSV files
files = download_all_trip_data("./output_dir")
# Returns: List of file paths
```

### Data Processing

```python
from mobi import combine_trip_data, save_to_parquet

# Combine and standardize
df = combine_trip_data(file_paths)
# Returns: pandas DataFrame with ~7.6M rows

# Save as Parquet
save_to_parquet(df, "output.parquet")
```

## Testing

```bash
# Run all tests (uses real API)
uv run pytest -v

# 16/16 integration tests pass in ~6.5s
```

## Data Output

After running `01_data.ipynb`:

```
/dbfs/mobi_data/  (Databricks)
├── mobi_trips.parquet          # 7.6M trips, 82MB
├── mobi_stations.parquet       # 262 stations
└── mobi_stations.csv           # Same, CSV format
```

## Project Structure

```
mobi/
├── 01_data.ipynb              # Main notebook
├── src/mobi/                  # Python package
│   ├── gbfs.py                # GBFS API client
│   ├── data_downloader.py     # Download trip CSVs
│   ├── data_processor.py      # Process & combine data
│   └── station_data.py        # Station data functions
├── tests/                     # Integration tests
│   ├── conftest.py
│   └── test_gbfs.py
├── pyproject.toml             # Dependencies
└── README.md                  # This file
```

## Requirements

- Python 3.9+
- `requests`, `pandas`, `pyarrow`, `beautifulsoup4`, `openpyxl`
- For dev: `pytest`, `pytest-cov`, `ruff`, `mypy`

## License

MIT
