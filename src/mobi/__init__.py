"""
Mobi Vancouver API Tools

A Python package for interacting with Vancouver Rogers Mobi bike share APIs.
"""

from mobi.data_downloader import (
    MobiDataDownloaderError,
    download_all_trip_data,
    get_available_data_files,
)
from mobi.data_processor import (
    DataProcessorError,
    combine_trip_data,
    save_to_parquet,
)
from mobi.gbfs import GBFSClient, GBFSClientError
from mobi.station_data import (
    StationDataError,
    fetch_station_info_from_gbfs,
    fetch_station_status_from_gbfs,
)

__version__ = "0.1.0"

__all__ = [
    "GBFSClient",
    "GBFSClientError",
    "MobiDataDownloaderError",
    "download_all_trip_data",
    "get_available_data_files",
    "DataProcessorError",
    "combine_trip_data",
    "save_to_parquet",
    "StationDataError",
    "fetch_station_info_from_gbfs",
    "fetch_station_status_from_gbfs",
]
