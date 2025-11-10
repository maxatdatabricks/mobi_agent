"""
Module for fetching and processing station information.

This module provides functions to fetch station data from various sources
and save it in a standardized format.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from mobi.gbfs import GBFSClient, GBFSClientError


class StationDataError(Exception):
    """Base exception for station data errors."""

    pass


def fetch_station_info_from_gbfs(
    discovery_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch station information from the GBFS API.

    Args:
        discovery_url: Optional custom GBFS discovery URL

    Returns:
        DataFrame containing station information

    Raises:
        StationDataError: If station data cannot be fetched
    """
    try:
        if discovery_url:
            client = GBFSClient(discovery_url=discovery_url)
        else:
            client = GBFSClient()

        # Get station information
        station_info = client.get_station_information()
        stations = station_info.get("data", {}).get("stations", [])

        if not stations:
            raise StationDataError("No station data returned from GBFS API")

        df = pd.DataFrame(stations)

        # Add timestamp
        last_updated = station_info.get("last_updated")
        if last_updated:
            df["data_fetched_at"] = pd.to_datetime(last_updated, unit="s")

        return df

    except GBFSClientError as e:
        raise StationDataError(f"Failed to fetch GBFS station data: {e}")


def fetch_station_status_from_gbfs(
    discovery_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch real-time station status from the GBFS API.

    Args:
        discovery_url: Optional custom GBFS discovery URL

    Returns:
        DataFrame containing station status

    Raises:
        StationDataError: If station status cannot be fetched
    """
    try:
        if discovery_url:
            client = GBFSClient(discovery_url=discovery_url)
        else:
            client = GBFSClient()

        # Get station status
        station_status = client.get_station_status()
        stations = station_status.get("data", {}).get("stations", [])

        if not stations:
            raise StationDataError("No station status returned from GBFS API")

        df = pd.DataFrame(stations)

        # Add timestamp
        last_updated = station_status.get("last_updated")
        if last_updated:
            df["status_fetched_at"] = pd.to_datetime(last_updated, unit="s")

        return df

    except GBFSClientError as e:
        raise StationDataError(f"Failed to fetch GBFS station status: {e}")


def combine_station_data(
    station_info: pd.DataFrame,
    station_status: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Combine station information and status into a single DataFrame.

    Args:
        station_info: DataFrame with station information
        station_status: Optional DataFrame with station status

    Returns:
        Combined DataFrame with station data
    """
    combined = station_info.copy()

    if station_status is not None:
        # Merge on station_id
        combined = combined.merge(
            station_status,
            on="station_id",
            how="left",
            suffixes=("", "_status"),
        )

    return combined


def save_station_data(
    df: pd.DataFrame,
    output_path: Path,
    format: str = "parquet",
) -> Path:
    """
    Save station data to a file.

    Args:
        df: DataFrame with station data
        output_path: Path where the file should be saved
        format: Output format (parquet, csv, json)

    Returns:
        Path to the saved file

    Raises:
        StationDataError: If the file cannot be saved
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if format == "parquet":
            df.to_parquet(output_path, compression="snappy", index=False)
        elif format == "csv":
            df.to_csv(output_path, index=False)
        elif format == "json":
            df.to_json(output_path, orient="records", indent=2)
        else:
            raise StationDataError(f"Unsupported format: {format}")

        print(f"✓ Saved station data to {output_path}")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")

        return output_path

    except Exception as e:
        raise StationDataError(f"Failed to save station data: {e}")
