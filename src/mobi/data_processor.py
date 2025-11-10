"""
Module for processing and standardizing Mobi bike share data.

This module handles reading CSV files, standardizing schemas across different
data formats, and combining data into a unified format.
"""

import zipfile
from pathlib import Path
from typing import List, Optional

import pandas as pd


class DataProcessorError(Exception):
    """Base exception for data processor errors."""

    pass


def read_trip_data_file(file_path: Path) -> pd.DataFrame:
    """
    Read a trip data file (CSV or ZIP) into a DataFrame.

    Args:
        file_path: Path to the data file

    Returns:
        DataFrame containing the trip data

    Raises:
        DataProcessorError: If the file cannot be read
    """
    file_path = Path(file_path)

    try:
        if file_path.suffix.lower() == ".zip":
            # Extract and read CSV from ZIP
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                # Find CSV file in the ZIP
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    raise DataProcessorError(f"No CSV file found in {file_path}")

                # Read the first CSV file
                with zip_ref.open(csv_files[0]) as csv_file:
                    df = pd.read_csv(csv_file)
        else:
            # Read CSV directly
            df = pd.read_csv(file_path)

        return df

    except Exception as e:
        raise DataProcessorError(f"Failed to read {file_path}: {e}")


def standardize_trip_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the schema of a trip data DataFrame.

    Mobi data may have different column names over time. This function
    normalizes them to a consistent format.

    Args:
        df: Input DataFrame with trip data

    Returns:
        DataFrame with standardized column names and types

    Raises:
        DataProcessorError: If required columns are missing
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Define column name mappings (old name -> new name)
    column_mappings = {
        # Common variations of column names
        "departure": "departure_time",
        "return": "return_time",
        "departure_station": "departure_station_name",
        "return_station": "return_station_name",
        "covered_distance": "covered_distance_km",
        "duration": "duration_sec",
        "stopover": "has_stopover",
        "bike": "bike_id",
        "account": "account_id",
    }

    # Apply column mappings (case-insensitive)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    for old_name, new_name in column_mappings.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.rename(columns={old_name: new_name})

    # Parse datetime columns if they exist
    datetime_columns = ["departure_time", "return_time"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Convert numeric columns
    numeric_columns = {
        "covered_distance_km": float,
        "duration_sec": float,
    }
    for col, dtype in numeric_columns.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert boolean columns
    if "has_stopover" in df.columns:
        # Handle various representations of boolean
        df["has_stopover"] = df["has_stopover"].map(
            {
                "Yes": True,
                "No": False,
                "yes": True,
                "no": False,
                True: True,
                False: False,
            }
        )

    return df


def combine_trip_data(file_paths: List[Path]) -> pd.DataFrame:
    """
    Read and combine multiple trip data files into a single DataFrame.

    Args:
        file_paths: List of paths to trip data files

    Returns:
        Combined DataFrame with all trip data

    Raises:
        DataProcessorError: If files cannot be read or combined
    """
    if not file_paths:
        raise DataProcessorError("No files provided to combine")

    all_dataframes = []

    for i, file_path in enumerate(file_paths, 1):
        print(f"[{i}/{len(file_paths)}] Reading {file_path.name}...")

        try:
            df = read_trip_data_file(file_path)
            df = standardize_trip_schema(df)

            # Add metadata about the source file
            df["source_file"] = file_path.name

            all_dataframes.append(df)
            print(f"  ✓ Loaded {len(df):,} rows")

        except DataProcessorError as e:
            print(f"  ✗ Failed: {e}")
            continue

    if not all_dataframes:
        raise DataProcessorError("No data files were successfully read")

    print("\nCombining all data...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"✓ Combined dataset has {len(combined_df):,} total rows")

    return combined_df


def save_to_parquet(
    df: pd.DataFrame,
    output_path: Path,
    compression: str = "snappy",
) -> Path:
    """
    Save a DataFrame to a Parquet file.

    Args:
        df: DataFrame to save
        output_path: Path where the Parquet file should be saved
        compression: Compression algorithm to use (snappy, gzip, brotli, etc.)

    Returns:
        Path to the saved Parquet file

    Raises:
        DataProcessorError: If the file cannot be saved
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_parquet(output_path, compression=compression, index=False)
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"\n✓ Saved to {output_path}")
        print(f"  File size: {file_size_mb:.2f} MB")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        return output_path

    except Exception as e:
        raise DataProcessorError(f"Failed to save Parquet file: {e}")


def get_data_summary(df: pd.DataFrame) -> dict:
    """
    Generate a summary of the trip data.

    Args:
        df: DataFrame with trip data

    Returns:
        Dict containing summary statistics
    """
    summary = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
    }

    # Date range if datetime columns exist
    if "departure_time" in df.columns:
        summary["date_range"] = {
            "start": df["departure_time"].min(),
            "end": df["departure_time"].max(),
        }

    # Null counts
    null_counts = df.isnull().sum()
    summary["null_counts"] = null_counts[null_counts > 0].to_dict()

    # Data types
    summary["dtypes"] = df.dtypes.astype(str).to_dict()

    return summary
