"""
Module for processing Mobi bike share data.

This module handles reading raw CSV files and combining them into a unified
bronze dataset without altering the source schema.
"""

import re
from pathlib import Path

import pandas as pd
from loguru import logger


class DataProcessorError(Exception):
    """Base exception for data processor errors."""


def _sanitize_columns(column_names: list[str]) -> list[str]:
    """
    Produce Spark-safe, unique column names using lower snake-case.

    Args:
        column_names: Original column names.

    Returns:
        List of sanitized column names with duplicates disambiguated.
    """
    sanitized: list[str] = []

    for raw_name in column_names:
        name = str(raw_name).strip().lower()
        name = re.sub(r"[^a-z0-9_]+", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        if not name:
            name = "column"
        if name[0].isdigit():
            name = f"_{name}"

        sanitized.append(name)

    return sanitized


def read_trip_data_file(file_path: Path) -> pd.DataFrame:
    """
    Read a trip data CSV file into a DataFrame.

    Args:
        file_path: Path to the data file

    Returns:
        DataFrame containing the trip data

    Raises:
        DataProcessorError: If the file type is unsupported or cannot be read
    """
    file_path = Path(file_path)

    last_exception = None
    for encoding in ("utf-8", "latin1"):
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                on_bad_lines="skip",
                low_memory=False,
            )
            logger.info(
                "Successfully read {file_name} with encoding {encoding}",
                file_name=file_path.name,
                encoding=encoding,
            )
            return df
        except UnicodeDecodeError as exc:
            last_exception = exc
            logger.warning(
                "Failed to decode {file_name} with encoding {encoding}",
                file_name=file_path.name,
                encoding=encoding,
            )
        except (pd.errors.ParserError, pd.errors.EmptyDataError, OSError, ValueError) as exc:
            last_exception = exc
            logger.warning(
                "Error while reading {file_name} with encoding {encoding}: {error}",
                file_name=file_path.name,
                encoding=encoding,
                error=exc,
            )

    error_message = (
        f"Failed to read {file_path.name} using UTF-8 or Latin-1 encodings"
    )
    logger.error(
        "{message}. Last error: {error}",
        message=error_message,
        error=last_exception,
    )
    raise DataProcessorError(error_message) from last_exception

def combine_trip_data(file_paths: list[Path]) -> pd.DataFrame:
    """
    Read and combine multiple trip data files into a single DataFrame.

    Args:
        file_paths: list of paths to trip data files

    Returns:
        Combined DataFrame with all trip data

    Raises:
        DataProcessorError: If files cannot be read or combined
    """
    if not file_paths:
        raise DataProcessorError("No files provided to combine")

    all_dataframes = []

    for i, file_path in enumerate(file_paths, 1):
        logger.info(
            "Reading file [{index}/{total}]: {file_name}",
            index=i,
            total=len(file_paths),
            file_name=file_path.name,
        )
        try:
            df = read_trip_data_file(file_path)

            # Add metadata about the source file
            df["source_file"] = file_path.name

            original_columns = list(df.columns)
            sanitized_columns = _sanitize_columns(original_columns)
            if original_columns != sanitized_columns:
                mapping = {
                    original: sanitized
                    for original, sanitized in zip(original_columns, sanitized_columns)
                    if original != sanitized
                }
            df.columns = sanitized_columns

            all_dataframes.append(df)
            logger.info(
                "Loaded {row_count:,} rows from {file_name}",
                row_count=len(df),
                file_name=file_path.name,
            )

        except DataProcessorError as e:
            logger.error("Failed to load {file_name}: {error}", file_name=file_path.name, error=e)
            continue

    if not all_dataframes:
        raise DataProcessorError("No data files were successfully read")

    logger.info("Combining {file_count} dataframes", file_count=len(all_dataframes))
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    logger.info(
        "Combined dataset has {row_count:,} total rows",
        row_count=len(combined_df),
    )

    return combined_df


def save_to_parquet(
    df: pd.DataFrame,
    output_path: Path,
    compression: str = "snappy",
) -> Path:
    """
    Save a DataFrame to a Parquet file (Spark/Delta compatible).

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
        # Use pyarrow engine and coerce timestamps to microseconds so Spark can read the file
        # without TIMESTAMP(NANOS) incompatibility. This is important when Spark reads the Parquet
        # directly (as opposed to pandas→Spark conversion).
        df.to_parquet(
            output_path,
            engine="pyarrow",
            compression=compression,
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info("Saved dataset to {path}", path=str(output_path))
        logger.info(
            "File stats for {file_name}: size={size:.2f} MB, rows={rows:,}, columns={columns}",
            file_name=output_path.name,
            size=file_size_mb,
            rows=len(df),
            columns=len(df.columns),
        )
        return output_path

    except Exception as e:
        logger.error(
            "Failed to save parquet file {file_name}: {error}",
            file_name=output_path.name,
            error=e,
        )
        raise DataProcessorError(f"Failed to save Parquet file: {e}") from e

