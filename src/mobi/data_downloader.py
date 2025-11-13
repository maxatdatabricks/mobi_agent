"""
Module for downloading historic trip data from Mobi by Rogers website.

This module provides functions to scrape and download CSV files containing
historic bike share trip data from https://www.mobibikes.ca/en/system-data
"""

import re
import shutil
import zipfile
from pathlib import Path, PurePosixPath
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class MobiDataDownloaderError(Exception):
    """Base exception for Mobi data downloader errors."""

    pass


def get_available_data_files(
    base_url: str = "https://www.mobibikes.ca/en/system-data",
    timeout: int = 30,
) -> List[dict]:
    """
    Scrape the Mobi system data page to find all available CSV download links.

    Args:
        base_url: URL of the Mobi system data page
        timeout: Request timeout in seconds

    Returns:
        List of dicts containing file metadata (url, month, year, filename)

    Raises:
        MobiDataDownloaderError: If the page cannot be accessed or parsed
    """
    try:
        response = requests.get(base_url, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise MobiDataDownloaderError(f"Failed to fetch data page: {e}")

    try:
        soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        raise MobiDataDownloaderError(f"Failed to parse HTML: {e}")

    # Find all links that look like data download links
    data_files = []
    links = soup.find_all("a", href=True)

    for link in links:
        href = link["href"]
        link_text = link.get_text(strip=True)

        # Look for Google Drive links or direct CSV/ZIP links
        is_gdrive = "drive.google.com" in href
        is_csv_or_zip = ".csv" in href.lower() or ".zip" in href.lower()

        if is_gdrive or is_csv_or_zip:
            # Try to parse month and year from link text
            month_year_match = re.search(
                r"(January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+(\d{4})",
                link_text,
                re.IGNORECASE,
            )

            if month_year_match:
                month = month_year_match.group(1)
                year = month_year_match.group(2)
            else:
                # Try to extract from href
                month_year_match = re.search(r"(\d{4})[-_]?(\d{2})", href)
                if month_year_match:
                    year = month_year_match.group(1)
                    month_num = month_year_match.group(2)
                    month_names = [
                        "January",
                        "February",
                        "March",
                        "April",
                        "May",
                        "June",
                        "July",
                        "August",
                        "September",
                        "October",
                        "November",
                        "December",
                    ]
                    month = month_names[int(month_num) - 1]
                else:
                    month = "Unknown"
                    year = "Unknown"

            # Convert Google Drive view link to download link
            if is_gdrive and "/file/d/" in href:
                # Extract file ID from Google Drive link
                match = re.search(r"/file/d/([^/]+)", href)
                if match:
                    file_id = match.group(1)
                    download_url = (
                        f"https://drive.google.com/uc?export=download&id={file_id}"
                    )
                else:
                    download_url = href
            else:
                # Make absolute URL for direct links
                download_url = urljoin(base_url, href)

            # Create filename
            filename = f"mobi_{year}_{month}.csv".replace(" ", "_")

            data_files.append(
                {
                    "url": download_url,
                    "month": month,
                    "year": year,
                    "filename": filename,
                    "link_text": link_text,
                }
            )

    return data_files


def download_file(
    url: str,
    output_path: Path,
    timeout: int = 120,
    chunk_size: int = 8192,
) -> Path:
    """
    Download a file from a URL to the specified output path.

    Args:
        url: URL of the file to download
        output_path: Local path where the file should be saved
        timeout: Request timeout in seconds
        chunk_size: Size of chunks to download at a time (bytes)

    Returns:
        Path to the downloaded file

    Raises:
        MobiDataDownloaderError: If the download fails
    """
    try:
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

        return output_path

    except requests.exceptions.RequestException as e:
        raise MobiDataDownloaderError(f"Failed to download {url}: {e}")
    except IOError as e:
        raise MobiDataDownloaderError(f"Failed to save file to {output_path}: {e}")


def download_all_trip_data(
    output_dir: Path,
    base_url: str = "https://www.mobibikes.ca/en/system-data",
    overwrite: bool = False,
) -> List[Path]:
    """
    Download all available historic trip data CSV files.

    Args:
        output_dir: Directory where files should be saved
        base_url: URL of the Mobi system data page
        overwrite: Whether to overwrite existing files

    Returns:
        List of paths to downloaded files

    Raises:
        MobiDataDownloaderError: If download fails
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Finding available data files from {base_url}...")
    data_files = get_available_data_files(base_url)
    print(f"Found {len(data_files)} data file(s)")

    downloaded_files = []

    for i, file_info in enumerate(data_files, 1):
        filename = file_info["filename"]
        output_path = output_dir / filename

        if output_path.exists() and not overwrite:
            print(f"[{i}/{len(data_files)}] Skipping {filename} (already exists)")
            downloaded_files.append(output_path)
            continue

        print(
            f"[{i}/{len(data_files)}] Downloading {filename} "
            f"({file_info['month']} {file_info['year']})..."
        )

        try:
            downloaded_path = download_file(file_info["url"], output_path)
            downloaded_files.append(downloaded_path)
            print(f"  ✓ Saved to {downloaded_path}")
        except MobiDataDownloaderError as e:
            print(f"  ✗ Failed: {e}")
            continue

    print(f"\nDownloaded {len(downloaded_files)} file(s) to {output_dir}")
    return downloaded_files


def _resolve_volume_root(target_dir: Path) -> Path:
    """
    Infer the Unity Catalog volume root from a nested destination directory.

    Args:
        target_dir: Directory within the volume hierarchy.

    Returns:
        The inferred volume root directory.
    """
    try:
        return target_dir.parents[1]
    except IndexError:
        return target_dir.parent


def _ensure_bundle_available(bundle_path: Path, volume_root: Path) -> Path:
    """
    Ensure the offline bundle is present on the Unity Catalog volume.

    Args:
        bundle_path: Local path to the bundle shipped with the project.
        volume_root: Destination directory on the volume.

    Returns:
        Path to the bundle located on the volume.

    Raises:
        FileNotFoundError: If the bundle cannot be found in either location.
    """
    target_zip = volume_root / bundle_path.name

    if target_zip.exists():
        return target_zip

    if bundle_path.exists():
        target_zip.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(bundle_path, target_zip)
        return target_zip

    raise FileNotFoundError(
        f"Fallback bundle not found at {bundle_path} or {target_zip}"
    )


def _extract_bundle_subdir(
    archive_path: Path,
    archive_subdir: str,
    destination: Path,
) -> List[Path]:
    """
    Extract a subdirectory from the offline bundle into the destination.

    Args:
        archive_path: Path to the offline bundle on the volume.
        archive_subdir: Directory inside the archive to extract.
        destination: Target directory on the volume.

    Returns:
        List of extracted file paths.

    Raises:
        RuntimeError: If the requested subdirectory is not found.
    """
    destination.mkdir(parents=True, exist_ok=True)
    extracted_files: List[Path] = []
    prefix = PurePosixPath(archive_subdir.strip("/"))

    with zipfile.ZipFile(archive_path, "r") as archive:
        for member in archive.infolist():
            member_path = PurePosixPath(member.filename)

            if member.is_dir():
                continue

            try:
                relative_path = member_path.relative_to(prefix)
            except ValueError:
                continue

            target_path = destination / Path(*relative_path.parts)
            target_path.parent.mkdir(parents=True, exist_ok=True)

            with archive.open(member) as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted_files.append(target_path)

    if not extracted_files:
        raise RuntimeError(
            f"No files were found under '{archive_subdir}' in {archive_path}"
        )

    return extracted_files


def restore_trip_data_from_bundle(raw_dir: Path, bundle_path: Path) -> List[Path]:
    """
    Restore trip CSV files from the offline bundle when downloads fail.

    Args:
        raw_dir: Destination directory for raw trip CSV files.
        bundle_path: Path to the offline bundle.

    Returns:
        Sorted list of CSV file paths available in `raw_dir`.

    Raises:
        FileNotFoundError: If the bundle cannot be located.
        RuntimeError: If extraction completes without producing CSV files.
    """
    existing_csvs = sorted(raw_dir.glob("*.csv"))
    if existing_csvs:
        return existing_csvs

    raw_dir.mkdir(parents=True, exist_ok=True)
    volume_root = _resolve_volume_root(raw_dir)
    archive_path = _ensure_bundle_available(bundle_path, volume_root)

    extracted_files = _extract_bundle_subdir(
        archive_path=archive_path,
        archive_subdir="data/trip_data/raw",
        destination=raw_dir,
    )

    csv_paths = sorted(path for path in extracted_files if path.suffix.lower() == ".csv")
    if not csv_paths:
        csv_paths = sorted(raw_dir.glob("*.csv"))

    if not csv_paths:
        raise RuntimeError(
            f"No CSV files were extracted from fallback bundle {archive_path}"
        )

    return csv_paths


def restore_site_data_from_bundle(site_raw_dir: Path, bundle_path: Path) -> List[Path]:
    """
    Restore site markdown files from the offline bundle when scraping fails.

    Args:
        site_raw_dir: Destination directory for raw markdown content.
        bundle_path: Path to the offline bundle.

    Returns:
        Sorted list of markdown file paths available in `site_raw_dir`.

    Raises:
        FileNotFoundError: If the bundle cannot be located.
        RuntimeError: If extraction completes without producing markdown files.
    """
    existing_markdown = sorted(
        path for path in site_raw_dir.glob("*.md") if path.is_file()
    )
    if existing_markdown:
        return existing_markdown

    site_raw_dir.mkdir(parents=True, exist_ok=True)
    volume_root = _resolve_volume_root(site_raw_dir)
    archive_path = _ensure_bundle_available(bundle_path, volume_root)

    extracted_files = _extract_bundle_subdir(
        archive_path=archive_path,
        archive_subdir="data/mobi_site/raw",
        destination=site_raw_dir,
    )

    markdown_paths = sorted(
        path for path in extracted_files if path.suffix.lower() in {".md", ".markdown"}
    )
    if not markdown_paths:
        markdown_paths = sorted(site_raw_dir.glob("*.md"))

    if not markdown_paths:
        raise RuntimeError(
            f"No markdown files were extracted from fallback bundle {archive_path}"
        )

    return markdown_paths
