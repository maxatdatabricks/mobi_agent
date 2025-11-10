"""
GBFS (General Bikeshare Feed Specification) API client for Mobi Vancouver.

This module provides functions to interact with the GBFS API endpoints
to retrieve real-time bike share data.
"""

from typing import Any, Dict, Optional

import requests


class GBFSClientError(Exception):
    """Base exception for GBFS client errors."""

    pass


def get_gbfs_feed(url: str, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetch a GBFS feed from the given URL.

    Args:
        url: The URL of the GBFS feed
        timeout: Request timeout in seconds

    Returns:
        Dict containing the parsed JSON response

    Raises:
        GBFSClientError: If the request fails or returns invalid data
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        if "data" not in data:
            raise GBFSClientError(f"Invalid GBFS response: missing 'data' key")

        return data
    except requests.exceptions.RequestException as e:
        raise GBFSClientError(f"Failed to fetch GBFS feed from {url}: {e}")
    except ValueError as e:
        raise GBFSClientError(f"Failed to parse JSON response from {url}: {e}")


class GBFSClient:
    """
    Client for interacting with Mobi Vancouver's GBFS API.

    The GBFS API provides real-time data about bike share stations,
    bike availability, and system status.
    """

    # Default GBFS discovery URL for Mobi Vancouver
    DEFAULT_DISCOVERY_URL = "https://gbfs.kappa.fifteen.eu/gbfs/2.2/mobi/en/gbfs.json"

    def __init__(
        self,
        discovery_url: Optional[str] = None,
        timeout: int = 10,
    ) -> None:
        """
        Initialize the GBFS client.

        Args:
            discovery_url: URL for the GBFS discovery feed (gbfs.json)
            timeout: Request timeout in seconds
        """
        self.discovery_url = discovery_url or self.DEFAULT_DISCOVERY_URL
        self.timeout = timeout
        self._feed_urls: Optional[Dict[str, str]] = None

    def _get_feed_urls(self) -> Dict[str, str]:
        """
        Fetch and cache the feed URLs from the GBFS discovery endpoint.

        Returns:
            Dict mapping feed names to their URLs
        """
        if self._feed_urls is None:
            discovery_data = get_gbfs_feed(self.discovery_url, self.timeout)

            feeds = discovery_data.get("data", {}).get("en", {}).get("feeds", [])
            if not feeds:
                raise GBFSClientError("No feeds found in GBFS discovery response")

            self._feed_urls = {feed["name"]: feed["url"] for feed in feeds}

        return self._feed_urls

    def get_feeds(self) -> Dict[str, str]:
        """
        Get all available GBFS feed URLs.

        Returns:
            Dict mapping feed names to their URLs
        """
        return self._get_feed_urls()

    def get_feed(self, feed_name: str) -> Dict[str, Any]:
        """
        Fetch a specific GBFS feed by name.

        Args:
            feed_name: Name of the feed (e.g., 'station_information', 'station_status')

        Returns:
            Dict containing the feed data

        Raises:
            GBFSClientError: If the feed is not found or cannot be fetched
        """
        feed_urls = self._get_feed_urls()

        if feed_name not in feed_urls:
            available = ", ".join(feed_urls.keys())
            raise GBFSClientError(
                f"Feed '{feed_name}' not found. Available feeds: {available}"
            )

        return get_gbfs_feed(feed_urls[feed_name], self.timeout)

    def get_station_information(self) -> Dict[str, Any]:
        """
        Get static information about all bike share stations.

        Returns:
            Dict containing station information including location, capacity, etc.
        """
        return self.get_feed("station_information")

    def get_station_status(self) -> Dict[str, Any]:
        """
        Get real-time status of all bike share stations.

        Returns:
            Dict containing current bike and dock availability for each station
        """
        return self.get_feed("station_status")

    def get_system_information(self) -> Dict[str, Any]:
        """
        Get general information about the bike share system.

        Returns:
            Dict containing system-wide information like name, timezone, etc.
        """
        return self.get_feed("system_information")

    def get_system_alerts(self) -> Dict[str, Any]:
        """
        Get current system alerts and messages.

        Returns:
            Dict containing active system alerts
        """
        try:
            return self.get_feed("system_alerts")
        except GBFSClientError:
            # Some systems don't have alerts, return empty structure
            return {"data": {"alerts": []}}

    def get_free_bike_status(self) -> Dict[str, Any]:
        """
        Get status of free-floating bikes (if available).

        Returns:
            Dict containing information about dockless bikes

        Note:
            This feed may not be available for all systems
        """
        try:
            return self.get_feed("free_bike_status")
        except GBFSClientError:
            # Not all systems have free-floating bikes
            return {"data": {"bikes": []}}
