"""Integration tests for the GBFS client module using real API."""

import pytest

from mobi.gbfs import GBFSClient, GBFSClientError


class TestGBFSClient:
    """Integration tests using real Mobi Vancouver GBFS API."""

    @pytest.fixture
    def client(self) -> GBFSClient:
        """Create a GBFS client with default (working) endpoint."""
        return GBFSClient()

    def test_initialization_default_url(self) -> None:
        """Test client initialization with default discovery URL."""
        client = GBFSClient()
        assert client.discovery_url == GBFSClient.DEFAULT_DISCOVERY_URL
        assert "gbfs.kappa.fifteen.eu" in client.discovery_url
        assert client.timeout == 10

    def test_get_system_information(self, client: GBFSClient) -> None:
        """Test getting system information from real API."""
        result = client.get_system_information()

        assert "data" in result
        assert "name" in result["data"]
        assert "Mobi" in result["data"]["name"]
        assert "Vancouver" in result["data"]["name"]
        assert result["data"]["timezone"] == "America/Vancouver"

    def test_get_feeds(self, client: GBFSClient) -> None:
        """Test getting all available feeds from real API."""
        feeds = client.get_feeds()

        # Verify expected feeds are present
        assert "system_information" in feeds
        assert "station_information" in feeds
        assert "station_status" in feeds

        # All URLs should be valid
        for feed_name, url in feeds.items():
            assert url.startswith("https://")
            assert "gbfs.kappa.fifteen.eu" in url

    def test_get_station_information(self, client: GBFSClient) -> None:
        """Test getting station information from real API."""
        result = client.get_station_information()

        assert "data" in result
        assert "stations" in result["data"]

        stations = result["data"]["stations"]
        assert len(stations) > 0  # Should have stations

        # Check first station has expected fields
        first_station = stations[0]
        assert "station_id" in first_station
        assert "name" in first_station
        assert "lat" in first_station
        assert "lon" in first_station
        assert "capacity" in first_station

    def test_get_station_status(self, client: GBFSClient) -> None:
        """Test getting real-time station status from real API."""
        result = client.get_station_status()

        assert "data" in result
        assert "stations" in result["data"]

        stations = result["data"]["stations"]
        assert len(stations) > 0  # Should have stations

        # Check first station has expected status fields
        first_station = stations[0]
        assert "station_id" in first_station
        assert "num_bikes_available" in first_station
        assert "num_docks_available" in first_station

        # Values should be non-negative integers
        assert first_station["num_bikes_available"] >= 0
        assert first_station["num_docks_available"] >= 0

    def test_station_count_consistency(self, client: GBFSClient) -> None:
        """Test that station information and status have same count."""
        station_info = client.get_station_information()
        station_status = client.get_station_status()

        info_count = len(station_info["data"]["stations"])
        status_count = len(station_status["data"]["stations"])

        # Should have same number of stations
        assert info_count == status_count
        assert info_count > 200  # Vancouver has 250+ stations

    def test_get_system_alerts(self, client: GBFSClient) -> None:
        """Test getting system alerts from real API."""
        result = client.get_system_alerts()

        # Should return valid structure even if no alerts
        assert "data" in result
        assert "alerts" in result["data"]

        # Alerts is a list (may be empty)
        assert isinstance(result["data"]["alerts"], list)

    def test_get_feed_by_name(self, client: GBFSClient) -> None:
        """Test getting a specific feed by name."""
        # Get vehicle types feed
        result = client.get_feed("vehicle_types")

        assert "data" in result
        assert "vehicle_types" in result["data"]

    def test_invalid_feed_raises_error(self, client: GBFSClient) -> None:
        """Test that requesting invalid feed raises error."""
        with pytest.raises(GBFSClientError, match="not found"):
            client.get_feed("nonexistent_feed")

    def test_feeds_caching(self, client: GBFSClient) -> None:
        """Test that feed URLs are cached after first request."""
        # First call should fetch from API
        feeds1 = client.get_feeds()

        # Second call should use cached data
        feeds2 = client.get_feeds()

        assert feeds1 == feeds2
        assert client._feed_urls is not None

    def test_custom_timeout(self) -> None:
        """Test client with custom timeout."""
        client = GBFSClient(timeout=30)
        assert client.timeout == 30

        # Should still work with custom timeout
        result = client.get_system_information()
        assert "data" in result
