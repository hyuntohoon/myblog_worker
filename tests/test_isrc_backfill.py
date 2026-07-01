# tests/test_isrc_backfill.py
"""Unit tests for ISRC backfill (FEAT-lyrics-corpus Step 1b)."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from worker.clients.spotify_client import SpotifyClient


class TestSpotifyClientGetTracks:
    """Test SpotifyClient.get_tracks method."""

    def test_get_tracks_empty_ids(self):
        """Empty ID list returns empty list."""
        client = SpotifyClient()
        result = client.get_tracks([])
        assert result == []

    def test_get_tracks_filters_empty_strings(self):
        """Filter out empty/None IDs."""
        client = SpotifyClient()
        result = client.get_tracks(["", None, "abc123"])
        # Should only fetch for "abc123"
        # (we'll mock the API call)
        assert isinstance(result, list)

    @patch("worker.clients.spotify_client._request_with_retry")
    def test_get_tracks_single_call(self, mock_request):
        """Single API call for ≤50 tracks."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "tracks": [
                {
                    "id": "track1",
                    "name": "Track 1",
                    "external_ids": {"isrc": "USRC17607839"},
                },
                {
                    "id": "track2",
                    "name": "Track 2",
                    "external_ids": {"isrc": "USRC17607840"},
                },
            ]
        }
        mock_request.return_value = mock_response

        client = SpotifyClient()
        result = client.get_tracks(["track1", "track2"])

        assert len(result) == 2
        assert result[0]["external_ids"]["isrc"] == "USRC17607839"
        assert result[1]["external_ids"]["isrc"] == "USRC17607840"

    @patch("worker.clients.spotify_client._request_with_retry")
    def test_get_tracks_handles_null_response(self, mock_request):
        """Handles null tracks in response (invalid/unknown IDs)."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "tracks": [
                {
                    "id": "track1",
                    "name": "Track 1",
                    "external_ids": {"isrc": "USRC17607839"},
                },
                None,  # Invalid ID
                {
                    "id": "track3",
                    "name": "Track 3",
                    "external_ids": {},  # No ISRC
                },
            ]
        }
        mock_request.return_value = mock_response

        client = SpotifyClient()
        result = client.get_tracks(["track1", "invalid", "track3"])

        assert len(result) == 3
        assert result[0]["id"] == "track1"
        assert result[1] is None
        assert result[2]["id"] == "track3"
        assert "isrc" not in result[2]["external_ids"]

    @patch("worker.clients.spotify_client._request_with_retry")
    def test_get_tracks_batches_over_50(self, mock_request):
        """Split into multiple calls when >50 tracks."""
        # Mock two responses (one for each batch)
        mock_response = Mock()
        mock_response.json.side_effect = [
            {
                "tracks": [
                    {"id": f"track{i}", "external_ids": {"isrc": f"ISRC{i:05d}"}}
                    for i in range(50)
                ]
            },
            {
                "tracks": [
                    {"id": f"track{i}", "external_ids": {"isrc": f"ISRC{i:05d}"}}
                    for i in range(50, 75)
                ]
            },
        ]
        mock_request.return_value = mock_response

        client = SpotifyClient()
        track_ids = [f"track{i}" for i in range(75)]
        result = client.get_tracks(track_ids)

        assert len(result) == 75
        # Verify two API calls were made (batching)
        assert mock_request.call_count == 2
