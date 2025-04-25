import json
import re
from unittest.mock import call
from unittest.mock import mock_open
from unittest.mock import patch

from etl.constants import INFO_LOG_LEVEL
from etl.constants import PROPERTIES_TABLE
from etl.constants import WARNING_LOG_LEVEL
from etl.update_zipcodes_from_cache import get_zipcodes_cache_as_json
from etl.update_zipcodes_from_cache import update_property_zipcodes_in_db_from_cache


def normalize_query(query: str) -> str:
    """Remove extra spaces and newlines from a SQL query for consistent comparisons."""
    return re.sub(r"\s+", " ", query).strip()


class TestGetZipcodesCacheAsJSON:

    @patch("etl.update_zipcodes_from_cache.download_zipcodes_cache_from_s3")
    @patch("etl.update_zipcodes_from_cache.open", new_callable=mock_open, read_data='{"1001": "12345", "1002": "67890"}')
    @patch("etl.update_zipcodes_from_cache.os.path.exists", return_value=True)
    @patch("etl.update_zipcodes_from_cache.custom_logger")
    def test_get_zipcodes_cache_as_json_success(self, mock_logger, mock_exists, mock_open_file, mock_download):
        """Test loading the ZIP codes cache file successfully."""
        result = get_zipcodes_cache_as_json()
        assert result == {"1001": "12345", "1002": "67890"}
        mock_logger.assert_called_once_with(INFO_LOG_LEVEL, "Loading zipcodes cache from S3 as JSON...")

    @patch("etl.update_zipcodes_from_cache.download_zipcodes_cache_from_s3")
    @patch("etl.update_zipcodes_from_cache.open", new_callable=mock_open)
    @patch("etl.update_zipcodes_from_cache.os.path.exists", return_value=True)
    @patch("etl.update_zipcodes_from_cache.custom_logger")
    def test_get_zipcodes_cache_as_json_json_error(self, mock_logger, mock_exists, mock_open_file, mock_download):
        """Test handling invalid JSON in the cache file."""
        mock_open_file.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        result = get_zipcodes_cache_as_json()
        assert result == {}  # Ensure an empty dictionary is returned on error
        mock_logger.assert_any_call(WARNING_LOG_LEVEL,
                                    "Error loading zipcodes cache: Invalid JSON: line 1 column 1 (char 0)")

    @patch("etl.update_zipcodes_from_cache.download_zipcodes_cache_from_s3")
    @patch("etl.update_zipcodes_from_cache.os.path.exists", return_value=False)
    @patch("etl.update_zipcodes_from_cache.custom_logger")
    def test_get_zipcodes_cache_as_json_file_not_found(self, mock_logger, mock_exists, mock_download):
        """Test the behavior when the ZIP codes cache file is not found."""
        result = get_zipcodes_cache_as_json()
        assert result == {}  # Ensure a fallback empty dictionary is returned
        mock_logger.assert_not_called()  # No logging since cache file doesn't exist


class TestUpdatePropertyZipcodesInDBFromCache:

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_success(self, mock_execute):
        """Test successfully updating properties in the database using the ZIP codes cache."""
        # Mock database row updates
        mock_execute.return_value = 1  # Each update affects one row
        zipcodes_cache = {"1001": "12345", "1002": "67890"}

        num_updated = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        # Assertions
        assert num_updated == 2  # Ensure the total number of successful updates is returned
        assert mock_execute.call_count == 2  # Queries should be executed for each cache entry

        # Normalize and compare the queries to handle formatting differences
        expected_calls = [
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("12345", "1001")),
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("67890", "1002")),
        ]

        # Extract and normalize the actual query calls
        actual_calls = [
            (normalize_query(call_args[0][0]), call_args[0][1])  # call_args[0][0] is query, call_args[0][1] is params
            for call_args in mock_execute.call_args_list
        ]

        assert actual_calls == expected_calls

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_partial_failure(self, mock_execute):
        """Test updating the database with some failures in the ZIP codes cache updates."""
        # Mock database row updates
        mock_execute.side_effect = [1, 0]  # Simulate the second update failing
        zipcodes_cache = {"1001": "12345", "1002": "67890"}

        num_updated = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        # Assertions
        assert num_updated == 1  # Only one successful update
        assert mock_execute.call_count == 2  # Two attempts were made

        # Normalize and compare the queries to handle formatting differences
        expected_calls = [
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("12345", "1001")),
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("67890", "1002")),
        ]

        # Extract and normalize the actual query calls
        actual_calls = [
            (normalize_query(call_args[0][0]), call_args[0][1])  # call_args[0][0] is query, call_args[0][1] is params
            for call_args in mock_execute.call_args_list
        ]

        assert actual_calls == expected_calls

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_no_updates(self, mock_execute):
        """Test the case where no ZIP codes match database entries (zero updates)."""
        # Mock zero rows updated for each query
        mock_execute.return_value = 0
        zipcodes_cache = {"1001": "12345", "1002": "67890"}

        result = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        # Assertions
        assert result == 0  # No updates were made
        assert mock_execute.call_count == 2  # Two attempts were made

        # Normalize and compare the queries to handle formatting differences
        expected_calls = [
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("12345", "1001")),
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("67890", "1002")),
        ]

        # Extract and normalize the actual query calls
        actual_calls = [
            (normalize_query(call_args[0][0]), call_args[0][1])  # call_args[0][0] is query, call_args[0][1] is params
            for call_args in mock_execute.call_args_list
        ]

        assert actual_calls == expected_calls

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_empty_cache(self, mock_execute):
        """Test the behavior when the ZIP codes cache is empty."""
        # No queries should be executed
        zipcodes_cache = {}

        result = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        # Assertions
        assert result == 0
        mock_execute.assert_not_called()  # No database interactions for an empty cache
