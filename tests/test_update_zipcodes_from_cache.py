import json
import re
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
        assert result == {}
        mock_logger.assert_any_call(WARNING_LOG_LEVEL,
                                    "Error loading zipcodes cache: Invalid JSON: line 1 column 1 (char 0)")

    @patch("etl.update_zipcodes_from_cache.download_zipcodes_cache_from_s3")
    @patch("etl.update_zipcodes_from_cache.os.path.exists", return_value=False)
    @patch("etl.update_zipcodes_from_cache.custom_logger")
    def test_get_zipcodes_cache_as_json_file_not_found(self, mock_logger, mock_exists, mock_download):
        """Test the behavior when the ZIP codes cache file is not found."""
        result = get_zipcodes_cache_as_json()
        assert result == {}
        mock_logger.assert_not_called()


class TestUpdatePropertyZipcodesInDBFromCache:

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_success(self, mock_execute):
        """Test successfully updating properties in the database using the ZIP codes cache."""
        mock_execute.return_value = 1
        zipcodes_cache = {"1001": "12345", "1002": "67890"}

        num_updated = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        assert num_updated == 2
        assert mock_execute.call_count == 2
        expected_calls = [
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("12345", "1001")),
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("67890", "1002")),
        ]
        actual_calls = [
            (normalize_query(call_args[0][0]), call_args[0][1])
            for call_args in mock_execute.call_args_list
        ]

        assert actual_calls == expected_calls

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_partial_failure(self, mock_execute):
        """Test updating the database with some failures in the ZIP codes cache updates."""
        mock_execute.side_effect = [1, 0]
        zipcodes_cache = {"1001": "12345", "1002": "67890"}
        num_updated = update_property_zipcodes_in_db_from_cache(zipcodes_cache)
        assert num_updated == 1
        assert mock_execute.call_count == 2
        expected_calls = [
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("12345", "1001")),
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("67890", "1002")),
        ]
        actual_calls = [
            (normalize_query(call_args[0][0]), call_args[0][1])
            for call_args in mock_execute.call_args_list
        ]

        assert actual_calls == expected_calls

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_no_updates(self, mock_execute):
        """Test the case where no ZIP codes match database entries (zero updates)."""
        mock_execute.return_value = 0
        zipcodes_cache = {"1001": "12345", "1002": "67890"}

        result = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        assert result == 0
        assert mock_execute.call_count == 2
        expected_calls = [
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("12345", "1001")),
            (normalize_query(f"UPDATE {PROPERTIES_TABLE} SET address_zip = ? WHERE id = ?"), ("67890", "1002")),
        ]
        actual_calls = [
            (normalize_query(call_args[0][0]), call_args[0][1])
            for call_args in mock_execute.call_args_list
        ]

        assert actual_calls == expected_calls

    @patch("etl.update_zipcodes_from_cache.execute_db_query")
    def test_update_property_zipcodes_in_db_from_cache_empty_cache(self, mock_execute):
        """Test the behavior when the ZIP codes cache is empty."""
        zipcodes_cache = {}

        result = update_property_zipcodes_in_db_from_cache(zipcodes_cache)

        assert result == 0
        mock_execute.assert_not_called()
