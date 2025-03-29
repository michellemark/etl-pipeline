from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import requests
from pydantic import ValidationError

from etl.zillow_datasets import zillow_zhvi_sfh
from etl.zillow_datasets.zillow_zhvi_sfh import get_current_download_url
from etl.zillow_datasets.zillow_zhvi_sfh import get_free_zillow_zhvi_sfh
from etl.zillow_datasets.zillow_zhvi_sfh import is_cny_county
from etl.zillow_datasets.zillow_zhvi_sfh import parse_csv_row_into_valid_location_for_db
from etl.zillow_datasets.zillow_zhvi_sfh import prepare_db_records


@pytest.fixture
def mock_session():
    """Create a mock session object for testing HTTP requests."""
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_session.get.return_value = mock_response
    mock_response.ok = True
    mock_response.status_code = 200
    return mock_session, mock_response


@pytest.fixture
def valid_city_row():
    """Return a valid city row from CSV data."""
    return {
        "RegionID": "1234",
        "RegionName": "Syracuse",
        "RegionType": "City",
        "StateName": "New York",
        "State": "NY",
        "Metro": "Syracuse, NY",
        "CountyName": "Onondaga County",
        "SizeRank": "150",
        "2020-01-01": "150000",
        "2020-02-01": "151000",
        "2020-03-01": "",
        "NotADate": "something"
    }


class TestGetCurrentDownloadUrl:

    def test_successful_url_retrieval(self, mock_session):
        """Test successful retrieval of download URL."""
        mock_session, mock_response = mock_session

        # Create HTML content with the expected dropdown structure
        html_content = """
        <select id="median-home-value-zillow-home-value-index-zhvi-dropdown-2">
            <option value="https://example.com/data.csv">City</option>
            <option value="something-else">County</option>
        </select>
        """
        mock_response.content = html_content.encode('utf-8')

        # Mock BeautifulSoup parsing
        with patch('etl.zillow_datasets.zillow_zhvi_sfh.BeautifulSoup', autospec=True) as mock_bs:
            mock_soup = MagicMock()
            mock_bs.return_value = mock_soup

            # Mock the dropdown element
            mock_dropdown = MagicMock()
            mock_soup.find.return_value = mock_dropdown

            # Mock finding the City option
            mock_option = MagicMock()
            mock_option.text.strip.return_value = 'City'
            mock_option.has_attr.return_value = True
            mock_option.__getitem__.return_value = "https://example.com/data.csv"
            mock_dropdown.find_all.return_value = [mock_option]

            result = get_current_download_url(session=mock_session)

            # Verify the result matches the expected URL
            assert result == "https://example.com/data.csv"
            mock_session.get.assert_called_once()

    def test_missing_dropdown(self, mock_session):
        """Test when the dropdown element is not found."""
        mock_session, mock_response = mock_session
        mock_response.content = "<html><body>No dropdown here</body></html>".encode('utf-8')

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.BeautifulSoup', autospec=True) as mock_bs:
            mock_soup = MagicMock()
            mock_bs.return_value = mock_soup
            mock_soup.find.return_value = None  # No dropdown found

            result = get_current_download_url(session=mock_session)

            assert result is None
            mock_session.get.assert_called_once()

    def test_city_option_not_found(self, mock_session):
        """Test when the City option is not found in the dropdown."""
        mock_session, mock_response = mock_session
        mock_response.content = "<html><body>Some content</body></html>".encode('utf-8')

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.BeautifulSoup', autospec=True) as mock_bs:
            mock_soup = MagicMock()
            mock_bs.return_value = mock_soup

            # Mock the dropdown element
            mock_dropdown = MagicMock()
            mock_soup.find.return_value = mock_dropdown

            # Mock options without the City option
            mock_option = MagicMock()
            mock_option.text.strip.return_value = 'County'
            mock_dropdown.find_all.return_value = [mock_option]

            result = get_current_download_url(session=mock_session)

            assert result is None
            mock_session.get.assert_called_once()

    def test_invalid_url_format(self, mock_session):
        """Test when the URL doesn't start with https://."""
        mock_session, mock_response = mock_session
        mock_response.content = "<html><body>Some content</body></html>".encode('utf-8')

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.BeautifulSoup', autospec=True) as mock_bs:
            mock_soup = MagicMock()
            mock_bs.return_value = mock_soup

            # Mock the dropdown element
            mock_dropdown = MagicMock()
            mock_soup.find.return_value = mock_dropdown

            # Mock finding the City option with invalid URL
            mock_option = MagicMock()
            mock_option.text.strip.return_value = 'City'
            mock_option.has_attr.return_value = True
            mock_option.__getitem__.return_value = "invalid-url"
            mock_dropdown.find_all.return_value = [mock_option]

            with patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger') as mock_logger:
                result = get_current_download_url(session=mock_session)

                assert result is None
                mock_session.get.assert_called_once()
                mock_logger.assert_called_once()

    def test_request_failure(self, mock_session):
        """Test handling when the HTTP request fails."""
        mock_session, mock_response = mock_session
        mock_response.ok = False
        mock_response.status_code = 403
        mock_response.text = "Forbidden"

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger') as mock_logger:
            result = get_current_download_url(session=mock_session)

            assert result is None
            mock_session.get.assert_called_once()
            mock_logger.assert_called_once()

    def test_request_exception(self, mock_session):
        """Test handling of request exceptions."""
        mock_session, _ = mock_session
        mock_session.get.side_effect = requests.RequestException("Connection error")

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger') as mock_logger:
            result = get_current_download_url(session=mock_session)

            assert result is None
            mock_session.get.assert_called_once()
            mock_logger.assert_called_once()

    def test_general_exception(self, mock_session):
        """Test handling of general exceptions."""
        mock_session, _ = mock_session
        mock_session.get.side_effect = Exception("Unexpected error")

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger') as mock_logger:
            result = get_current_download_url(session=mock_session)

            assert result is None
            mock_session.get.assert_called_once()
            mock_logger.assert_called_once()


class TestParseCSVRowIntoValidLocationForDB:

    @patch('etl.zillow_datasets.zillow_zhvi_sfh.ZillowHomeValueIndexSFHCity')
    def test_valid_row_parsing(self, mock_model_class, valid_city_row):
        """Test parsing a valid CSV row."""
        mock_model = MagicMock()
        mock_model_class.return_value = mock_model
        mock_model.RegionName = "Syracuse"
        mock_model.State = "NY"
        mock_model.generate_county_name.return_value = "Onondaga"

        result = parse_csv_row_into_valid_location_for_db(valid_city_row)

        assert result is not None
        assert result == {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }
        mock_model_class.assert_called_once_with(**valid_city_row)

    def test_validation_error(self):
        """Test handling of validation errors."""
        invalid_city_row = {
            "RegionID": "1234",
            "RegionName": "",
            "RegionType": "City",
            "StateName": "New York",
            "State": "NY",
            "Metro": "Syracuse, NY",
            "CountyName": "Onondaga County",
            "SizeRank": "150",
            "2020-01-01": "150000",
            "2020-02-01": "151000",
            "2020-03-01": "",
            "NotADate": "something"
        }

        with patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger') as mock_logger:
            result = parse_csv_row_into_valid_location_for_db(invalid_city_row)

            assert result is None
            assert mock_logger.call_count >= 2


class TestIsCNYCounty:

    def test_valid_cny_county(self, monkeypatch):
        """Test with a valid CNY county and NY state."""
        monkeypatch.setattr("etl.zillow_datasets.zillow_zhvi_sfh.CNY_COUNTY_LIST", ['Onondaga'])
        monkeypatch.setattr("etl.zillow_datasets.zillow_zhvi_sfh.ALL_PROPERTIES_STATE", 'NY')
        location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }

        result = is_cny_county(location)

        assert result is True

    def test_invalid_county(self, monkeypatch):
        """Test with a non-CNY county."""
        monkeypatch.setattr("etl.zillow_datasets.zillow_zhvi_sfh.CNY_COUNTY_LIST", ['Madison'])
        monkeypatch.setattr("etl.zillow_datasets.zillow_zhvi_sfh.ALL_PROPERTIES_STATE", 'NY')
        location = {
            "municipality_name": "New York",
            "county_name": "New York",
            "state": "NY"
        }

        result = is_cny_county(location)

        assert result is False

    def test_invalid_state(self, monkeypatch):
        """Test with a valid county but invalid state."""
        monkeypatch.setattr("etl.zillow_datasets.zillow_zhvi_sfh.CNY_COUNTY_LIST", ['Onondaga'])
        monkeypatch.setattr("etl.zillow_datasets.zillow_zhvi_sfh.ALL_PROPERTIES_STATE", 'NY')
        location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "PA"
        }

        result = is_cny_county(location)

        assert result is False


class TestPrepareDBRecords:

    def test_prepare_valid_records(self, valid_city_row):
        """Test preparation of valid database records."""
        location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }

        result = prepare_db_records(valid_city_row, location)

        # Should create two records for the two dates with values
        assert len(result) == 2

        # Check first record
        assert result[0] == (
            "Syracuse",
            "Onondaga",
            "NY",
            "2020-01",
            150000.0
        )

        # Check second record
        assert result[1] == (
            "Syracuse",
            "Onondaga",
            "NY",
            "2020-02",
            151000.0
        )

    def test_prepare_records_empty_value(self):
        """Test with a date that has an empty value."""
        csv_row = {
            "2020-01-01": "",  # Empty value
            "2020-02-01": "150000",
            "NotADate": "something"
        }

        location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }

        result = prepare_db_records(csv_row, location)

        # Should only create one record for the date with a value
        assert len(result) == 1
        assert result[0] == (
            "Syracuse",
            "Onondaga",
            "NY",
            "2020-02",
            150000.0
        )

    def test_prepare_records_no_valid_dates(self):
        """Test with no valid dates in the CSV row."""
        csv_row = {
            "NotADate1": "150000",
            "NotADate2": "something"
        }

        location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }

        result = prepare_db_records(csv_row, location)

        # Should return an empty list
        assert result == []

    def test_prepare_records_invalid_date_format(self):
        """Test with an invalid date format."""
        csv_row = {
            "01/01/2020": "150000",  # Wrong format
            "2020-02-01": "151000",
        }

        location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }

        result = prepare_db_records(csv_row, location)

        # Should only process the valid date
        assert len(result) == 1
        assert result[0] == (
            "Syracuse",
            "Onondaga",
            "NY",
            "2020-02",
            151000.0
        )


class TestGetFreeZillowZHVISFH:

    @patch('etl.zillow_datasets.zillow_zhvi_sfh.get_current_download_url')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.csv.DictReader')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.StringIO')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.insert_or_replace_into_database')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.parse_csv_row_into_valid_location_for_db')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.is_cny_county')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.prepare_db_records')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger')
    def test_successful_data_retrieval(
            self, mock_logger, mock_prepare_records, mock_is_cny,
            mock_parse_row, mock_db_insert, mock_stringio,
            mock_dict_reader, mock_get_url, mock_session
    ):
        """Test successful retrieval and processing of data."""
        mock_session, mock_response = mock_session
        mock_get_url.return_value = "https://example.com/data.csv"
        mock_response.text = "sample,csv,data"
        mock_csv_rows = [{"row1": "data1"}, {"row2": "data2"}]
        mock_reader = MagicMock()
        mock_dict_reader.return_value = mock_reader
        mock_reader.__iter__.return_value = iter(mock_csv_rows)
        valid_location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }
        mock_parse_row.return_value = valid_location
        mock_is_cny.return_value = True
        mock_prepare_records.return_value = [("Syracuse", "Onondaga", "NY", "2020-01", 150000.0)]
        mock_db_insert.return_value = (1, 0)

        get_free_zillow_zhvi_sfh(mock_session)

        mock_get_url.assert_called_once()
        mock_session.get.assert_called_once_with("https://example.com/data.csv")
        mock_dict_reader.assert_called_once()
        assert mock_parse_row.call_count == 2
        assert mock_is_cny.call_count == 2
        assert mock_prepare_records.call_count == 2
        assert mock_db_insert.call_count == 2
        mock_logger.assert_called()

    @patch('etl.zillow_datasets.zillow_zhvi_sfh.get_current_download_url')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger')
    def test_no_download_url(self, mock_logger, mock_get_url, mock_session):
        """Test handling when no download URL is found."""
        mock_get_url.return_value = None
        get_free_zillow_zhvi_sfh(mock_session[0])

        mock_get_url.assert_called_once()
        mock_session[0].get.assert_not_called()
        mock_logger.assert_called_once()

    @patch('etl.zillow_datasets.zillow_zhvi_sfh.get_current_download_url')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger')
    def test_download_failure(self, mock_logger, mock_get_url, mock_session):
        """Test handling when the download fails."""
        mock_session, mock_response = mock_session
        mock_get_url.return_value = "https://example.com/data.csv"
        mock_response.ok = False
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        get_free_zillow_zhvi_sfh(mock_session)

        mock_get_url.assert_called_once()
        mock_session.get.assert_called_once()
        assert mock_logger.call_count >= 2

    @patch('etl.zillow_datasets.zillow_zhvi_sfh.get_current_download_url')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.csv.DictReader')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.StringIO')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.parse_csv_row_into_valid_location_for_db')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.is_cny_county')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger')
    def test_non_cny_county_skipped(
            self, mock_logger, mock_is_cny, mock_parse_row,
            mock_stringio, mock_dict_reader, mock_get_url,
            mock_session
    ):
        """Test that non-CNY counties are skipped."""
        mock_session, mock_response = mock_session
        mock_get_url.return_value = "https://example.com/data.csv"
        mock_response.text = "sample,csv,data"
        mock_csv_rows = [{"row1": "data1"}]
        mock_reader = MagicMock()
        mock_dict_reader.return_value = mock_reader
        mock_reader.__iter__.return_value = iter(mock_csv_rows)

        # Mock row validation
        valid_location = {
            "municipality_name": "New York",
            "county_name": "New York",
            "state": "NY"
        }
        mock_parse_row.return_value = valid_location
        mock_is_cny.return_value = False

        get_free_zillow_zhvi_sfh(mock_session)

        mock_get_url.assert_called_once()
        mock_session.get.assert_called_once()
        mock_dict_reader.assert_called_once()
        mock_parse_row.assert_called_once()
        mock_is_cny.assert_called_once()
        mock_logger.assert_called()

    @patch('etl.zillow_datasets.zillow_zhvi_sfh.get_current_download_url')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.csv.DictReader')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.StringIO')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.parse_csv_row_into_valid_location_for_db')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.is_cny_county')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.prepare_db_records')
    @patch('etl.zillow_datasets.zillow_zhvi_sfh.custom_logger')
    def test_no_records_to_store(
            self, mock_logger, mock_prepare_records, mock_is_cny,
            mock_parse_row, mock_stringio, mock_dict_reader,
            mock_get_url, mock_session
    ):
        """Test handling when there are no records to store."""
        mock_session, mock_response = mock_session
        mock_get_url.return_value = "https://example.com/data.csv"
        mock_response.text = "sample,csv,data"
        mock_csv_rows = [{"row1": "data1"}]
        mock_reader = MagicMock()
        mock_dict_reader.return_value = mock_reader
        mock_reader.__iter__.return_value = iter(mock_csv_rows)

        # Mock row validation
        valid_location = {
            "municipality_name": "Syracuse",
            "county_name": "Onondaga",
            "state": "NY"
        }
        mock_parse_row.return_value = valid_location
        mock_is_cny.return_value = True
        mock_prepare_records.return_value = []

        get_free_zillow_zhvi_sfh(mock_session)

        mock_get_url.assert_called_once()
        mock_session.get.assert_called_once()
        mock_dict_reader.assert_called_once()
        mock_parse_row.assert_called_once()
        mock_is_cny.assert_called_once()
        mock_prepare_records.assert_called_once()
        mock_logger.assert_called()
