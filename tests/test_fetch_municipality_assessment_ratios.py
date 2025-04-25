import socket
from unittest.mock import MagicMock
from unittest.mock import patch

from etl.constants import ASSESSMENT_RATIOS_TABLE
from etl.constants import CNY_COUNTY_LIST
from etl.constants import INFO_LOG_LEVEL
from etl.constants import MINIMUM_ASSESSMENT_YEAR
from etl.constants import OPEN_NY_BASE_URL
from etl.constants import WARNING_LOG_LEVEL
from etl.open_ny_apis.municipality_assessment_ratios import check_if_county_assessment_ratio_exists
from etl.open_ny_apis.municipality_assessment_ratios import fetch_county_assessment_ratios
from etl.open_ny_apis.municipality_assessment_ratios import fetch_municipality_assessment_ratios
from etl.open_ny_apis.municipality_assessment_ratios import save_municipality_assessment_ratios


def test_check_if_county_assessment_ratio_exists_no_matching_record():
    """Test when rate_year and county_name have no matching records."""
    mocked_query_result = []

    with patch("etl.open_ny_apis.municipality_assessment_ratios.execute_db_query", return_value=mocked_query_result):
        does_ratio_exist = check_if_county_assessment_ratio_exists(2024, "Cayuga")

    assert does_ratio_exist is False


def test_check_if_county_assessment_ratio_exists_matching_record():
    """Test when there is a matching record for rate_year and county_name."""
    test_rate_year = 2024
    test_county_name = "Cayuga"
    mocked_query_result = [(test_rate_year, "050100", "Auburn", test_county_name, 88.00)]

    with patch("etl.open_ny_apis.municipality_assessment_ratios.execute_db_query", return_value=mocked_query_result):
        does_ratio_exist = check_if_county_assessment_ratio_exists(test_rate_year, test_county_name)

        assert does_ratio_exist is True


def test_fetch_county_assessment_ratios_success():
    """Test successful data retrieval from Socrata API."""
    with patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_custom_logger, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.Socrata") as mock_socrata_client:
        app_token = "app_token"
        rate_year = 2024
        county_name = "Cayuga"
        mock_response = [
            {
                "rate_year": f"{2024}",
                "swis_code": "050100",
                "type": "City",
                "county_name": county_name,
                "municipality_name": "Auburn",
                "residential_assessment_ratio": "88.00"
            },
            {
                "rate_year": f"{2024}",
                "swis_code": "052000",
                "type": "Town",
                "county_name": county_name,
                "municipality_name": "Aurelius",
                "residential_assessment_ratio": "76.11"
            }
        ]
        mock_client_instance = MagicMock()
        mock_client_instance.get.return_value = mock_response
        mock_socrata_client.return_value.__enter__.return_value = mock_client_instance

        result = fetch_county_assessment_ratios(app_token, rate_year, county_name)

        assert result == mock_response
        mock_socrata_client.assert_called_once_with(OPEN_NY_BASE_URL, app_token=app_token, timeout=60)
        mock_client_instance.get.assert_called_once()
        mock_custom_logger.assert_called_once_with(
            INFO_LOG_LEVEL,
            f"Fetching municipality assessment ratios for rate_year: {rate_year} and county_name: {county_name}")


def test_fetch_county_assessment_ratios_failure():
    """Test failure behavior when Socrata API raises an exception."""
    with patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_custom_logger, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.Socrata") as mock_socrata_client:
        app_token = "app_token"
        rate_year = 2024
        county_name = "Cayuga"
        api_error_message = "API error"
        mock_client_instance = MagicMock()
        mock_client_instance.get.side_effect = Exception(api_error_message)
        mock_socrata_client.return_value.__enter__.return_value = mock_client_instance

        result = fetch_county_assessment_ratios(app_token, rate_year, county_name)

        assert result is None
        mock_custom_logger.assert_any_call(
            INFO_LOG_LEVEL,
            f"Fetching municipality assessment ratios for rate_year: {rate_year} and county_name: {county_name}")
        mock_custom_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            f"Failed fetching municipality assessment ratios for rate_year: {rate_year} and county_name: {county_name}. Error: {api_error_message}"
        )


def test_fetch_county_assessment_ratios_retryable_error():
    """Test that retryable errors are properly raised and handled in fetch_county_assessment_ratios."""
    # Prepare the mocks
    with patch("etl.open_ny_apis.municipality_assessment_ratios.Socrata") as mock_socrata_client:
        app_token = "mock_token"
        rate_year = 2024
        county_name = "Cayuga"
        mock_client_instance = MagicMock()
        mock_client_instance.get.side_effect = socket.timeout
        mock_socrata_client.return_value.__enter__.return_value = mock_client_instance

        try:
            fetch_county_assessment_ratios(app_token, rate_year, county_name)
        except socket.timeout:
            pass
        else:
            assert False, "Retryable error did not propagate as expected"


def test_municipality_assessment_ratios_data_already_exists():
    """Test fetch_county_assessment_ratios is always skipped if records exists in db for all years and counties checked."""

    with patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_logger, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.fetch_county_assessment_ratios") as mock_fetch_county_ratios, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.check_if_county_assessment_ratio_exists") as mock_check_exists:
        mock_check_exists.return_value = True
        mock_fetch_county_ratios.return_value = None
        app_token = "mock_token"

        results = fetch_municipality_assessment_ratios(app_token, MINIMUM_ASSESSMENT_YEAR)

        for county in CNY_COUNTY_LIST:
            mock_check_exists.assert_any_call(MINIMUM_ASSESSMENT_YEAR, county)

        mock_fetch_county_ratios.assert_not_called()

        assert mock_logger.call_count == len(CNY_COUNTY_LIST) + 2
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            f"Starting fetching municipality assessment ratios for {MINIMUM_ASSESSMENT_YEAR}...")
        mock_logger.assert_any_call(INFO_LOG_LEVEL,
                                    f"Completed fetching municipality assessment ratios, {len(results)} found.")
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            f"Found municipality assessment ratios for rate_year: 2024 and county_name: {CNY_COUNTY_LIST[0]}, skipping."
        )
        assert results == []


def test_fetch_municipality_assessment_ratios_data_not_exists():
    """Test fetch_county_assessment_ratios is called and returns data when no preexisting data in db exists."""

    with patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_logger, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.fetch_county_assessment_ratios") as mock_fetch_county_ratios, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.check_if_county_assessment_ratio_exists") as mock_check_exists:
        mock_check_exists.return_value = False
        fake_response = [
            {
                "rate_year": "2024",
                "swis_code": "050100",
                "type": "City",
                "county_name": "Cayuga",
                "municipality_name": "Auburn",
                "residential_assessment_ratio": "96.00"
            },
            {
                "rate_year": "2024",
                "swis_code": "052000",
                "type": "Town",
                "county_name": "Cayuga",
                "municipality_name": "Aurelius",
                "residential_assessment_ratio": "81.83"
            }
        ]
        mock_fetch_county_ratios.return_value = fake_response
        app_token = "mock_token"

        results = fetch_municipality_assessment_ratios(app_token, MINIMUM_ASSESSMENT_YEAR)
        expected_call_count = len(CNY_COUNTY_LIST) * (2024 - MINIMUM_ASSESSMENT_YEAR + 1)
        assert mock_fetch_county_ratios.call_count == expected_call_count
        assert len(results) == expected_call_count * len(fake_response)
        assert results[0] == fake_response[0]
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            f"Starting fetching municipality assessment ratios for {MINIMUM_ASSESSMENT_YEAR}...")
        mock_logger.assert_any_call(INFO_LOG_LEVEL, f"Completed fetching municipality assessment ratios, {len(results)} found.")


def test_save_all_valid_ratios():
    """Test saving all valid municipality ratios."""
    mock_data = [
        {
            "rate_year": "2024",
            "swis_code": "050100",
            "type": "City",
            "county_name": "Cayuga",
            "municipality_name": "Auburn",
            "residential_assessment_ratio": "96.00"
        },
        {
            "rate_year": "2024",
            "swis_code": "052000",
            "type": "Town",
            "county_name": "Cayuga",
            "municipality_name": "Aurelius",
            "residential_assessment_ratio": "81.83"
        }
    ]

    with patch("etl.open_ny_apis.municipality_assessment_ratios.insert_or_replace_into_database") as mock_db, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_logger:
        mock_db.return_value = (2, 0)

        save_municipality_assessment_ratios(mock_data)

        mock_db.assert_called_once()
        assert mock_db.call_args[0][1] == [
            "rate_year",
            "municipality_code",
            "county_name",
            "municipality_name",
            "residential_assessment_ratio"
        ]
        assert mock_db.call_args[0][2] == [
            (2024, "050100", "Cayuga", "Auburn", 96.00),
            (2024, "052000", "Cayuga", "Aurelius", 81.83)
        ]
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            "Completed saving 2 valid municipality assessment ratios to database rows_inserted: 2, rows_failed: 0."
        )


def test_save_some_invalid_ratios():
    """Test case for mixed valid and invalid municipality ratios."""
    mock_data = [
        {
            "rate_year": "1824",
            "swis_code": "050100",
            "type": "City",
            "county_name": "Cayuga",
            "municipality_name": "Auburn",
            "residential_assessment_ratio": "96.00"
        },
        {
            "rate_year": "2024",
            "swis_code": "052000",
            "type": "Town",
            "county_name": "Cayuga",
            "municipality_name": "Aurelius",
            "residential_assessment_ratio": "81.83"
        }
    ]

    with patch("etl.open_ny_apis.municipality_assessment_ratios.insert_or_replace_into_database") as mock_db, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_logger:
        mock_db.return_value = (1, 0)

        save_municipality_assessment_ratios(mock_data)
        mock_db.assert_called_once()
        mock_db.assert_called_with(
            ASSESSMENT_RATIOS_TABLE,
            [
                "rate_year",
                "municipality_code",
                "county_name",
                "municipality_name",
                "residential_assessment_ratio"
            ],
            [(2024, "052000", "Cayuga", "Aurelius", 81.83)]
        )
        mock_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            "Failed to validate municipality assessment ratio {'rate_year': '1824', 'swis_code': '050100', 'type': 'City', 'county_name': 'Cayuga', 'municipality_name': 'Auburn', 'residential_assessment_ratio': '96.00'} Errors:"
        )
        mock_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            "Error in field rate_year. Message: Input should be greater than or equal to 2024")
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            "Completed saving 1 valid municipality assessment ratios to database rows_inserted: 1, rows_failed: 0."
        )


def test_save_none_ratios():
    """Test case for empty municipality ratios."""
    with patch("etl.open_ny_apis.municipality_assessment_ratios.custom_logger") as mock_logger, \
            patch("etl.open_ny_apis.municipality_assessment_ratios.insert_or_replace_into_database") as mock_db:
        save_municipality_assessment_ratios([])

        mock_db.assert_not_called()
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            "No valid municipality assessment ratios found, skipping."
        )
