import os
import sqlite3
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from etl.constants import ASSESSMENT_RATIOS_TABLE
from etl.constants import CNY_COUNTY_LIST
from etl.constants import ERROR_LOG_LEVEL
from etl.constants import GENERATED_DATA_DIR
from etl.constants import INFO_LOG_LEVEL
from etl.constants import MINIMUM_ASSESSMENT_YEAR
from etl.constants import NY_PROPERTY_ASSESSMENTS_TABLE
from etl.constants import OPEN_NY_BASE_URL
from etl.constants import PROJECT_ROOT
from etl.constants import PROPERTIES_TABLE
from etl.constants import WARNING_LOG_LEVEL
from etl.db_utilities import insert_into_database
from etl.etl_ml_flow import check_if_county_assessment_ratio_exists
from etl.etl_ml_flow import check_if_property_assessments_exist
from etl.etl_ml_flow import cny_real_estate_etl_workflow
from etl.etl_ml_flow import fetch_county_assessment_ratios
from etl.etl_ml_flow import fetch_municipality_assessment_ratios
from etl.etl_ml_flow import save_municipality_assessment_ratios


def test_check_if_county_assessment_ratio_exists_no_matching_record():
    """Test when rate_year and county_name have no matching records."""
    mocked_query_result = []

    with patch("etl.etl_ml_flow.execute_select_query", return_value=mocked_query_result):
        does_ratio_exist = check_if_county_assessment_ratio_exists(2024, "Cayuga")

    assert does_ratio_exist is False


def test_check_if_county_assessment_ratio_exists_matching_record():
    """Test when there is a matching record for rate_year and county_name."""
    test_rate_year = 2024
    test_county_name = "Cayuga"
    mocked_query_result = [(test_rate_year, "050100", "Auburn", test_county_name, 88.00)]

    with patch("etl.etl_ml_flow.execute_select_query", return_value=mocked_query_result):
        does_ratio_exist = check_if_county_assessment_ratio_exists(test_rate_year, test_county_name)

        assert does_ratio_exist is True


def test_fetch_county_assessment_ratios_success():
    """Test successful data retrieval from Socrata API."""
    with patch("etl.etl_ml_flow.custom_logger") as mock_custom_logger, \
        patch("etl.etl_ml_flow.Socrata") as mock_socrata_client:
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
    with patch("etl.etl_ml_flow.custom_logger") as mock_custom_logger, \
        patch("etl.etl_ml_flow.Socrata") as mock_socrata_client:
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


def test_fetch_municipality_assessment_ratios_data_already_exists():
    """Test fetch_county_assessment_ratios is always skipped if records exists in db for all years and counties checked."""

    with patch("etl.etl_ml_flow.custom_logger") as mock_logger, \
        patch("etl.etl_ml_flow.fetch_county_assessment_ratios") as mock_fetch_county_ratios, \
        patch("etl.etl_ml_flow.check_if_county_assessment_ratio_exists") as mock_check_exists:

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

    with patch("etl.etl_ml_flow.custom_logger") as mock_logger, \
        patch("etl.etl_ml_flow.fetch_county_assessment_ratios") as mock_fetch_county_ratios, \
        patch("etl.etl_ml_flow.check_if_county_assessment_ratio_exists") as mock_check_exists:
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

        # Assert `fetch_county_assessment_ratios` is called for all years and counties
        expected_call_count = len(CNY_COUNTY_LIST) * (2024 - MINIMUM_ASSESSMENT_YEAR + 1)
        assert mock_fetch_county_ratios.call_count == expected_call_count

        # All responses should be returned in a single list
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

    with patch("etl.etl_ml_flow.insert_into_database") as mock_db, \
        patch("etl.etl_ml_flow.custom_logger") as mock_logger:
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

    with patch("etl.etl_ml_flow.insert_into_database") as mock_db, \
        patch("etl.etl_ml_flow.custom_logger") as mock_logger:

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
    with patch("etl.etl_ml_flow.custom_logger") as mock_logger, \
        patch("etl.etl_ml_flow.insert_into_database") as mock_db:

        save_municipality_assessment_ratios([])

        mock_db.assert_not_called()
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            "No valid municipality assessment ratios found, skipping."
        )


def test_check_if_property_assessments_exist_no_matching_record():
    """Test when there are NOT matching records for roll year and county_name."""
    test_rate_year = 2024
    test_county_name = "Oswego"
    mocked_query_result = [(0,)]

    with patch("etl.etl_ml_flow.execute_select_query", return_value=mocked_query_result):
        does_county_roll_year_exist = check_if_property_assessments_exist(test_rate_year, test_county_name)
        assert does_county_roll_year_exist is False


def test_check_if_property_assessments_exist_matching_record():
    """Test when there are matching records for roll year and county_name."""
    test_rate_year = 2024
    test_county_name = "Oswego"
    mocked_query_result = [(1,)]

    with patch("etl.etl_ml_flow.execute_select_query", return_value=mocked_query_result):

        does_county_roll_year_exist = check_if_property_assessments_exist(test_rate_year, test_county_name)
        assert does_county_roll_year_exist is True


@patch("os.path.exists")
@patch("etl.etl_ml_flow.custom_logger")
@patch("etl.etl_ml_flow.get_open_ny_app_token")
@patch("etl.etl_ml_flow.download_database_from_s3")
@patch("etl.etl_ml_flow.create_database")
@patch("etl.etl_ml_flow.fetch_properties_and_assessments_from_open_ny")
@patch("etl.etl_ml_flow.fetch_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.get_assessment_year_to_query")
@patch("etl.etl_ml_flow.save_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.upload_database_to_s3")
def test_workflow_with_municipal_assessment_ratios_not_fetched(
    mock_upload,
    mock_save,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists
):
    """Test workflow when token retrieval fails."""
    mock_token.return_value = None

    cny_real_estate_etl_workflow()

    mock_logger.assert_called_once_with(ERROR_LOG_LEVEL, "Cannot proceed, ending ETL workflow.")
    mock_assessment_year.assert_not_called()
    mock_download.assert_not_called()
    mock_create_db.assert_not_called()
    mock_fetch.assert_not_called()
    mock_fetch_properties_and_assessments.assert_not_called()
    mock_save.assert_not_called()
    mock_upload.assert_not_called()
    mock_path_exists.assert_not_called()


@patch("os.path.exists")
@patch("etl.etl_ml_flow.custom_logger")
@patch("etl.etl_ml_flow.get_open_ny_app_token")
@patch("etl.etl_ml_flow.download_database_from_s3")
@patch("etl.etl_ml_flow.create_database")
@patch("etl.etl_ml_flow.fetch_properties_and_assessments_from_open_ny")
@patch("etl.etl_ml_flow.fetch_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.get_assessment_year_to_query")
@patch("etl.etl_ml_flow.save_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.upload_database_to_s3")
def test_workflow_with_municipal_assessment_ratios_not_fetched(
    mock_upload,
    mock_save,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists
):
    """Test workflow with valid token but database not already in s3 calls to create db."""
    mock_assessment_year.return_value = 2025
    mock_token.return_value = "valid_token"
    mock_path_exists.return_value = False
    mock_fetch.return_value = None
    mock_fetch_properties_and_assessments.return_value = None

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_called_once()


@patch("os.path.exists")
@patch("etl.etl_ml_flow.custom_logger")
@patch("etl.etl_ml_flow.get_open_ny_app_token")
@patch("etl.etl_ml_flow.download_database_from_s3")
@patch("etl.etl_ml_flow.create_database")
@patch("etl.etl_ml_flow.fetch_properties_and_assessments_from_open_ny")
@patch("etl.etl_ml_flow.fetch_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.get_assessment_year_to_query")
@patch("etl.etl_ml_flow.save_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.upload_database_to_s3")
def test_workflow_with_municipal_assessment_ratios_not_fetched(
    mock_upload,
    mock_save,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists
):
    """Test workflow when no assessment ratios are fetched."""
    mock_assessment_year.return_value = 2025
    mock_token.return_value = "valid_token"
    mock_path_exists.return_value = True
    mock_fetch.return_value = None
    mock_fetch_properties_and_assessments.return_value = None

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_not_called()
    mock_fetch.assert_called_once_with(app_token="valid_token", query_year=2025)
    mock_fetch_properties_and_assessments.assert_called_once_with(app_token="valid_token", query_year=2025)
    mock_save.assert_not_called()
    mock_upload.assert_called_once()


@patch("os.path.exists")
@patch("etl.etl_ml_flow.custom_logger")
@patch("etl.etl_ml_flow.get_open_ny_app_token")
@patch("etl.etl_ml_flow.download_database_from_s3")
@patch("etl.etl_ml_flow.create_database")
@patch("etl.etl_ml_flow.fetch_properties_and_assessments_from_open_ny")
@patch("etl.etl_ml_flow.fetch_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.get_assessment_year_to_query")
@patch("etl.etl_ml_flow.save_property_assessments")
@patch("etl.etl_ml_flow.save_municipality_assessment_ratios")
@patch("etl.etl_ml_flow.upload_database_to_s3")
def test_workflow_with_municipal_assessment_ratios_not_fetched(
    mock_upload,
    mock_save_ratios,
    mock_save_properties,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists
):
    """Test workflow when assessment ratios are fetched."""
    mock_assessment_year.return_value = 2027
    mock_data = [{"a": "b"}, {"c": "d"}]
    mock_token.return_value = "valid_token"
    mock_path_exists.return_value = True
    mock_fetch.return_value = mock_data
    mock_fetch_properties_and_assessments.return_value = mock_data

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_not_called()
    mock_fetch.assert_called_once_with(app_token="valid_token", query_year=2027)
    mock_save_ratios.assert_called_once_with(mock_data)
    mock_save_properties.assert_called_once_with(mock_data)
    mock_upload.assert_called_once()
