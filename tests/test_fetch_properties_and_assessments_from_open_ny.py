from unittest.mock import MagicMock
from unittest.mock import patch

from pydantic import ValidationError

from etl.constants import INFO_LOG_LEVEL
from etl.constants import NY_PROPERTY_ASSESSMENTS_TABLE
from etl.constants import OPEN_NY_LIMIT_PER_PAGE
from etl.constants import OPEN_NY_PROPERTY_ASSESSMENTS_API_ID
from etl.constants import PROPERTIES_TABLE
from etl.constants import WARNING_LOG_LEVEL
from etl.fetch_properties_and_assessments_from_open_ny import check_if_property_assessments_exist
from etl.fetch_properties_and_assessments_from_open_ny import fetch_properties_and_assessments_from_open_ny
from etl.fetch_properties_and_assessments_from_open_ny import fetch_property_assessments_page
from etl.fetch_properties_and_assessments_from_open_ny import save_property_assessments


def test_check_if_property_assessments_exist_no_matching_record():
    """Test when there are NOT matching records for roll year and county_name."""
    test_rate_year = 2024
    test_county_name = "Oswego"
    mocked_query_result = [(0,)]

    with patch("etl.fetch_properties_and_assessments_from_open_ny.execute_db_query", return_value=mocked_query_result):
        does_county_roll_year_exist = check_if_property_assessments_exist(test_rate_year, test_county_name)
        assert does_county_roll_year_exist is False


def test_check_if_property_assessments_exist_matching_record():
    """Test when there are matching records for roll year and county_name."""
    test_rate_year = 2024
    test_county_name = "Oswego"
    mocked_query_result = [(1,)]

    with patch("etl.fetch_properties_and_assessments_from_open_ny.execute_db_query", return_value=mocked_query_result):
        does_county_roll_year_exist = check_if_property_assessments_exist(test_rate_year, test_county_name)
        assert does_county_roll_year_exist is True


@patch("etl.fetch_properties_and_assessments_from_open_ny.Socrata")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_fetch_property_assessments_page_success(mock_custom_logger, mock_socrata):
    mock_response = [{"print_key_code": "123"}, {"print_key_code": "456"}]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_response
    mock_socrata.return_value.__enter__.return_value = mock_client
    app_token = "fake_token"
    roll_year = 2024
    county_name = "Oswego"
    where_clause = "where_clause_example"
    offset = 0

    result = fetch_property_assessments_page(app_token, roll_year, county_name, where_clause, offset)

    mock_custom_logger.assert_called_with(
        INFO_LOG_LEVEL,
        f"Fetching property assessments for county_name: {county_name} starting at offset {offset}..."
    )
    mock_client.get.assert_called_once_with(
        OPEN_NY_PROPERTY_ASSESSMENTS_API_ID,
        roll_year=roll_year,
        county_name=county_name,
        roll_section=1,
        limit=OPEN_NY_LIMIT_PER_PAGE,
        offset=offset,
        order="swis_code,print_key_code ASC",
        where=where_clause
    )
    assert result == mock_response


@patch("etl.fetch_properties_and_assessments_from_open_ny.Socrata")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_fetch_property_assessments_page_exception(mock_custom_logger, mock_socrata):
    mock_client = MagicMock()
    mock_client.get.side_effect = Exception("API Error")
    mock_socrata.return_value.__enter__.return_value = mock_client
    app_token = "fake_token"
    roll_year = 2024
    county_name = "Oswego"
    where_clause = "where_clause_example"
    offset = 0

    result = fetch_property_assessments_page(app_token, roll_year, county_name, where_clause, offset)

    mock_custom_logger.assert_called_with(
        WARNING_LOG_LEVEL,
        f"Failed fetching property assessments for county_name: {county_name} at offset {offset}. Error: API Error"
    )
    assert result is None


@patch("etl.fetch_properties_and_assessments_from_open_ny.Socrata")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_fetch_property_assessments_page_empty_response(mock_custom_logger, mock_socrata):
    mock_client = MagicMock()
    mock_client.get.return_value = []
    mock_socrata.return_value.__enter__.return_value = mock_client
    app_token = "fake_token"
    roll_year = 2024
    county_name = "Oswego"
    where_clause = "where_clause_example"
    offset = 0

    result = fetch_property_assessments_page(app_token, roll_year, county_name, where_clause, offset)

    mock_custom_logger.assert_called_with(
        INFO_LOG_LEVEL,
        f"Fetching property assessments for county_name: {county_name} starting at offset {offset}..."
    )
    assert result == []


@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
@patch("etl.fetch_properties_and_assessments_from_open_ny.fetch_property_assessments_page")
@patch("etl.fetch_properties_and_assessments_from_open_ny.check_if_property_assessments_exist")
@patch("etl.fetch_properties_and_assessments_from_open_ny.get_ny_property_classes_for_where_clause")
@patch("etl.fetch_properties_and_assessments_from_open_ny.CNY_COUNTY_LIST", new_callable=lambda: ["County1", "County2"])
def test_fetch_properties_and_assessments_from_open_ny_success_returns_all(
    mock_county_list, mock_get_property_classes, mock_check_if_exist, mock_fetch_page, mock_logger):
    """Test successful fetching for all counties."""
    mock_get_property_classes.return_value = "property_class IN (\"210\", \"220\")"
    mock_check_if_exist.return_value = False
    mock_fetch_page.side_effect = [
        [{"key": "property1"}],  # County1 page 1
        [{"key": "property2"}],  # County1 page 2
        [],  # County1 no more data
        [{"key": "property3"}],  # County2 page 1
        [{"key": "property4"}],  # County2 page 2
        []  # County2 no more data
    ]
    app_token = "fake_token"
    query_year = 2024

    result = fetch_properties_and_assessments_from_open_ny(app_token, query_year)

    mock_logger.assert_any_call(
        INFO_LOG_LEVEL, "Starting fetching CNY property assessments for roll_year 2024..."
    )
    mock_get_property_classes.assert_called_once()
    mock_check_if_exist.assert_any_call(query_year, "County1")
    mock_check_if_exist.assert_any_call(query_year, "County2")
    assert mock_fetch_page.call_count == 6
    assert result == [{"key": "property1"}, {"key": "property2"}, {"key": "property3"}, {"key": "property4"}]


@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
@patch("etl.fetch_properties_and_assessments_from_open_ny.fetch_property_assessments_page")
@patch("etl.fetch_properties_and_assessments_from_open_ny.check_if_property_assessments_exist")
@patch("etl.fetch_properties_and_assessments_from_open_ny.get_ny_property_classes_for_where_clause")
@patch("etl.fetch_properties_and_assessments_from_open_ny.CNY_COUNTY_LIST", new_callable=lambda: ["County1", "County2"])
def test_fetch_properties_and_assessments_from_open_ny_skip_counties_with_existing_data(
    mock_county_list, mock_get_property_classes, mock_check_if_exist, mock_fetch_page, mock_logger):
    """Test that counties with existing data are skipped."""
    mock_get_property_classes.return_value = "property_class IN (\"210\", \"220\")"

    # First county has data, second doesn't
    mock_check_if_exist.side_effect = [True, False]
    mock_fetch_page.side_effect = [
        [{"key": "property_from_county2"}],
        []
    ]
    app_token = "fake_token"
    query_year = 2024

    result = fetch_properties_and_assessments_from_open_ny(app_token, query_year)

    mock_logger.assert_any_call(
        INFO_LOG_LEVEL,
        "Property assessments for county_name: County1 in roll year 2024 already exist, ending."
    )
    # Skipped County1; only fetched for County2
    assert mock_fetch_page.call_count == 2
    assert result == [{"key": "property_from_county2"}]


@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
@patch("etl.fetch_properties_and_assessments_from_open_ny.fetch_property_assessments_page")
@patch("etl.fetch_properties_and_assessments_from_open_ny.check_if_property_assessments_exist")
@patch("etl.fetch_properties_and_assessments_from_open_ny.get_ny_property_classes_for_where_clause")
@patch("etl.fetch_properties_and_assessments_from_open_ny.CNY_COUNTY_LIST", new_callable=lambda: ["County1"])
def test_fetch_properties_and_assessments_from_open_ny_empty_responses_end_fetching(
    mock_county_list, mock_get_property_classes, mock_check_if_exist, mock_fetch_page, mock_logger):
    """Test function handles None API responses gracefully."""
    mock_get_property_classes.return_value = "property_class IN (\"210\", \"220\")"
    mock_check_if_exist.return_value = False
    mock_fetch_page.side_effect = [
        [{"key": "property1"}],
        None
    ]
    app_token = "fake_token"
    query_year = 2024

    result = fetch_properties_and_assessments_from_open_ny(app_token, query_year)

    assert mock_fetch_page.call_count == 2
    assert result == [{"key": "property1"}]


@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
@patch("etl.fetch_properties_and_assessments_from_open_ny.get_ny_property_classes_for_where_clause")
def test_fetch_properties_and_assessments_from_open_ny_where_clause_construction(mock_get_property_classes, mock_logger):
    """Test that 'where_clause' is constructed as expected."""
    mock_get_property_classes.return_value = "property_class IN (\"210\", \"220\")"
    app_token = "fake_token"
    query_year = 2024

    fetch_properties_and_assessments_from_open_ny(app_token, query_year)

    mock_logger.assert_any_call(
        INFO_LOG_LEVEL, "\nwhere_clause built: roll_section = 1 AND property_class IN (\"210\", \"220\")\n"
    )
    mock_get_property_classes.assert_called_once()


@patch("etl.fetch_properties_and_assessments_from_open_ny.NYPropertyAssessment")
@patch("etl.fetch_properties_and_assessments_from_open_ny.insert_into_database")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_save_property_assessments_successful_validation_and_insertion(mock_logger, mock_insert_db, mock_model):
    """Test when all properties are valid and inserted successfully."""
    mock_instance = MagicMock()
    mock_instance.to_properties_row.return_value = {"column1": "value1", "column2": "value2"}
    mock_instance.to_ny_property_assessments_row.return_value = {"columnA": "valueA", "columnB": "valueB"}
    mock_model.side_effect = lambda **kwargs: mock_instance
    mock_insert_db.return_value = (10, 0)
    all_properties = [{"key1": "value1"}, {"key2": "value2"}]

    save_property_assessments(all_properties)

    mock_model.assert_called()
    mock_instance.to_properties_row.assert_called()
    mock_instance.to_ny_property_assessments_row.assert_called()
    mock_insert_db.assert_any_call(
        PROPERTIES_TABLE,
        ["column1", "column2"],
        [("value1", "value2"), ("value1", "value2")])
    mock_insert_db.assert_any_call(
        NY_PROPERTY_ASSESSMENTS_TABLE,
        ["columnA", "columnB"],
        [("valueA", "valueB"), ("valueA", "valueB")])
    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Completed saving 2 valid properties rows_inserted: 10, rows_failed: 0.")
    mock_logger.assert_any_call(
        INFO_LOG_LEVEL,
        "Completed saving 2 valid ny_property_assessment_data rows_inserted: 10, rows_failed: 0.")


@patch("etl.fetch_properties_and_assessments_from_open_ny.NYPropertyAssessment")
@patch("etl.fetch_properties_and_assessments_from_open_ny.insert_into_database")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_save_property_assessments_partial_validation_failure(mock_logger, mock_insert_db, mock_model):
    """Test partial validation failure, where some properties are invalid."""
    mock_instance = MagicMock()
    mock_instance.to_properties_row.return_value = {"column1": "value1", "column2": "value2"}
    mock_instance.to_ny_property_assessments_row.return_value = {"columnA": "valueA", "columnB": "valueB"}

    def side_effect(**kwargs):
        if "invalid" in kwargs["key1"]:
            raise ValidationError.from_exception_data(
                title='Validation Error',
                line_errors=[{
                    'loc': ('key1',),
                    'msg': 'Invalid field',
                    'type': 'value_error',
                    'ctx': {'error': 'Invalid field'}
                }]
            )

        return mock_instance

    mock_model.side_effect = side_effect
    mock_insert_db.return_value = (1, 0)
    all_properties = [{"key1": "valid1"}, {"key1": "valid2"}, {"key1": "invalid"}]

    save_property_assessments(all_properties)

    assert mock_model.call_count == 3
    mock_logger.assert_any_call(INFO_LOG_LEVEL,
                                "Completed saving 2 valid ny_property_assessment_data rows_inserted: 1, rows_failed: 0.")
    mock_logger.assert_any_call(WARNING_LOG_LEVEL, "Failed to validate property assessment:")
    mock_logger.assert_any_call(WARNING_LOG_LEVEL, "- Error: Field: key1. Message: Value error, Invalid field")
    mock_insert_db.assert_any_call(
        PROPERTIES_TABLE,
        ["column1", "column2"],
        [("value1", "value2"), ("value1", "value2")])


@patch("etl.fetch_properties_and_assessments_from_open_ny.NYPropertyAssessment")
@patch("etl.fetch_properties_and_assessments_from_open_ny.insert_into_database")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_save_property_assessments_all_validation_failures(mock_logger, mock_insert_db, mock_model):
    """Test when all properties fail validation."""
    mock_model.side_effect = ValidationError.from_exception_data(
        title='Validation Error',
        line_errors=[{
            'loc': ('key1',),
            'msg': 'Invalid field',
            'type': 'value_error',
            'ctx': {'error': 'Invalid field'}
        }]
    )
    all_properties = [{"key1": "invalid1"}, {"key1": "invalid2"}]

    save_property_assessments(all_properties)

    assert mock_model.call_count == 2
    mock_logger.assert_any_call(INFO_LOG_LEVEL, "No valid properties found, skipping saving to database.")
    mock_insert_db.assert_not_called()


@patch("etl.fetch_properties_and_assessments_from_open_ny.NYPropertyAssessment")
@patch("etl.fetch_properties_and_assessments_from_open_ny.insert_into_database")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_save_property_assessments_empty_input_list(mock_logger, mock_insert_db, mock_model):
    """Test when the input list is empty."""
    save_property_assessments([])

    mock_model.assert_not_called()
    mock_insert_db.assert_not_called()
    mock_logger.assert_any_call(INFO_LOG_LEVEL, "No valid properties found, skipping saving to database.")


@patch("etl.fetch_properties_and_assessments_from_open_ny.NYPropertyAssessment")
@patch("etl.fetch_properties_and_assessments_from_open_ny.insert_into_database")
@patch("etl.fetch_properties_and_assessments_from_open_ny.custom_logger")
def test_save_property_assessments_database_failure_handling(mock_logger, mock_insert_db, mock_model):
    """Test when database insertion fails."""
    mock_instance = MagicMock()
    mock_instance.to_properties_row.return_value = {"column1": "value1", "column2": "value2"}
    mock_instance.to_ny_property_assessments_row.return_value = {"columnA": "valueA", "columnB": "valueB"}
    mock_model.side_effect = lambda **kwargs: mock_instance
    mock_insert_db.return_value = (0, 1)
    all_properties = [{"key1": "valid"}]

    save_property_assessments(all_properties)

    mock_logger.assert_any_call(
        INFO_LOG_LEVEL,
        "Completed saving 1 valid properties rows_inserted: 0, rows_failed: 1.")
    mock_logger.assert_any_call(
        INFO_LOG_LEVEL,
        "Completed saving 1 valid ny_property_assessment_data rows_inserted: 0, rows_failed: 1.")
