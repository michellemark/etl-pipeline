import re
from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from etl.constants import INFO_LOG_LEVEL
from etl.constants import WARNING_LOG_LEVEL
from etl.update_zipcodes_from_census_bureau import create_csv_batch_file
from etl.update_zipcodes_from_census_bureau import get_all_properties_needing_zipcodes_from_database_write_as_csv
from etl.update_zipcodes_from_census_bureau import get_csv_file_path
from etl.update_zipcodes_from_census_bureau import get_zipcodes_from_geocoder_as_batch
from etl.update_zipcodes_from_census_bureau import parse_geocoder_response
from etl.update_zipcodes_from_census_bureau import sanitize_address_string
from etl.update_zipcodes_from_census_bureau import update_null_zipcodes_workflow
from etl.update_zipcodes_from_census_bureau import update_property_zipcodes_with_geocoder_response


@pytest.fixture(autouse=True)
def mock_constants(monkeypatch):
    monkeypatch.setattr("etl.update_zipcodes_from_census_bureau.US_CENSUS_BUREAU_BATCH_SIZE", 3)
    monkeypatch.setattr("etl.update_zipcodes_from_census_bureau.PROPERTIES_TABLE", "properties")


def normalize_query(query: str) -> str:
    """Remove extra spaces and newlines from a SQL query for consistent comparisons."""
    return re.sub(r"\s+", " ", query).strip()


@patch("etl.update_zipcodes_from_census_bureau.EXTRACTED_DATA_DIR", "/tmp/test_directory")
def test_get_csv_file_path():
    """Test that get_csv_file_path returns a valid file path with a timestamp."""
    result = get_csv_file_path()
    assert result.startswith("/tmp/test_directory/zipcode_batch_")
    assert result.endswith(".csv")
    assert len(result.split("_")) >= 3


@patch("etl.update_zipcodes_from_census_bureau.EXTRACTED_DATA_DIR", "/test/directory")
@patch("etl.update_zipcodes_from_census_bureau.datetime")
def test_get_csv_file_path(mock_datetime):
    """Test that get_csv_file_path returns a valid file path with a timestamp."""
    mock_datetime.now.return_value = mock_datetime  # Return the mock itself
    mock_datetime.strftime.return_value = "20251130_093000_000001"

    result = get_csv_file_path()

    expected_path = "/test/directory/zipcode_batch_20251130_093000_000001.csv"
    assert result == expected_path
    mock_datetime.now.assert_called_once()
    mock_datetime.strftime.assert_called_once_with("%Y%m%d_%H%M%S_%f")


@patch("etl.update_zipcodes_from_census_bureau.get_csv_file_path", return_value="/tmp/mock_file.csv")
@patch("builtins.open", new_callable=mock_open)
def test_create_csv_batch_file(mock_open_file, mock_get_csv_file_path):
    """Test that create_csv_batch_file writes data to a CSV file."""
    data = [
        {"Unique ID": 1, "Street address": "123 Mock St", "City": "TestCity", "State": "NY", "ZIP": ""},
        {"Unique ID": 2, "Street address": "456 Example Rd", "City": "SamplePlace", "State": "CA", "ZIP": ""}
    ]

    result = create_csv_batch_file(data)
    assert result == "/tmp/mock_file.csv"
    mock_open_file.assert_called_once_with("/tmp/mock_file.csv", mode="w+", newline="", encoding="utf-8")
    handle = mock_open_file()
    handle.write.assert_any_call("Unique ID,Street address,City,State,ZIP\n")  # Now matches the observed format
    handle.write.assert_any_call("1,123 Mock St,TestCity,NY,\n")
    handle.write.assert_any_call("2,456 Example Rd,SamplePlace,CA,\n")


@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
def test_create_csv_batch_file_no_data(mock_logger):
    """Test that create_csv_batch_file handles no data gracefully."""
    result = create_csv_batch_file([])
    assert result is None
    mock_logger.assert_not_called()


@patch("etl.update_zipcodes_from_census_bureau.get_csv_file_path", return_value="/tmp/error_file.csv")
@patch("builtins.open", side_effect=OSError("Mocked write error"))
@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
def test_create_csv_batch_file_exception(mock_logger, mock_open_file, mock_get_csv_path):
    """Test that errors during CSV creation are logged."""
    data = [{"Unique ID": 1, "Street address": "123 Mock St", "City": "TestCity", "State": "NY", "ZIP": ""}]
    result = create_csv_batch_file(data)
    assert result == "/tmp/error_file.csv"
    mock_logger.assert_called_once_with(
        WARNING_LOG_LEVEL,
        "Error creating csv /tmp/error_file.csv: Mocked write error"
    )


@pytest.mark.parametrize(
    "input_address,expected_result",
    [
        # Test cases from the docstring
        ("1634 Clark St Rd (Parking/Residual)", "1634 Clark St Rd"),  # Removes content in parentheses
        ("John St/Future Site", "John St - Future Site"),  # Replaces slash with " - "
        ("242/246 Clark St Rd", "242 - 246 Clark St Rd"),  # Replaces slash with " - "
        ("off Watkins Rd", "Watkins Rd"),  # Removes "off" at the start
        # Cases for ampersand replacement
        ("123 Main & Elm St", "123 Main - Elm St"),  # Replaces ampersand with " - "
        # Cases for removing quotes
        ("'123 Mock St'", "123 Mock St"),  # Removes single quotes
        ('"123 Mock St"', "123 Mock St"),  # Removes double quotes
        # Cases for removing "+"
        ("123+Main St", "123Main St"),  # Removes "+", no spaces added
        # Combination of patterns
        ("off '123 Main & Elm (Suite/Unit)'", "123 Main - Elm"),  # Combines multiple replacements
        # Cases with only "off"
        ("off", ""),  # Removes "off" without any other text
        ("123 off Main St", "123 off Main St"),  # Leaves "off" if not at the start
        # Miscellaneous edge cases
        ("No changes needed", "No changes needed"),  # Leaves valid addresses unchanged
        ("", ""),  # Empty string should return empty
        ("   ", ""),  # Whitespace-only string
        ("\t\t123 Main St  ", "123 Main St"),  # Strips leading/trailing whitespace
        ("(Just text in parentheses)", ""),  # Leaves nothing if only parentheses
    ]
)
def test_sanitize_address_string(input_address, expected_result):
    """Test all scenarios for sanitize_address_string function."""
    result = sanitize_address_string(input_address)
    assert result == expected_result


@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
@patch("etl.update_zipcodes_from_census_bureau.execute_db_query")
@patch("etl.update_zipcodes_from_census_bureau.create_csv_batch_file")
@patch("etl.update_zipcodes_from_census_bureau.sanitize_address_string")
def test_get_all_properties_batches(
        mock_sanitize, mock_create_csv, mock_execute_query, mock_logger
):
    mock_execute_query.side_effect = [
        [
            (1, "123 Mock St", "Test City", "NY"),
            (2, "456 Fake Blvd", "Sample Town", "CA"),
            (3, "789 Example Ave", "Another City", "TX"),
        ],
        [
            (4, "789 Another Rd", "City A", "FL"),
        ],
        [],
    ]
    mock_sanitize.side_effect = lambda x: x
    mock_create_csv.side_effect = ["/tmp/batch_1.csv", "/tmp/batch_2.csv"]

    result = get_all_properties_needing_zipcodes_from_database_write_as_csv()

    assert len(result) == 2
    assert result == ["/tmp/batch_1.csv", "/tmp/batch_2.csv"]

    expected_query = """
                     SELECT id, address_street, municipality_name, address_state
                     FROM properties
                     WHERE address_zip IS NULL \
                        OR address_zip = ''
                     ORDER BY id
                     LIMIT ? OFFSET ? \
                     """
    expected_query_normalized = normalize_query(expected_query)

    for call_args in mock_execute_query.call_args_list:
        actual_query = call_args.kwargs["query"]
        assert normalize_query(actual_query) == expected_query_normalized

    mock_sanitize.assert_any_call("123 Mock St")
    mock_sanitize.assert_any_call("456 Fake Blvd")
    mock_sanitize.assert_any_call("789 Another Rd")

    mock_create_csv.assert_any_call(
        [
            {
                "Unique ID": 1,
                "Street address": "123 Mock St",
                "City": "Test City",
                "State": "NY",
                "ZIP": "",
            },
            {
                "Unique ID": 2,
                "Street address": "456 Fake Blvd",
                "City": "Sample Town",
                "State": "CA",
                "ZIP": "",
            },
            {
                "Unique ID": 3,
                "Street address": "789 Example Ave",
                "City": "Another City",
                "State": "TX",
                "ZIP": "",
            },
        ]
    )
    mock_create_csv.assert_any_call(
        [
            {
                "Unique ID": 4,
                "Street address": "789 Another Rd",
                "City": "City A",
                "State": "FL",
                "ZIP": "",
            },
        ]
    )

    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Fetching properties without zipcodes...")
    mock_logger.assert_any_call(
        INFO_LOG_LEVEL, "Batch 1 csv created with 3 properties at /tmp/batch_1.csv."
    )
    mock_logger.assert_any_call(
        INFO_LOG_LEVEL, "Batch 2 csv created with 1 properties at /tmp/batch_2.csv."
    )


@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
@patch("etl.update_zipcodes_from_census_bureau.execute_db_query")
@patch("etl.update_zipcodes_from_census_bureau.create_csv_batch_file")
def test_get_all_properties_no_results(mock_create_csv, mock_execute_query, mock_logger):
    mock_execute_query.return_value = []

    result = get_all_properties_needing_zipcodes_from_database_write_as_csv()

    assert result == []
    mock_create_csv.assert_not_called()

    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Fetching properties without zipcodes...")


@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
@patch("etl.update_zipcodes_from_census_bureau.execute_db_query")
@patch("etl.update_zipcodes_from_census_bureau.create_csv_batch_file")
def test_get_all_properties_csv_creation_failure(
        mock_create_csv, mock_execute_query, mock_logger
):
    mock_execute_query.side_effect = [
        [
            (1, "123 Mock St", "Test City", "NY"),
        ],
        [],
    ]
    mock_create_csv.return_value = None

    result = get_all_properties_needing_zipcodes_from_database_write_as_csv()

    assert result == []
    mock_create_csv.assert_called_once_with(
        [
            {
                "Unique ID": 1,
                "Street address": "123 Mock St",
                "City": "Test City",
                "State": "NY",
                "ZIP": "",
            }
        ]
    )
    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Fetching properties without zipcodes...")


@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
@pytest.mark.parametrize(
    "raw_response,expected_rows,expected_log_message",
    [
        (
                # Valid property_id and zip_code combinations
                '1,"Match","Exact",,,"Match","Exact",,,,12345\n'
                '2,"Match","Exact",,,"Match","Exact",,,,67890\n',
                [
                    {"property_id": "1", "zip_code": "12345"},
                    {"property_id": "2", "zip_code": "67890"},
                ],
                "Successfully parsed 2 rows with exact match out of 2 responses from geocoder response.",
        ),
        (
                # Entry with missing property_id
                ',"Match","Exact",,,"Match","Exact",,,,12345\n',
                [],
                "Successfully parsed 0 rows with exact match out of 1 responses from geocoder response.",
        ),
        (
                # Entry with invalid/missing zip_code
                '1,"Match","Exact",,,"Match","Exact",,,,,\n',
                [],
                "Successfully parsed 0 rows with exact match out of 1 responses from geocoder response.",
        ),
        (
                # Mixed valid and invalid entries
                '1,"Match","Exact",,,"Match","Exact",,,,12345\n'
                ',"Match","Exact",,,"Match","Exact",,,,67890\n'
                '2,"Match","Exact",,,"Match","Exact",,,,34567\n',
                [
                    {"property_id": "1", "zip_code": "12345"},
                    {"property_id": "2", "zip_code": "34567"},
                ],
                "Successfully parsed 2 rows with exact match out of 3 responses from geocoder response.",
        ),
        (
                # Completely malformed response
                'Invalid line,missing columns,another invalid line\n',
                [],
                "Successfully parsed 0 rows with exact match out of 1 responses from geocoder response.",
        ),
    ],
)
def test_parse_geocoder_response_property_id_and_zip_code(
        mock_logger, raw_response, expected_rows, expected_log_message
):
    result = parse_geocoder_response(raw_response)
    assert result == expected_rows
    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Parsing geocoder response...")
    mock_logger.assert_any_call(INFO_LOG_LEVEL, expected_log_message)


@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
def test_parse_geocoder_response_invalid(mock_logger):
    """Test that parse_geocoder_response handles invalid input gracefully."""
    invalid_response = "invalid_line_with_no_columns\nanother_wrong_line"
    result = parse_geocoder_response(invalid_response)
    assert result == []

    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Parsing geocoder response...")
    mock_logger.assert_any_call(
        INFO_LOG_LEVEL,
        "Successfully parsed 0 rows with exact match out of 2 responses from geocoder response."
    )


@patch("etl.update_zipcodes_from_census_bureau.requests.post")
@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
@patch("builtins.open", new_callable=mock_open, read_data="mocked csv content")
def test_get_zipcodes_from_geocoder_as_batch_success(mock_open_file, mock_logger, mock_post):
    """Test that get_zipcodes_from_geocoder_as_batch handles successful responses."""
    mock_post.return_value.ok = True
    mock_post.return_value.content = b"property_id,zip_code\n1,12345"

    result = get_zipcodes_from_geocoder_as_batch("/path/to/mock_file.csv")

    assert result == "property_id,zip_code\n1,12345"
    mock_logger.assert_any_call(INFO_LOG_LEVEL, "Batch file submitted successfully. Parsing response...")
    mock_open_file.assert_called_once_with("/path/to/mock_file.csv", "rb")


@patch("etl.update_zipcodes_from_census_bureau.requests.post", side_effect=Exception("Mocked API failure"))
@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
@patch("builtins.open", new_callable=mock_open, read_data="mocked csv content")
def test_get_zipcodes_from_geocoder_as_batch_failure(mock_open_file, mock_logger, mock_post):
    """Test that get_zipcodes_from_geocoder_as_batch handles API exceptions."""
    with pytest.raises(Exception, match="Mocked API failure"):
        get_zipcodes_from_geocoder_as_batch("/path/to/mock_file.csv")

    mock_logger.assert_any_call(
        INFO_LOG_LEVEL, "Submitting batch file: /path/to/mock_file.csv"
    )
    mock_open_file.assert_called_once_with("/path/to/mock_file.csv", "rb")


@patch("etl.update_zipcodes_from_census_bureau.execute_db_query", return_value=1)
@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
def test_update_property_zipcodes_with_geocoder_response_success(mock_logger, mock_execute_query):
    """Test the successful update of property ZIP codes."""
    parsed_response = [{"property_id": "1", "zip_code": "12345"}]
    number_updated = update_property_zipcodes_with_geocoder_response(parsed_response)
    assert number_updated == 1

    # Normalize the expected and actual queries for consistent comparison
    expected_query = normalize_query("UPDATE properties SET address_zip = ? WHERE id = ?")
    actual_query = normalize_query(mock_execute_query.call_args[0][0])

    assert actual_query == expected_query
    mock_execute_query.assert_called_once_with(
        mock_execute_query.call_args[0][0],
        ("12345", "1"),
        fetch_results=False
    )
    mock_logger.assert_called_once_with(
        INFO_LOG_LEVEL, "Updated 1 properties with ZIP codes."
    )


@patch("etl.update_zipcodes_from_census_bureau.execute_db_query", side_effect=Exception("Mocked DB error"))
@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
def test_update_property_zipcodes_with_geocoder_response_failure(mock_logger, mock_execute_query):
    """Test the handling of database update errors."""
    parsed_response = [{"property_id": "1", "zip_code": "12345"}]
    number_updated = update_property_zipcodes_with_geocoder_response(parsed_response)
    assert number_updated == 0
    mock_logger.assert_called_once_with(
        WARNING_LOG_LEVEL, "Error updating property ZIP codes: Mocked DB error"
    )


@patch("etl.update_zipcodes_from_census_bureau.os.path.exists", return_value=False)
@patch("etl.update_zipcodes_from_census_bureau.custom_logger")
def test_update_null_zipcodes_workflow_no_db(mock_logger, mock_path_exists):
    """Test workflow exits when the database file does not exist."""
    update_null_zipcodes_workflow()
    mock_logger.assert_any_call(
        WARNING_LOG_LEVEL, "No database file found, exiting workflow."
    )
