import os
from unittest.mock import patch

import pytest
import sqlite3

from etl.db_utilities import insert_into_database
from etl.log_utilities import ERROR_LOG_LEVEL, INFO_LOG_LEVEL

test_column_names = ["id", "name", "age"]
test_db_path = "test_database.db"
test_table_name = "test_table"


@pytest.fixture
def setup_database(monkeypatch):
    """Create a test database and return its connection, clean up after testing."""
    with patch("etl.db_utilities.db_local_path", test_db_path):
        with sqlite3.connect(test_db_path) as connection:
            cursor = connection.cursor()
            cursor.execute(f"""
                CREATE TABLE {test_table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL
                )
            """)
            connection.commit()

            yield

    if os.path.exists(test_db_path):
        os.remove(test_db_path)


def get_data_in_test_database():
    """Helper function to retrieve data from the test database."""
    with sqlite3.connect(test_db_path) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {test_table_name}")
        rows = cursor.fetchall()

        return rows

def test_successful_insertion(setup_database):
    """Test inserting valid data into the table."""
    data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35)
    ]
    rows_inserted, rows_failed = insert_into_database(test_table_name, test_column_names, data)
    assert rows_inserted == 3
    assert rows_failed == 0

    rows = get_data_in_test_database()
    assert len(rows) == 3
    assert rows == data


def test_partial_failures(setup_database):
    """Test inserting rows where one fails due to a duplicate primary key."""
    data = [
        (1, "Alice", 30),
        (1, "Duplicate", 40),  # duplicate primary key
        (2, "Bob", 25)
    ]
    rows_inserted, rows_failed = insert_into_database(test_table_name, test_column_names, data)
    assert rows_inserted == 2
    assert rows_failed == 1

    rows = get_data_in_test_database()
    assert len(rows) == 2
    assert (1, "Alice", 30) in rows
    assert (2, "Bob", 25) in rows


def test_empty_data(setup_database):
    """Test calling the function with an empty data list."""
    data = []
    rows_inserted, rows_failed = insert_into_database(test_table_name, test_column_names, data)
    assert rows_inserted == 0
    assert rows_failed == 0
    rows = get_data_in_test_database()
    assert len(rows) == 0


def test_invalid_table_name(setup_database):
    """Test using a non-existent table."""
    table_name = "non_existent_table"
    data = [
        (1, "Alice", 30)
    ]
    rows_inserted, rows_failed = insert_into_database(table_name, test_column_names, data)
    assert rows_inserted == 0
    assert rows_failed == len(data)
    rows = get_data_in_test_database()
    assert len(rows) == 0


def test_generic_database_error():
    """Simulate a database-level error and ensure it is logged."""
    data = [(1, "Alice", 30)]

    # Simulate an error in the execute function
    with patch("etl.db_utilities.sqlite3.connect", autospec=True) as mock_connect, \
        patch("etl.db_utilities.db_local_path", test_db_path), \
        patch("etl.db_utilities.custom_logger") as mock_logger:
        mock_connection = mock_connect.return_value
        mock_enter_connection = mock_connection.__enter__.return_value
        mock_cursor = mock_enter_connection.cursor.return_value
        mock_cursor.execute.side_effect = sqlite3.Error("Simulated error")
        rows_inserted, rows_failed = insert_into_database(test_table_name, test_column_names, data)

        mock_connect.assert_called_once_with(test_db_path)
        mock_enter_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(
            'INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)',
            data[0]
        )
        mock_logger.assert_any_call(
            ERROR_LOG_LEVEL,
            f"Row 1 failed to insert due to a general database error: Simulated error. Row data: {data[0]}"
        )
        mock_logger.assert_any_call(INFO_LOG_LEVEL, "rows_inserted: 0, rows_failed: 1")
        assert rows_inserted == 0
        assert rows_failed == 1
