import os
import sqlite3
from unittest.mock import patch

import pytest

from etl.constants import ERROR_LOG_LEVEL
from etl.db_utilities import execute_select_query

test_db_path = "test_database.db"
test_table_name = "test_table"


@pytest.fixture
def setup_database():
    """Create a test database and return its connection, clean up after testing."""
    with patch("etl.db_utilities.DB_LOCAL_PATH", test_db_path):
        with sqlite3.connect(test_db_path) as connection:
            cursor = connection.cursor()
            cursor.execute(f"""
                CREATE TABLE {test_table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL
                )
            """)
            cursor.executemany(
                f"""INSERT INTO {test_table_name} (id, name, age) VALUES (?, ?, ?)""",
                [
                    (1234, "Marty", 42),
                    (5678, "Mary", 34)
                ]
            )
            connection.commit()

            yield

    if os.path.exists(test_db_path):
        os.remove(test_db_path)


@patch("etl.db_utilities.custom_logger")
def test_execute_select_query_success_no_params(mock_custom_logger, setup_database):
    query = f"SELECT * FROM {test_table_name}"
    result = execute_select_query(query)
    assert result == [(1234, 'Marty', 42), (5678, 'Mary', 34)]
    mock_custom_logger.assert_not_called()


@patch("etl.db_utilities.custom_logger")
def test_execute_select_query_success_with_params(mock_custom_logger, setup_database):
    query = f"SELECT * FROM {test_table_name} WHERE name = ?"
    params = ("Mary",)
    result = execute_select_query(query, params)
    assert result == [(5678, 'Mary', 34)]
    mock_custom_logger.assert_not_called()


@patch("etl.db_utilities.sqlite3.connect", autospec=True)
@patch("etl.db_utilities.custom_logger")
def test_execute_select_query_handles_sqlite_error(mock_custom_logger, mock_connect, setup_database):
    mock_connection = mock_connect.return_value
    mock_enter_connection = mock_connection.__enter__.return_value
    mock_cursor = mock_enter_connection.cursor.return_value
    mock_cursor.execute.side_effect = sqlite3.Error("Simulated error")
    query = f"SELECT * FROM {test_table_name}"
    result = execute_select_query(query)
    mock_connect.assert_called_once_with(test_db_path)
    assert result is None
    mock_custom_logger.assert_called_once_with(
        ERROR_LOG_LEVEL,
        f"Query {query} failed, database error: Simulated error."
    )


@patch("etl.db_utilities.custom_logger")
def test_execute_select_query_no_results(mock_custom_logger, setup_database):
    query = f"SELECT * FROM {test_table_name} WHERE name = ?"
    params = ("Henry",)
    result = execute_select_query(query, params)
    assert result == []
    mock_custom_logger.assert_not_called()
