from unittest.mock import patch, MagicMock

import pytest
from prefect import task, flow
from prefect.testing.utilities import prefect_test_harness

from etl.log_utilities import custom_logger, ALLOWED_LOG_LEVELS


def test_custom_logger_local_context():
    """Test that custom_logger falls back to print when not in a Prefect context."""
    with patch("builtins.print") as mock_print:
        local_message = "This is a local message."
        custom_logger("info", local_message)
        mock_print.assert_called_once_with(local_message)


@pytest.mark.parametrize("log_level", ALLOWED_LOG_LEVELS)
def test_custom_logger_prefect_context_allowed_levels(log_level
                                            ):
    """Test that custom_logger uses Prefect's logger when in a Prefect context."""
    prefect_message = "This is a Prefect message."

    with prefect_test_harness():

        with patch("etl.log_utilities.get_run_logger") as mock_get_run_logger:
            mock_logger = MagicMock()
            mock_get_run_logger.return_value = mock_logger
            log_method = getattr(mock_logger, log_level)

            @task
            def task_with_custom_logger():
                # Call custom logger inside a task to ensure a valid context
                custom_logger(log_level, prefect_message)

            @flow
            def test_logging_flow():
                task_with_custom_logger()

            test_logging_flow()
            log_method.assert_called_once_with(prefect_message)


def test_custom_logger_invalid_log_level():
    """Test that custom_logger raises a ValueError for an invalid log level."""
    invalid_log_level = "invalid_level"
    message = "This is a test message."

    # Assert that a ValueError is raised with the expected message
    with pytest.raises(ValueError, match=f"Invalid log level: {invalid_log_level} Message: {message}"):
        custom_logger(invalid_log_level, message)
