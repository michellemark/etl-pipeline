from unittest.mock import patch

from etl.constants import DEBUG_LOG_LEVEL
from etl.constants import ERROR_LOG_LEVEL
from etl.constants import INFO_LOG_LEVEL
from etl.constants import WARNING_LOG_LEVEL
from etl.log_utilities import custom_logger
from etl.log_utilities import log_retry


def test_custom_logger_debug_default_annotations():
    """Test  custom_logger defaults to debug GitHub annotations."""
    with patch("builtins.print") as mock_print:
        message = "This is a message."
        custom_logger("", message)
        mock_print.assert_called_once_with(f"::notice::{message}")


def test_custom_logger_debug_annotations():
    """Test  custom_logger defaults to debug GitHub annotations."""
    with patch("builtins.print") as mock_print:
        message = "This is a message."
        custom_logger(INFO_LOG_LEVEL, message)
        mock_print.assert_any_call(f"::notice::{message}")
        custom_logger(DEBUG_LOG_LEVEL, message)
        mock_print.assert_any_call(f"::notice::{message}")


def test_custom_logger_error_annotations():
    """Test that custom_logger uses error GitHub annotations for ERROR_LOG_LEVEL."""
    with patch("builtins.print") as mock_print:
        message = "This is an error message."
        custom_logger(ERROR_LOG_LEVEL, message)
        mock_print.assert_called_once_with(f"::error::{message}")


def test_custom_logger_warning_annotations():
    """Test that custom_logger uses warning GitHub annotations for WARNING_LOG_LEVEL."""
    with patch("builtins.print") as mock_print:
        message = "This is an error message."
        custom_logger(WARNING_LOG_LEVEL, message)
        mock_print.assert_called_once_with(f"::warning::{message}")


def test_log_retry_logs_correct_message():
    """Test that log_retry logs the correct retry message using custom_logger."""
    retry_details = {
        "wait": 2.5,
        "tries": 3,
        "target": "mock_function",
        "args": ("arg1", "arg2"),
        "kwargs": {"key1": "value1"}
    }

    expected_message = (
        "Backing off 2.5 seconds after 3 tries "
        "calling function mock_function with args ('arg1', 'arg2') and kwargs {'key1': 'value1'}"
    )

    with patch("etl.log_utilities.custom_logger") as mock_logger:
        log_retry(retry_details)
        mock_logger.assert_called_once_with(INFO_LOG_LEVEL, expected_message)
