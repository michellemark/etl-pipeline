from unittest.mock import patch

from etl.constants import DEBUG_LOG_LEVEL
from etl.constants import ERROR_LOG_LEVEL
from etl.constants import INFO_LOG_LEVEL
from etl.constants import WARNING_LOG_LEVEL
from etl.log_utilities import custom_logger


def test_custom_logger_debug_default_annotations():
    """Test  custom_logger defaults to debug GitHub annotations."""
    with patch("builtins.print") as mock_print:
        message = "This is a message."
        custom_logger("", message)
        mock_print.assert_called_once_with(f"::debug::{message}")


def test_custom_logger_debug_annotations():
    """Test  custom_logger defaults to debug GitHub annotations."""
    with patch("builtins.print") as mock_print:
        message = "This is a message."
        custom_logger(INFO_LOG_LEVEL, message)
        mock_print.assert_any_call(f"::debug::{message}")
        custom_logger(DEBUG_LOG_LEVEL, message)
        mock_print.assert_any_call(f"::debug::{message}")


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
