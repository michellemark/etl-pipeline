from time import time
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from etl.constants import WARNING_LOG_LEVEL
from etl.rate_limits import rate_per_minute


@patch("etl.rate_limits.storage_backend")
def test_rate_per_minute_allows_execution(mock_storage_backend):
    """Test that the decorator allows function execution under the rate limit."""
    mock_storage_backend.incr.return_value = True
    mock_function = MagicMock(return_value="success")
    mock_function.__name__ = "mock_function"

    decorated_function = rate_per_minute(10)(mock_function)

    result = decorated_function()

    mock_function.assert_called_once()
    assert result == "success"


@patch("etl.rate_limits.storage_backend")
@patch("etl.rate_limits.time.sleep", return_value=None)
@patch("etl.rate_limits.custom_logger")
def test_rate_per_minute_exceeded_limit(mock_logger, mock_sleep, mock_storage_backend):
    """Test that the decorator handles rate limit exceeded by waiting and logging."""

    # Rate limit exceeded first try, not the next
    mock_storage_backend.incr.side_effect = [False, True]
    mock_expiry_time = time() + 4
    mock_storage_backend.get_expiry.return_value = mock_expiry_time
    mock_function = MagicMock(return_value="success")
    mock_function.__name__ = "mock_function"

    decorated_function = rate_per_minute(10)(mock_function)
    result = decorated_function()

    # Calculate expected sleep time dynamically based on mocked expiry time
    expected_sleep_time = max(0, int(mock_expiry_time - time()))

    mock_function.assert_called_once()
    mock_sleep.assert_called_once_with(expected_sleep_time)
    mock_logger.assert_called_with(WARNING_LOG_LEVEL, f"Rate limit exceeded. Waiting {expected_sleep_time} seconds.")
    assert result == "success"


@patch("etl.rate_limits.storage_backend")
def test_rate_per_minute_raises_exception(mock_storage_backend):
    """
    Test that the decorator raises and propagates exceptions from the wrapped function.
    """
    mock_storage_backend.incr.return_value = True
    mock_function = MagicMock(side_effect=ValueError("Simulated exception"))
    mock_function.__name__ = "mock_function"

    decorated_function = rate_per_minute(10)(mock_function)

    with pytest.raises(ValueError, match="Simulated exception"):
        decorated_function()

    mock_function.assert_called_once()
