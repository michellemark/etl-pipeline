from limits import RateLimitItemPerMinute
from limits.storage import MemoryStorage
from functools import wraps
import time

from etl.constants import WARNING_LOG_LEVEL
from etl.log_utilities import custom_logger


storage_backend = MemoryStorage()


def rate_per_minute(calls_per_minute):
    """Decorator to enforce rate limits on API calls."""
    limit = RateLimitItemPerMinute(calls_per_minute)

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            # Use unique key for decorated function (e.g., based on function name)
            key = f"rate_limiter:{func.__name__}"

            try:

                # Increment rate limit
                if not storage_backend.incr(key=key, expiry=limit.get_expiry(), elastic_expiry=False):

                    # Exceeded d rate limit, calculate remaining time
                    time_to_reset = max(0, int(storage_backend.get_expiry(key) - time.time()))

                    # Instead of raising an exception, wait for limit to reset
                    custom_logger(WARNING_LOG_LEVEL, f"Rate limit exceeded. Waiting {time_to_reset} seconds.")
                    time.sleep(time_to_reset)

                # Proceed with function call
                return func(*args, **kwargs)

            except Exception as ex:
                custom_logger(WARNING_LOG_LEVEL, "Unexpected error rate limiting call: ", ex)

        return wrapper

    return decorator
