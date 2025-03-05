from etl.constants import *


def custom_logger(level:str, message: str):
    """
    Enhanced print to log with GitHub Actions annotations.
    """
    if level == ERROR_LOG_LEVEL:
        print(f"::error::{message}")
    elif level == WARNING_LOG_LEVEL:
        print(f"::warning::{message}")
    else:
        print(f"::notice::{message}")


def log_retry(details):
    """Callback function for backoff to log retry attempts"""
    custom_logger(
        INFO_LOG_LEVEL,
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )
