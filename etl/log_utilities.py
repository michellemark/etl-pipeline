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
