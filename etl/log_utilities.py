from prefect.context import get_run_context
from prefect import get_run_logger


CRITICAL_LOG_LEVEL = "critical"
ERROR_LOG_LEVEL = "error"
WARNING_LOG_LEVEL = "warning"
INFO_LOG_LEVEL = "info"
DEBUG_LOG_LEVEL = "debug"
ALLOWED_LOG_LEVELS = [
    CRITICAL_LOG_LEVEL,
    ERROR_LOG_LEVEL,
    WARNING_LOG_LEVEL,
    INFO_LOG_LEVEL,
    DEBUG_LOG_LEVEL
]


def custom_logger(level:str, message: str):
    """
    Log any message using Prefect's logger in a Prefect flow/task context.
    Fall back to print when outside of Prefect, for local debugging.
    Level must be one of the allowed log levels, defined in ALLOWED_LOG_LEVELS.
    Default level is INFO.
    """
    if level not in ALLOWED_LOG_LEVELS:
        raise ValueError(f"Invalid log level: {level} Message: {message}")

    try:
        # Check if running in a Prefect context, works only when running in a Prefect flow/task
        context = get_run_context()
        logger = get_run_logger()

        if level == CRITICAL_LOG_LEVEL:
            logger.critical(message)
        elif level == ERROR_LOG_LEVEL:
            logger.error(message)
        elif level == WARNING_LOG_LEVEL:
            logger.warning(message)
        elif level == DEBUG_LOG_LEVEL:
            logger.debug(message)
        else:
            logger.info(message)
    except RuntimeError:
        print(message)
