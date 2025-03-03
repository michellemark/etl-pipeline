import os
from datetime import datetime

from etl.constants import DESIRED_PROPERTY_CATEGORIES
from etl.constants import MINIMUM_ASSESSMENT_YEAR
from etl.constants import OPEN_NY_PROPERTY_CLASS_MAP
from etl.constants import OTHER_PROPERTY_CATEGORY
from etl.constants import WARNING_LOG_LEVEL
from etl.log_utilities import custom_logger


def get_assessment_year_to_query():
    """
    Get the rate year to query for all data to import.
    If the current date is August 1st or later, return the current year.
    Else return the previous year.

    NY only re-assesses for tax purposes once a year.  That process does not fully complete
    until the beginning of July, then municipalities can take time getting final data published.
    The lowest rate year to be queried or possibly returned is 2024 as this is being
    written in for first run in 2025 before new data is available.
    """
    current_year = datetime.now().year
    current_month = datetime.now().month

    if current_month >= 8 or current_year == MINIMUM_ASSESSMENT_YEAR:
        rate_year = current_year
    else:
        rate_year = current_year - 1

    return rate_year


def get_ny_property_category_for_property_class(property_class: int):
    """
    Look up property category for matching property_class in OPEN_NY_PROPERTY_CLASS_MAP.
    """
    return_property_category = OTHER_PROPERTY_CATEGORY

    for item in OPEN_NY_PROPERTY_CLASS_MAP:

        if item.get("property_class") == property_class:
            return_property_category = item.get("property_category", OTHER_PROPERTY_CATEGORY)
            break

    return return_property_category


def get_ny_property_classes_for_where_clause() -> str:
    """
    Create a string of property_class values to use in a WHERE clause when calling
    Open NY property assessment API.  Include all property classes with a
    property_category value in DESIRED_PROPERTY_CATEGORIES, comma separated and in quotes.
    Example Return:
        'property_class IN (\"190\", \"200\", \"210\"...)'
    """
    where_clause = ""
    desired_property_classes = [x.get("property_class") for x in OPEN_NY_PROPERTY_CLASS_MAP if
                                x.get("property_category") in DESIRED_PROPERTY_CATEGORIES]

    if desired_property_classes:
        property_class_where = ", ".join(f"\"{pcl}\"" for pcl in desired_property_classes)
        where_clause = f"property_class IN ({property_class_where})"

    return where_clause


def get_open_ny_app_token() -> str or None:
    app_token = os.environ.get('OPEN_DATA_APP_TOKEN')

    if not app_token:
        custom_logger(WARNING_LOG_LEVEL, "Missing OPEN_DATA_APP_TOKEN environment variable.")

    return app_token
