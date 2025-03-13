import json
import os
from typing import Dict

from etl.constants import INFO_LOG_LEVEL
from etl.constants import PROPERTIES_TABLE
from etl.constants import WARNING_LOG_LEVEL
from etl.constants import ZIPCODE_CACHE_LOCAL_PATH
from etl.db_utilities import download_zipcodes_cache_from_s3
from etl.db_utilities import execute_db_query
from etl.log_utilities import custom_logger


def get_zipcodes_cache_as_json() -> dict | None:
    """
    Download zipcodes cache from S3 bucket to local path, and if successful,
    return all values as a JSON dictionary, else empty dictionary.
    """
    zipcodes_cache = {}

    try:
        # Always download incase there is an updated cache file
        download_zipcodes_cache_from_s3()

        if os.path.exists(ZIPCODE_CACHE_LOCAL_PATH):
            custom_logger(INFO_LOG_LEVEL, "Loading zipcodes cache from S3 as JSON...")

            with open(ZIPCODE_CACHE_LOCAL_PATH, "r") as zipcodes_file:
                zipcodes_cache = json.load(zipcodes_file)

    except (OSError, json.JSONDecodeError) as error:
        custom_logger(WARNING_LOG_LEVEL, f"Error loading zipcodes cache: {error}")

    return zipcodes_cache


def update_property_zipcodes_in_db_from_cache(zipcodes_cache: Dict[str, str]) -> int:
    """
    Update properties table with ZIP codes from zipcodes cache for properties
    where address_zip null or an empty string and return number of updated properties.

    :param cache: dict - Mapping of property IDs to ZIP codes.
    :return: int - Number of properties updated.
    """
    number_updated = 0
    query = f"""
    UPDATE {PROPERTIES_TABLE} 
    SET address_zip = ?
    WHERE id = ? 
    """

    for property_id, zipcode in zipcodes_cache.items():
        rowcount = execute_db_query(query, (zipcode, property_id), fetch_results=False)

        if rowcount:
            number_updated += rowcount

    return number_updated
