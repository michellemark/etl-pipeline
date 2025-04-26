import json
import os
from typing import Dict

from etl.constants import INFO_LOG_LEVEL
from etl.constants import PROPERTIES_TABLE
from etl.constants import WARNING_LOG_LEVEL
from etl.constants import ZIPCODE_CACHE_LOCAL_PATH
from etl.db_utilities import download_zipcodes_cache_from_s3
from etl.db_utilities import execute_db_query
from etl.db_utilities import upload_zipcodes_cache_to_s3
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


def update_zipcode_cache(zipcode_cache: dict) -> dict:
    """
    Update the zipcode cache with the latest zipcodes from the properties table.
    Updates the local cache file and uploads to S3.

    :param zipcode_cache: dict - The current zipcode cache loaded as a JSON object.
    :return: dict - The updated zipcode cache with the latest zipcodes from the properties table.
    """
    try:
        query = f"SELECT id, address_zip FROM {PROPERTIES_TABLE} WHERE address_zip IS NOT NULL"
        results = execute_db_query(query, fetch_results=True)

        if results:
            custom_logger(INFO_LOG_LEVEL, f"Fetched {len(results)} properties with zipcodes from the database.")

        for property_id, zipcode in results:
            zipcode_cache[str(property_id)] = zipcode

        custom_logger(INFO_LOG_LEVEL, "Updated the zipcode cache with recently fetched property zipcodes.")

        with open(ZIPCODE_CACHE_LOCAL_PATH, "w") as cache_file:
            json.dump(zipcode_cache, cache_file, indent=4)
            custom_logger(INFO_LOG_LEVEL, f"Updated the local zipcode cache file at {ZIPCODE_CACHE_LOCAL_PATH}.")

        upload_zipcodes_cache_to_s3()

    except Exception as e:
        custom_logger(WARNING_LOG_LEVEL, f"Failed to update the zipcode cache: {e}")

    return zipcode_cache
