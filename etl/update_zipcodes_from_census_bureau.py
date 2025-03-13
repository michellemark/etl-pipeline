import json
import os
import re
from csv import DictWriter
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests
from backoff import expo
from backoff import on_exception

from etl.constants import DB_LOCAL_PATH
from etl.constants import EXTRACTED_DATA_DIR
from etl.constants import INFO_LOG_LEVEL
from etl.constants import PROPERTIES_TABLE
from etl.constants import RETRYABLE_ERRORS
from etl.constants import US_CENSUS_BUREAU_BATCH_SIZE
from etl.constants import US_CENSUS_BUREAU_BATCH_URL
from etl.constants import US_CENSUS_BUREAU_CALLS_PER_PERIOD
from etl.constants import WARNING_LOG_LEVEL
from etl.constants import ZIPCODE_CACHE_LOCAL_PATH
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import ensure_data_directories_exist
from etl.db_utilities import execute_db_query
from etl.db_utilities import upload_database_to_s3
from etl.db_utilities import upload_zipcodes_cache_to_s3
from etl.log_utilities import custom_logger
from etl.log_utilities import log_retry
from etl.rate_limits import rate_per_minute
from etl.update_zipcodes_from_cache import get_zipcodes_cache_as_json
from etl.update_zipcodes_from_cache import update_property_zipcodes_in_db_from_cache


def get_csv_file_path() -> str:
    """Make csv file path with a timestamp.  Returns full path to file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    temp_file_name = f"zipcode_batch_{timestamp}.csv"

    return os.path.join(EXTRACTED_DATA_DIR, temp_file_name)


def create_csv_batch_file(data: List[Dict[str, Any]]) -> Optional[str]:
    """
    Write properties without ZIP codes as batch CSV file to send to geocoder batch api.
    Returns name of created CSV file or None if no csv written.
    """
    # batch_file_path = None

    try:

        if data:

            # Define CSV header columns in required order
            header = ["Unique ID", "Street address", "City", "State", "ZIP"]
            batch_file_path = get_csv_file_path()

            with open(batch_file_path, mode="w+", newline="", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=header)
                writer.writeheader()
                writer.writerows(data)

    except Exception as error:
        custom_logger(WARNING_LOG_LEVEL, f"Error creating csv {batch_file_path}: {error}")

    return batch_file_path


def sanitize_address_string(address: str) -> str:
    """
    Some characters in addresses cause geocoder errors.
    Remove any parentheses, and all text inside, from a given string.

    Example address 1: '1634 Clark St Rd (Parking/Residual)'
    Return 1: '1634 Clark St Rd'
    Example address 2: 'John St/Future Site'
    Return 2: 'John St - Future Site'
    Example address 3: '242/246 Clark St Rd'
    Return 3: '242 - 246 Clark St Rd'
    Example address 4: 'off Watkins Rd'
    Return 4: 'Watkins Rd'
    """
    substitutions = {
        r"\s*\(.*?\)": "",  # Remove anything in parentheses
        r"/": " - ",  # Replace slash with space dash space
        r"&": " - ",  # Replace ampersand with space dash space
        r"['\"]": "",  # Remove single and double quotes
        r"\+": "",  # Remove '+'
        r"^\s*[oO][fF]{2}\b": ""  # Remove 'off' (any case) at the start of a string
    }

    # Perform all substitutions
    for pattern, replacement in substitutions.items():
        address = re.sub(pattern, replacement, address)

    # Strip any trailing whitespace
    return address.strip()


def get_all_properties_needing_zipcodes_from_database_write_as_csv() -> List[str | None]:
    """
    Fetch properties without ZIP codes from database in batches, write each batch to
    a CSV file using `create_address_batch_file`, and return a list of file names.
    """
    batch_file_paths = []
    offset = 0
    batch_number = 1
    query = f"""
        SELECT id, address_street, municipality_name, address_state
        FROM {PROPERTIES_TABLE} 
        WHERE address_zip IS NULL OR address_zip = ''
        ORDER BY id
        LIMIT ? OFFSET ?
    """
    custom_logger(INFO_LOG_LEVEL, "Fetching properties without zipcodes...")

    while True:
        results = execute_db_query(
            query=query,
            params=(US_CENSUS_BUREAU_BATCH_SIZE, offset),
            fetch_results=True)

        # No more results to fetch
        if not results:
            break

        batch_data = []

        for row in results:
            # temp_unique_id = map_property_id_to_temp_unique_id(row[0])
            sanitized_street_address = sanitize_address_string(row[1])
            batch_data.append(
                {
                    "Unique ID": row[0],
                    "Street address": sanitized_street_address,
                    "City": row[2],
                    "State": row[3],
                    "ZIP": ""
                }
            )

        batch_file_path = create_csv_batch_file(batch_data)

        if batch_file_path:
            batch_file_paths.append(batch_file_path)
            offset += US_CENSUS_BUREAU_BATCH_SIZE
            custom_logger(
                INFO_LOG_LEVEL,
                f"Batch {batch_number} csv created with {len(batch_data)} properties at {batch_file_path}.")
            batch_number += 1

    return batch_file_paths


def parse_geocoder_response(raw_response: str) -> List[Dict]:
    """
    Parse the Census Geocoder response into a list of dictionaries, where each dictionary is a processed row.

    :param raw_response: str - The raw response string as returned by the Geocoder.
    :return: List[Dict]: A list of dictionaries, where each dictionary is a processed row.
    """
    parsed_rows = []
    custom_logger(INFO_LOG_LEVEL, "Parsing geocoder response...")

    try:
        # Split response into rows on line breaks
        lines = raw_response.strip().split("\n")

        for line in lines:

            # Split line into individual columns on commas
            columns = line.split(",")

            if len(columns) >= 11 and columns[5].strip('"') == "Match" and columns[6].strip('"') == "Exact":
                property_id = columns[0].strip('"')
                zip_code = columns[10].strip('"').split(",")[-1].strip()

                if property_id and zip_code:

                    # Map raw values to meaningful keys
                    parsed_rows.append({
                        "property_id": property_id,
                        "zip_code": zip_code
                    })

        custom_logger(INFO_LOG_LEVEL, f"Successfully parsed {len(parsed_rows)} rows with exact match out of {len(lines)} responses from geocoder response.")
    except Exception as error:
        custom_logger(WARNING_LOG_LEVEL, f"Error parsing geocoder response: {error}")

    return parsed_rows


@on_exception(
    expo,
    RETRYABLE_ERRORS,
    factor=20,
    max_tries=3,
    on_backoff=log_retry
)
@rate_per_minute(calls_per_minute=US_CENSUS_BUREAU_CALLS_PER_PERIOD)
def get_zipcodes_from_geocoder_as_batch(batch_file_path) -> Optional[str]:
    """
    Submit csv batch file to Census Bureau Geocoding API.
    Returns dictionary with job_id, result_url and batch_file (path to file sent) or None if there is an exception.
    """
    raw_response = None
    custom_logger(INFO_LOG_LEVEL, f"Submitting batch file: {batch_file_path}")

    with open(batch_file_path, "rb") as batch_file_obj:
        file_name = os.path.basename(batch_file_path)
        files = {
            "addressFile": (file_name, batch_file_obj, "text/csv")
        }
        form_data = {
            "benchmark": "Public_AR_Current"
        }
        response = requests.post(
            US_CENSUS_BUREAU_BATCH_URL,
            data=form_data,
            files=files,
            timeout=180
        )

    if response.ok:
        custom_logger(INFO_LOG_LEVEL, "Batch file submitted successfully. Parsing response...")
        raw_response = response.content.decode("utf-8")
    else:
        response.raise_for_status()

    return raw_response


def update_property_zipcodes_with_geocoder_response(parsed_geo_response: List[Dict[str, str]]) -> int:
    """Update properties in database with geocoded ZIP codes, return number of updated properties."""
    number_updated = 0

    try:
        query = f"""
        UPDATE {PROPERTIES_TABLE} 
        SET address_zip = ?
        WHERE id = ? 
        """

        for row in parsed_geo_response:
            property_id = row.get("property_id")
            zip_code = row.get("zip_code")

            if property_id and zip_code:
                rowcount = execute_db_query(query, (zip_code, property_id), fetch_results=False)

                if rowcount:
                    number_updated += rowcount

        custom_logger(INFO_LOG_LEVEL, f"Updated {number_updated} properties with ZIP codes.")
    except Exception as error:
        custom_logger(WARNING_LOG_LEVEL, f"Error updating property ZIP codes: {error}")

    return number_updated


def update_null_zipcodes_workflow():
    """Execute workflow for updating null property ZIP codes"""
    start_time = datetime.now()
    custom_logger(INFO_LOG_LEVEL, f"Starting update null zipcodes workflow at {start_time:%Y-%m-%d %H:%M:%S}")

    # First see if we already have a database in s3 to add updated data to
    download_database_from_s3()

    # If we do not have a database file then nothing to do
    if not os.path.exists(DB_LOCAL_PATH):
        custom_logger(WARNING_LOG_LEVEL, "No database file found, exiting workflow.")
    else:
        custom_logger(INFO_LOG_LEVEL, "Database file found, proceeding with workflow.")
        # Ensure any needed directories for generated files exist
        ensure_data_directories_exist()

        try:
            # Get current zipcode cache from S3 or an empty dict
            zipcode_cache = get_zipcodes_cache_as_json()

            # Redundant, given also done at end of etl pipeline, but balanced by potential to save even one batch call
            number_updated = update_property_zipcodes_in_db_from_cache(zipcode_cache)
            custom_logger(INFO_LOG_LEVEL, f"Updated {number_updated} zipcodes from cache.")

            batch_file_paths = get_all_properties_needing_zipcodes_from_database_write_as_csv()

            for batch_file_path in batch_file_paths:

                try:
                    raw_response = get_zipcodes_from_geocoder_as_batch(batch_file_path)
                except Exception as error:
                    custom_logger(
                        WARNING_LOG_LEVEL,
                        f"Error unable to get zips for batch file {batch_file_path}: {str(error)}")
                else:
                    if raw_response:
                        parsed_response = parse_geocoder_response(raw_response)
                        update_property_zipcodes_with_geocoder_response(parsed_response)

                        # Update the zipcode cache - even if save to db failed next time zips are loaded from cache we want these
                        for row in parsed_response:
                            property_id = row.get("property_id")
                            zip_code = row.get("zip_code")

                            if property_id and zip_code:
                                zipcode_cache[property_id] = zip_code

        except Exception as error:
            custom_logger(WARNING_LOG_LEVEL, f"Error in update null zipcodes workflow: {str(error)}")
        finally:

            if zipcode_cache:
                # Save updated zipcode cache back to local file and upload to s3
                try:

                    with open(ZIPCODE_CACHE_LOCAL_PATH, "w") as cache_file:
                        json.dump(zipcode_cache, cache_file, indent=4)

                except Exception as error:
                    custom_logger(WARNING_LOG_LEVEL, f"Error saving or uploading zipcodes cache: {error}")
                else:
                    # Upload updated cache to S3
                    upload_zipcodes_cache_to_s3()

                    # Upload database to s3 also
                    upload_database_to_s3()

        end_time = datetime.now()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        custom_logger(
            INFO_LOG_LEVEL,
            f"Total time taken: {int(hours)} hours, {int(minutes)} minutes, and {int(seconds)} seconds.")


if __name__ == "__main__":
    update_null_zipcodes_workflow()
