"""
Downloaded GeoJSON file of NY addresses from https://batch.openaddresses.io/job/554779#map=0/0/0
Then filtered to only include addresses for a CNY county and saved it in a new JSON file.
This was then manually uploaded to s3, as even filtered it is too large to want to check into the repo.
Sadly this only resulted in 13536 new zipcodes, but it's something more.
"""
import json
import os

import ijson
from botocore.exceptions import ClientError

from etl.constants import DB_LOCAL_PATH
from etl.constants import EXTRACTED_DATA_DIR
from etl.constants import INFO_LOG_LEVEL
from etl.constants import PROPERTIES_TABLE
from etl.constants import S3_BUCKET_NAME
from etl.constants import WARNING_LOG_LEVEL
from etl.constants import ZIPCODE_CACHE_KEY
from etl.constants import ZIPCODE_CACHE_LOCAL_PATH
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import execute_db_query
from etl.db_utilities import get_s3_client
from etl.db_utilities import upload_database_to_s3
from etl.db_utilities import upload_zipcodes_cache_to_s3
from etl.log_utilities import custom_logger
from etl.update_zipcodes_from_cache import get_zipcodes_cache_as_json

GEOJSON_FILE_NAME = "open_addresses_filtered_ny_addresses.geojson"
GEOJSON_FILE_PATH = os.path.join(EXTRACTED_DATA_DIR, GEOJSON_FILE_NAME)
geojson_data = None

# First see if we already have a database in s3 to add updated data to
download_database_from_s3()

# If we do not have a database file then nothing to do
if not os.path.exists(DB_LOCAL_PATH):
    custom_logger(WARNING_LOG_LEVEL, "No database file found, exiting workflow.")
else:
    custom_logger(INFO_LOG_LEVEL, "Database file found, proceeding with workflow.")

    # Get current zipcode cache from S3 or an empty dict so we can update / create one
    zipcode_cache = get_zipcodes_cache_as_json()
    s3_client = get_s3_client()

    if s3_client:
        custom_logger(INFO_LOG_LEVEL, f"Looking for zipcodes matching property records in GeoJSON data...")

        # Fetch address_street and municipality_name from properties
        query = f"SELECT id, address_street, municipality_name FROM {PROPERTIES_TABLE}"
        properties_data = execute_db_query(query, fetch_results=True)

        # Create a quick lookup dictionary from fetched properties data
        properties_dict = {
            (address_street.strip().lower(), municipality_name.strip().lower()): prop_id
            for prop_id, address_street, municipality_name in properties_data
        }

        count_matches = 0
        existing_value_matches = 0
        discrepancy_count = 0
        new_zipcodes_found = 0

        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=GEOJSON_FILE_NAME)
        except ClientError as e:
            custom_logger(INFO_LOG_LEVEL, f"Failed to fetch file from S3: {e}")
        else:

            # "response['Body']" is a streaming object, compatible with ijson
            # Save memory by not loading this large file all at once but streaming it
            geojson_data = ijson.items(response['Body'], 'item')

            for entry in geojson_data:
                props = entry.get('properties', {})
                street_number = props.get('number', '').strip()
                street_name = props.get('street', '').strip()
                city = props.get('city', '').strip().lower()
                postcode = props.get('postcode', '').strip()

                if not street_number or not street_name or not city or not postcode:
                    # Skip incomplete records
                    continue

                full_geojson_street = f"{street_number} {street_name}".lower()
                prop_key = (full_geojson_street, city)

                if prop_key in properties_dict:
                    prop_id = properties_dict[prop_key]
                    count_matches += 1
                    current_value = zipcode_cache.get(prop_id)

                    if current_value:
                        if current_value == postcode:
                            existing_value_matches += 1

                            # Zipcode already known, save the update query
                            continue
                        else:
                            custom_logger(
                                INFO_LOG_LEVEL,
                                f"Matched property {prop_id} has discrepancy with existing zipcode {current_value}, updating to {postcode}.")
                            discrepancy_count += 1
                    else:
                        custom_logger(INFO_LOG_LEVEL, f"Updating property {prop_id} with geo zipcode {postcode}")
                        new_zipcodes_found += 1

                    update_query = f"""
                    UPDATE {PROPERTIES_TABLE} 
                    SET address_zip = ?
                    WHERE id = ?
                    """
                    print(f"Query Params: postcode={postcode}, prop_id={prop_id}")
                    execute_db_query(update_query, params=(postcode, prop_id), fetch_results=False)
                    zipcode_cache[prop_id] = postcode

        # Update zipcode cache and upload this and updated database to s3
        try:

            with open(ZIPCODE_CACHE_LOCAL_PATH, "w") as cache_file:
                json.dump(zipcode_cache, cache_file, indent=4)

        except Exception as error:
            custom_logger(WARNING_LOG_LEVEL, f"Error saving or uploading zipcodes cache: {error}")
        else:
            upload_zipcodes_cache_to_s3()
            upload_database_to_s3()

        custom_logger(INFO_LOG_LEVEL, f"Total number of matches: {count_matches}")
        custom_logger(INFO_LOG_LEVEL, f"Existing zipcodes matched and so not updated: {existing_value_matches}")
        custom_logger(INFO_LOG_LEVEL, f"Discrepancies with existing zipcodes updated: {discrepancy_count}")
        custom_logger(INFO_LOG_LEVEL, f"New zipcodes added: {new_zipcodes_found}")
