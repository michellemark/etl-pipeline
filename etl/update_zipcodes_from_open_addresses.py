"""
Downloaded GeoJSON file of NY addresses from https://batch.openaddresses.io/job/554779#map=0/0/0
Then filtered to only include addresses for a CNY county and saved it in a new JSON file.
This was then manually uploaded to s3, as even filtered it is too large to want to check into the repo.
Sadly this only resulted in 13536 new zipcodes, but it's something more.
"""
import json
import os

from etl.constants import EXTRACTED_DATA_DIR
from etl.constants import INFO_LOG_LEVEL
from etl.constants import PROPERTIES_TABLE
from etl.constants import S3_BUCKET_NAME
from etl.constants import WARNING_LOG_LEVEL
from etl.db_utilities import ensure_data_directories_exist
from etl.db_utilities import execute_db_query
from etl.db_utilities import get_s3_client
from etl.log_utilities import custom_logger


GEOJSON_FILE_NAME = "open_addresses_filtered_ny_addresses.geojson"
GEOJSON_FILE_PATH = os.path.join(EXTRACTED_DATA_DIR, GEOJSON_FILE_NAME)

s3_client = get_s3_client()

if s3_client:
    ensure_data_directories_exist()

    try:
        s3_client.download_file(
            Bucket=S3_BUCKET_NAME,
            Key=GEOJSON_FILE_NAME,
            Filename=GEOJSON_FILE_PATH
        )
        custom_logger(
            INFO_LOG_LEVEL,
            f"Successfully downloaded {GEOJSON_FILE_NAME} from s3://{S3_BUCKET_NAME}/{GEOJSON_FILE_NAME} to {GEOJSON_FILE_PATH}")
    except Exception as ex:
        custom_logger(
            WARNING_LOG_LEVEL,
            f"Failed to download database from S3: {ex}")

GEOJSON_FILE_PATH = os.path.join(EXTRACTED_DATA_DIR, GEOJSON_FILE_NAME)

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

# Parse filtered GeoJSON file and update database
with open(GEOJSON_FILE_PATH, 'r', encoding='utf-8') as geofile:
    all_entries = json.load(geofile)

    # Each line is a JSON object but not the whole file
    for entry in all_entries:
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
            print(f"Matched property {prop_id} with zipcode {postcode}")

            # Could be more efficient not doing select and keeping stats, should need arise.
            # For now only finding 13536 matches, all new and I know that because of this code
            select_query = f"SELECT address_zip FROM {PROPERTIES_TABLE} WHERE id = '{prop_id}'"
            current_value_result = execute_db_query(select_query, fetch_results=True)
            current_value = current_value_result[0] if current_value_result else None
            update_query = f"UPDATE {PROPERTIES_TABLE} SET address_zip = '{postcode}' WHERE id = '{prop_id}'"

            # Keep some stats on change being made before we update
            if current_value:  # Existing value present
                print(f"Existing ZIP: {current_value}, Newly Found ZIP: {postcode}")
                if current_value == postcode:
                    existing_value_matches += 1
                else:
                    discrepancy_count += 1
            else:
                new_zipcodes_found += 1

            execute_db_query(update_query, fetch_results=False)

print(f"Total number of matches: {count_matches}")
print(f"Total Existing ZIP matches: {existing_value_matches}")
print(f"Total ZIP discrepancies found: {discrepancy_count}")
print(f"Total new ZIP codes populated (previously empty): {new_zipcodes_found}")
