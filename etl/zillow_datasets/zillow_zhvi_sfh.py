import csv
from datetime import datetime
from io import StringIO
from typing import Optional

import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from etl.constants import ALL_PROPERTIES_STATE
from etl.constants import CNY_COUNTY_LIST
from etl.constants import INFO_LOG_LEVEL
from etl.constants import WARNING_LOG_LEVEL
from etl.db_utilities import insert_or_replace_into_database
from etl.log_utilities import custom_logger
from etl.validation_models import ZillowHomeValueIndexSFHCity


ZILLOW_DATA_PAGE_URL = "https://www.zillow.com/research/data/"
ZILLOW_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.zillow.com/',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}
TIMEOUT_SECONDS = 30


def get_current_download_url(session=None) -> Optional[str]:
    """
    Use BeautifulSoup to scrape the current URL from the free Zillow data page
    located at ZILLOW_DATA_PAGE_URL.
    When select with id median-home-value-zillow-home-value-index-zhvi-dropdown-1 has
    option with value ZHVI Single-Family Homes Time Series ($) selected and
    select with id median-home-value-zillow-home-value-index-zhvi-dropdown-2 has
    option with value City selected return download URL or None if not found.

    :return: str or None: The URL to download the data if found, None otherwise.
    """
    download_url = None

    try:
        # Use session, if passed, to ease testing and set headers to mimic a browser request to avoid 403 error
        session = session or requests
        response = session.get(ZILLOW_DATA_PAGE_URL, headers=ZILLOW_HEADERS, timeout=TIMEOUT_SECONDS)

        if response.ok:
            soup = BeautifulSoup(response.content, 'html.parser')
            dropdown2 = soup.find('select', id='median-home-value-zillow-home-value-index-zhvi-dropdown-2')

            if dropdown2:
                city_option = None

                for option in dropdown2.find_all('option'):
                    if option.text.strip() == 'City':
                        city_option = option
                        break

                if city_option and city_option.has_attr('value'):
                    download_url = city_option['value']

                if not download_url.startswith(('https://')):
                    custom_logger(
                        WARNING_LOG_LEVEL,
                        f"Download URL doesn't appear to be valid: {download_url}"
                    )
                    download_url = None

        else:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed fetching Zillow data page. Status code: {response.status_code}. Error: {response.text}"
            )

    except requests.RequestException as e:
        custom_logger(
            WARNING_LOG_LEVEL,
            f"Network error when accessing Zillow data page: {str(e)}"
        )
    except Exception as e:
        custom_logger(
            WARNING_LOG_LEVEL,
            f"Unexpected error when scraping Zillow data URL: {str(e)}"
        )

    return download_url


def parse_csv_row_into_valid_location_for_db(csv_row: dict) -> Optional[dict]:
    """Parse CSV data row and, if valid return a dict with all location values as needed to save a db record, else None."""
    validated_record = None

    try:
        model = ZillowHomeValueIndexSFHCity(**csv_row)
    except ValidationError as err:
        custom_logger(
            WARNING_LOG_LEVEL,
            f"Failed to validate zillow csv data row:")

        for error in err.errors():
            custom_logger(
                WARNING_LOG_LEVEL,
                f"- Error: Field: {error["loc"][0]}. Message: {error["msg"]}")
    else:
        validated_record = {
            "municipality_name": model.RegionName,
            "county_name": model.generate_county_name(),
            "state": model.State
        }

    return validated_record


def is_cny_county(validated_location: dict) -> bool:
    """Return True if county name and state are a Central New York county to save to database, else False."""
    return validated_location["county_name"] in CNY_COUNTY_LIST and validated_location["state"] == ALL_PROPERTIES_STATE


def prepare_db_records(csv_row: dict, validated_location: dict) -> list[tuple]:
    """
    Transform validated cny records into database-ready list of tuples to store for each date and related data point.
    If no dates with data return an empty list.
    """
    db_records = []

    # Csv dict reader creates a dictionary with each key being a heading.  Find all keys that are dates.
    for column, value in csv_row.items():

        try:
            date = datetime.strptime(column.strip(), "%Y-%m-%d")
        except ValueError:
            # If parsing fails not a value to store, skip to next column
            continue
        else:
            # Store date as YYYY-MM
            store_date = f"{date.year}-{date.month:02d}"

            # Store values as floats for ease of data analysis
            data_value = float(value) if value else None

            if data_value:
                db_records.append((
                    validated_location["municipality_name"],
                    validated_location["county_name"],
                    validated_location["state"],
                    store_date,
                    data_value,
                ))

    return db_records


def get_free_zillow_zhvi_sfh(session=None):
    data_description = "Zillow Home Value Index Single Family Homes"

    custom_logger(
        INFO_LOG_LEVEL,
        f"Fetching {data_description}..."
    )
    current_url = get_current_download_url()

    if current_url:
        # Use session, if passed, to ease testing
        session = session or requests
        response = session.get(current_url)

        if response.ok:
            reader = csv.DictReader(StringIO(response.text))
            num_records_stored = 0

            for row in reader:
                validated_record_location = parse_csv_row_into_valid_location_for_db(row)
                is_cny_record = is_cny_county(validated_record_location)

                if is_cny_record:
                    records_to_store = prepare_db_records(row, validated_record_location)

                    if records_to_store:
                        rows_inserted, rows_failed = insert_or_replace_into_database(
                            table_name="zillow_home_value_index_sfh",
                            column_names=["municipality_name", "county_name", "state", "date", "home_value_index"],
                            data=records_to_store
                        )
                        num_records_stored += rows_inserted

            custom_logger(
                INFO_LOG_LEVEL,
                f"Completed fetching {data_description} and stored {num_records_stored} valid records.")
        else:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed fetching {data_description}. Status code: {response.status_code}. Error: {response.text}"
            )
