import json
from datetime import datetime
from typing import List

from pydantic import ValidationError
from ratelimit import limits
from ratelimit import sleep_and_retry
from sodapy import Socrata

from etl.constants import *
from etl.db_utilities import create_database
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import execute_select_query
from etl.db_utilities import insert_into_database
from etl.db_utilities import upload_database_to_s3
from etl.log_utilities import custom_logger

from etl.validation_models import MunicipalityAssessmentRatio


def get_open_ny_app_token() -> str or None:
    app_token = os.environ.get('OPEN_DATA_APP_TOKEN')

    if not app_token:
        custom_logger(ERROR_LOG_LEVEL, "Missing OPEN_DATA_APP_TOKEN environment variable.")

    return app_token


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


def check_if_county_assessment_ratio_exists(rate_year: int, county_name: str) -> bool:
    """
    County assessment ratios are unique by rate_year and county_name but do
    not change over time.  Use this to check if we already have the data and save a call.
    """
    custom_logger(
        INFO_LOG_LEVEL,
        f"Checking if assessment ratios for rate_year: {rate_year} and county_name: {county_name} exist in database...")
    do_county_ratios_for_year_exist = False
    sql_query = f"SELECT * FROM {ASSESSMENT_RATIOS_TABLE} WHERE rate_year=? AND county_name=?"
    results = execute_select_query(sql_query, params=(rate_year, county_name))

    if results:
        do_county_ratios_for_year_exist = True

    return do_county_ratios_for_year_exist


@sleep_and_retry
@limits(calls=OPEN_NY_CALLS_PER_PERIOD, period=OPEN_NY_RATE_LIMIT_PERIOD)
def fetch_county_assessment_ratios(app_token: str, rate_year: int, county_name: str) -> List[dict] or None:
    """Call Open NY APIs to fetch municipality assessment ratios for a given county and year using rate limiting."""
    assessment_ratios = None
    custom_logger(
        INFO_LOG_LEVEL,
        f"Fetching municipality assessment ratios for rate_year: {rate_year} and county_name: {county_name}")
    try:
        with Socrata(OPEN_NY_BASE_URL, app_token=app_token, timeout=60) as client:
            assessment_ratios = client.get(
                OPEN_NY_ASSESSMENT_RATIOS_API_ID,
                rate_year=rate_year,
                county_name=county_name
            )
    except Exception as err:
        custom_logger(
            ERROR_LOG_LEVEL,
            f"Failed fetching municipality assessment ratios for rate_year: {rate_year} and county_name: {county_name}. Error: {err}")

    return assessment_ratios


def fetch_municipality_assessment_ratios(app_token: str, query_year: int) -> List[dict]:
    """
    Fetch municipality_assessment_ratios from Open NY APIs for all counties
    in the CNY_COUNTY_LIST.  Will get assessment ratios for all years from
    2009 to present, where data is available.  If the data is already in the
    database will skip it as this data does not change over time.
    """
    assessment_ratios = []
    custom_logger(INFO_LOG_LEVEL, f"Starting fetching municipality assessment ratios for {query_year}...")

    for county in CNY_COUNTY_LIST:

        # Check if it exists before we call our rate limited function to speed up processing when we have the data
        already_exists = check_if_county_assessment_ratio_exists(query_year, county)

        if already_exists:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Found municipality assessment ratios for rate_year: {query_year} and county_name: {county}, skipping.")
        else:
            ratio_results = fetch_county_assessment_ratios(
                                app_token=app_token,
                                rate_year=query_year,
                                county_name=county)

            if ratio_results and isinstance(ratio_results, list):
                assessment_ratios.extend(ratio_results)

    custom_logger(INFO_LOG_LEVEL, f"Completed fetching municipality assessment ratios, {len(assessment_ratios)} found.")

    return assessment_ratios


def save_municipality_assessment_ratios(all_ratios: List[dict]):
    """Task 2: Validate the municipality_assessment_ratios data and save valid data to database."""
    validated_ratio_data = []
    column_names = None

    for municipality_assessment_ratio in all_ratios:
        try:
            model = MunicipalityAssessmentRatio(**municipality_assessment_ratio)
        except ValidationError as err:
            custom_logger(
                ERROR_LOG_LEVEL,
                f"Failed to validate municipality assessment ratio {municipality_assessment_ratio} Errors:")
            for error in err.errors():
                custom_logger(
                    ERROR_LOG_LEVEL,
                    f"Error in field {error["loc"][0]}. Message: {error["msg"]}")
        else:
            ratio_data = json.loads(model.model_dump_json(by_alias=True))
            validated_ratio_data.append(tuple(ratio_data.values()))

            if not column_names:
                column_names = list(ratio_data.keys())

    if validated_ratio_data:
        rows_inserted, rows_failed = insert_into_database(ASSESSMENT_RATIOS_TABLE, column_names, validated_ratio_data)
        custom_logger(
            INFO_LOG_LEVEL,
            f"Completed saving {len(validated_ratio_data)} valid municipality assessment ratios to database rows_inserted: {rows_inserted}, rows_failed: {rows_failed}.")
    else:
        custom_logger(
            INFO_LOG_LEVEL,
            "No valid municipality assessment ratios found, skipping.")


def cny_real_estate_etl_workflow():
    """Main entry point for the ETL workflow."""
    open_ny_token = get_open_ny_app_token()

    if not open_ny_token:
        custom_logger(ERROR_LOG_LEVEL, "Cannot proceed, ending ETL workflow.")
        return

    query_year = get_assessment_year_to_query()

    # First see if we already have a database in s3 to add updated data to
    download_database_from_s3()

    # If we do not have a database file now then make one
    if not os.path.exists(DB_LOCAL_PATH):
        create_database()

    mar_results = fetch_municipality_assessment_ratios(open_ny_token, query_year)

    if mar_results:
        save_municipality_assessment_ratios(mar_results)
        upload_database_to_s3()
        custom_logger(INFO_LOG_LEVEL, "Completed ETL workflow successfully.")


if __name__ == "__main__":
    cny_real_estate_etl_workflow()
