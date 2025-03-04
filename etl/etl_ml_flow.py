import json
import os
from datetime import datetime
from pprint import pprint
import random
from time import sleep
from typing import List

from pydantic import ValidationError
from ratelimit import limits
from ratelimit import sleep_and_retry
from sodapy import Socrata

from etl.constants import ASSESSMENT_RATIOS_TABLE
from etl.constants import CNY_COUNTY_LIST
from etl.constants import DB_LOCAL_PATH
from etl.constants import ERROR_LOG_LEVEL
from etl.constants import INFO_LOG_LEVEL
from etl.constants import NY_PROPERTY_ASSESSMENTS_TABLE
from etl.constants import OPEN_NY_ASSESSMENT_RATIOS_API_ID
from etl.constants import OPEN_NY_BASE_URL
from etl.constants import OPEN_NY_CALLS_PER_PERIOD
from etl.constants import OPEN_NY_LIMIT_PER_PAGE
from etl.constants import OPEN_NY_PROPERTY_ASSESSMENTS_API_ID
from etl.constants import OPEN_NY_RATE_LIMIT_PERIOD
from etl.constants import PROPERTIES_TABLE
from etl.constants import RETRYABLE_ERRORS
from etl.constants import WARNING_LOG_LEVEL
from etl.db_utilities import create_database
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import execute_select_query
from etl.db_utilities import insert_into_database
from etl.db_utilities import upload_database_to_s3
from etl.log_utilities import custom_logger
from etl.property_utilities import get_assessment_year_to_query
from etl.property_utilities import get_ny_property_classes_for_where_clause
from etl.property_utilities import get_open_ny_app_token
from etl.validation_models import MunicipalityAssessmentRatio
from etl.validation_models import NYPropertyAssessment


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
            WARNING_LOG_LEVEL,
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
                WARNING_LOG_LEVEL,
                f"Failed to validate municipality assessment ratio {municipality_assessment_ratio} Errors:")
            for error in err.errors():
                custom_logger(
                    WARNING_LOG_LEVEL,
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


def check_if_property_assessments_exist(roll_year: int, county_name: str) -> bool:
    """
    Property assessments are only published once per year for each roll year.
    Use this to check if we already have the data and save a great many calls to the API.
    """
    do_property_assessments_for_year_exist = False
    custom_logger(
        INFO_LOG_LEVEL,
        f"Checking if property assessments for roll_year: {roll_year} and county_name: {county_name} exist in database...")
    sql_query = f"""SELECT COUNT(*) 
                    FROM {NY_PROPERTY_ASSESSMENTS_TABLE} AS npa
                    JOIN {PROPERTIES_TABLE} AS p ON npa.property_id = p.id
                    WHERE npa.roll_year = ? AND p.county_name = ?"""
    results = execute_select_query(sql_query, params=(roll_year, county_name))

    if results and isinstance(results[0], tuple) and len(results[0]) == 1:
        count = results[0][0]

        if count > 0:
            do_property_assessments_for_year_exist = True

    return do_property_assessments_for_year_exist


@sleep_and_retry
@limits(calls=OPEN_NY_CALLS_PER_PERIOD, period=OPEN_NY_RATE_LIMIT_PERIOD)
def fetch_property_assessments_page(app_token: str, roll_year: int, county_name: str, where_clause: str, offset: int) -> Optional[
    List[dict]]:
    result = None
    max_retries = 3

    # Maintain Open NY rate limit of 3 requests per minute
    wait_seconds = 20

    # Retry up to 3 times, unless successful
    for attempt in range(max_retries):

        try:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Fetching property assessments for county_name: {county_name} starting at offset {offset}..."
                + (f" (Attempt {attempt + 1}/3)" if attempt > 0 else "")
            )

            with Socrata(OPEN_NY_BASE_URL, app_token=app_token, timeout=60) as client:
                result = client.get(
                    OPEN_NY_PROPERTY_ASSESSMENTS_API_ID,
                    roll_year=roll_year,
                    county_name=county_name,
                    roll_section=1,
                    limit=OPEN_NY_LIMIT_PER_PAGE,
                    offset=offset,
                    order="swis_code,print_key_code ASC",
                    where=where_clause
                )
                break

        except RETRYABLE_ERRORS as err:

            # Don't sleep on last attempt
            if attempt < 2:
                sleep(wait_seconds)
            else:
                custom_logger(
                    WARNING_LOG_LEVEL,
                    f"Failed fetching property assessments for county_name: {county_name} at offset {offset}. Error: {err}"
                )

        except Exception as err:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed fetching property assessments for county_name: {county_name} at offset {offset}. Error: {err}"
            )
            break

    return result


def fetch_properties_and_assessments_from_open_ny(app_token: str, query_year: int) -> List[dict] or None:
    """
    Query Open NY APIs for properties and assessments for a given year for all counties
    in the CNY_COUNTY_LIST.  Only get properties with roll_section=1, meaning they are
    ordinary taxable property.
    """
    all_properties = []
    custom_logger(INFO_LOG_LEVEL, f"Starting fetching CNY property assessments for roll_year {query_year}...")

    # Initial results were getting back too many properties of not relevant types, limit results with a WHERE
    where_clause = "roll_section = 1"
    property_class_clause = get_ny_property_classes_for_where_clause()

    if property_class_clause:
        where_clause = f"{where_clause} AND {property_class_clause}"

    custom_logger(INFO_LOG_LEVEL, f"\nwhere_clause built: {where_clause}\n")

    for county in CNY_COUNTY_LIST:
        # First see if we already have data for this county and roll year as it is only published once a year
        already_exists = check_if_property_assessments_exist(query_year, county)

        if already_exists:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Property assessments for county_name: {county} in roll year {query_year} already exist, ending.")
        else:
            call_again = True
            current_offset = 0

            while call_again:

                property_results = fetch_property_assessments_page(
                    app_token=app_token,
                    roll_year=query_year,
                    county_name=county,
                    where_clause=where_clause,
                    offset=current_offset
                )

                if property_results and isinstance(property_results, list):
                    all_properties.extend(property_results)
                    current_offset += OPEN_NY_LIMIT_PER_PAGE
                else:
                    call_again = False
                    custom_logger(
                        INFO_LOG_LEVEL,
                        f"No more property assessments for county_name: {county}, ending.")

    custom_logger(INFO_LOG_LEVEL, f"Completed fetching all CNY property assessments, {len(all_properties)} found.")

    return all_properties


def save_property_assessments(all_properties: List[dict]):
    """Validate properties data and save valid data to database tables."""
    validated_properties_data = []
    validated_ny_property_assessment_data = []
    properties_column_names = None
    ny_property_assessment_column_names = None

    for property_assessment in all_properties:

        try:
            model = NYPropertyAssessment(**property_assessment)
        except ValidationError as err:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed to validate property assessment:")
            pprint(property_assessment)
            for error in err.errors():
                custom_logger(
                    WARNING_LOG_LEVEL,
                    f"- Error: Field: {error["loc"][0]}. Message: {error["msg"]}")
        else:
            # Get data for saving to properties table
            property_data = model.to_properties_row()
            validated_properties_data.append(tuple(property_data.values()))

            if not properties_column_names:
                properties_column_names = list(property_data.keys())

            # Get data for saving to ny_property_assessments table
            ny_property_assessment_data = model.to_ny_property_assessments_row()
            validated_ny_property_assessment_data.append(tuple(ny_property_assessment_data.values()))

            if not ny_property_assessment_column_names:
                ny_property_assessment_column_names = list(ny_property_assessment_data.keys())

    # Insert into two related tables
    if validated_properties_data and validated_ny_property_assessment_data:
        # Save to properties table
        rows_inserted, rows_failed = insert_into_database(
            PROPERTIES_TABLE,
            properties_column_names,
            validated_properties_data)
        custom_logger(
            INFO_LOG_LEVEL,
            f"Completed saving {len(validated_properties_data)} valid properties rows_inserted: {rows_inserted}, rows_failed: {rows_failed}.")

        # Save to ny_property_assessments for related properties
        rows_inserted, rows_failed = insert_into_database(
            NY_PROPERTY_ASSESSMENTS_TABLE,
            ny_property_assessment_column_names,
            validated_ny_property_assessment_data)
        custom_logger(
            INFO_LOG_LEVEL,
            f"Completed saving {len(validated_ny_property_assessment_data)} valid ny_property_assessment_data rows_inserted: {rows_inserted}, rows_failed: {rows_failed}.")

    else:
        custom_logger(
            INFO_LOG_LEVEL,
            "No valid properties found, skipping saving to database.")


def cny_real_estate_etl_workflow():
    """Main entry point for the ETL workflow."""
    open_ny_token = get_open_ny_app_token()

    if open_ny_token:

        query_year = get_assessment_year_to_query()

        # First see if we already have a database in s3 to add updated data to
        download_database_from_s3()

        # If we do not have a database file now then make one
        if not os.path.exists(DB_LOCAL_PATH):
            create_database()

        if os.path.exists(DB_LOCAL_PATH):
            mar_results = fetch_municipality_assessment_ratios(app_token=open_ny_token, query_year=query_year)

            if mar_results:
                save_municipality_assessment_ratios(mar_results)

            onypa_results = fetch_properties_and_assessments_from_open_ny(app_token=open_ny_token, query_year=query_year)

            if onypa_results:
                save_property_assessments(onypa_results)
                upload_database_to_s3()

        else:
            custom_logger(ERROR_LOG_LEVEL, "Cannot proceed, database creation failed, ending ETL workflow.")

    else:
        custom_logger(ERROR_LOG_LEVEL, "Cannot proceed, unable to get Open NY app token, ending ETL workflow.")


if __name__ == "__main__": # pragma: no cover
    start_time = datetime.now()
    custom_logger(INFO_LOG_LEVEL, f"Starting ETL workflow at {start_time:%Y-%m-%d %H:%M:%S}")
    cny_real_estate_etl_workflow()
    end_time = datetime.now()
    custom_logger(INFO_LOG_LEVEL, f"Completed ETL workflow at {end_time:%Y-%m-%d %H:%M:%S}")
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    custom_logger(INFO_LOG_LEVEL, f"Total time taken: {int(hours)} hours, {int(minutes)} minutes, and {int(seconds)} seconds.")
