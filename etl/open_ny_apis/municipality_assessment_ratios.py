import json
from typing import List

from backoff import expo
from backoff import on_exception
from pydantic import ValidationError
from ratelimit import limits
from sodapy import Socrata

from etl.constants import ASSESSMENT_RATIOS_TABLE
from etl.constants import CNY_COUNTY_LIST
from etl.constants import INFO_LOG_LEVEL
from etl.constants import OPEN_NY_ASSESSMENT_RATIOS_API_ID
from etl.constants import OPEN_NY_BASE_URL
from etl.constants import OPEN_NY_CALLS_PER_PERIOD
from etl.constants import OPEN_NY_RATE_LIMIT_PERIOD
from etl.constants import RETRYABLE_ERRORS
from etl.constants import WARNING_LOG_LEVEL
from etl.db_utilities import execute_db_query
from etl.db_utilities import insert_into_database
from etl.log_utilities import custom_logger
from etl.log_utilities import log_retry
from etl.validation_models import MunicipalityAssessmentRatio


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
    results = execute_db_query(sql_query, params=(rate_year, county_name), fetch_results=True)

    if results:
        do_county_ratios_for_year_exist = True

    return do_county_ratios_for_year_exist


@on_exception(
    expo,
    RETRYABLE_ERRORS,
    max_tries=3,
    on_backoff=log_retry
)
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

    except RETRYABLE_ERRORS:
        # Let these propagate to be handled by the @on_exception decorator
        raise

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
    """Validate the municipality_assessment_ratios data and save valid data to database."""
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
