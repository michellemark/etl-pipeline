import os
from pprint import pprint
from typing import List
from typing import Optional

from backoff import expo
from backoff import on_exception
from pydantic import ValidationError
from ratelimit import limits
from sodapy import Socrata

from etl.constants import CNY_COUNTY_LIST
from etl.constants import INFO_LOG_LEVEL
from etl.constants import NY_PROPERTY_ASSESSMENTS_TABLE
from etl.constants import OPEN_NY_BASE_URL
from etl.constants import OPEN_NY_CALLS_PER_PERIOD
from etl.constants import OPEN_NY_LIMIT_PER_PAGE
from etl.constants import OPEN_NY_PROPERTY_ASSESSMENTS_API_ID
from etl.constants import OPEN_NY_RATE_LIMIT_PERIOD
from etl.constants import PROPERTIES_TABLE
from etl.constants import RETRYABLE_ERRORS
from etl.constants import WARNING_LOG_LEVEL
from etl.db_utilities import execute_db_query
from etl.db_utilities import insert_into_database
from etl.log_utilities import custom_logger
from etl.log_utilities import log_retry
from etl.property_utilities import get_ny_property_classes_for_where_clause
from etl.validation_models import NYPropertyAssessment


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
    results = execute_db_query(sql_query, params=(roll_year, county_name))

    if results and isinstance(results[0], tuple) and len(results[0]) == 1:
        count = results[0][0]

        if count > 0:
            do_property_assessments_for_year_exist = True

    return do_property_assessments_for_year_exist


@on_exception(
    expo,
    RETRYABLE_ERRORS,
    max_tries=3,
    on_backoff=log_retry
)
@limits(calls=OPEN_NY_CALLS_PER_PERIOD, period=OPEN_NY_RATE_LIMIT_PERIOD)
def fetch_property_assessments_page(
    app_token: str,
    roll_year: int,
    county_name: str,
    where_clause: str,
    offset: int) -> Optional[List[dict]]:
    result = None

    try:
        custom_logger(
            INFO_LOG_LEVEL,
            f"Fetching property assessments for county_name: {county_name} starting at offset {offset}..."
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

    except RETRYABLE_ERRORS:
        # Let these propagate to be handled by the @on_exception decorator
        raise

    except Exception as err:
        custom_logger(
            WARNING_LOG_LEVEL,
            f"Failed fetching property assessments for county_name: {county_name} at offset {offset}. Error: {err}"
        )

    return result


def fetch_property_assessments(app_token: str, query_year: int) -> List[dict] or None:
    """
    Query Open NY APIs for property assessments for a given year for all counties
    in the CNY_COUNT
    Y_LIST.  Only get properties with roll_section=1, meaning they are
    ordinary taxable property.
    """
    all_properties = []
    force_refresh = os.getenv('FORCE_REFRESH', False)

    custom_logger(
        INFO_LOG_LEVEL,
        f"Starting fetching CNY property assessments for roll_year {query_year}..."
        + (f" Forcing refresh of all assessments." if force_refresh else "")
    )

    # Initial results were getting back too many properties of not relevant types, limit results with a WHERE
    where_clause = "roll_section = 1"
    property_class_clause = get_ny_property_classes_for_where_clause()

    if property_class_clause:
        where_clause = f"{where_clause} AND {property_class_clause}"

    custom_logger(INFO_LOG_LEVEL, f"\nwhere_clause built: {where_clause}\n")

    for county in CNY_COUNTY_LIST:
        # First see if we already have data for this county and roll year as it is only published once a year
        already_exists = check_if_property_assessments_exist(query_year, county)

        if already_exists and not force_refresh:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Property assessments for county_name: {county} in roll year {query_year} already exist, ending.")
            continue

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


def save_properties_and_assessments(all_properties: List[dict]):
    """
    Validate properties data and saves valid data to related
    database tables properties and ny_property_assessments.
    """
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
