import json
import os
from datetime import datetime
from typing import List

from prefect import flow, task
from pydantic import ValidationError
from ratelimit import sleep_and_retry, limits
from sodapy import Socrata

from etl.constants import *
from etl.db_utilities import insert_into_database, create_database, upload_database_to_s3, download_database_from_s3, \
    db_local_path
from etl.log_utilities import custom_logger, ERROR_LOG_LEVEL, INFO_LOG_LEVEL
from etl.validation_models import MunicipalityAssessmentRatio


def get_open_ny_app_token():
    app_token = os.environ.get('OPEN_DATA_APP_TOKEN')

    if not app_token:
        custom_logger(ERROR_LOG_LEVEL, "Missing OPEN_DATA_APP_TOKEN environment variable.")

    return app_token


@sleep_and_retry
@limits(calls=OPEN_NY_CALLS_PER_PERIOD, period=OPEN_NY_RATE_LIMIT_PERIOD)
def fetch_municipality_assessment_ratio(app_token: str, rate_year: int, county_name: str):
    custom_logger(
        INFO_LOG_LEVEL,
        f"Fetching municipality assessment ratios for rate_year: {rate_year} and county_name: {county_name}")

    with Socrata(OPEN_NY_BASE_URL, app_token=app_token, timeout=60) as client:
        assessment_ratios = client.get(
            OPEN_NY_ASSESSMENT_RATIOS_API_ID,
            rate_year=rate_year,
            county_name=county_name
        )

    return assessment_ratios


@task
def fetch_municipality_assessment_ratios(app_token):
    """
    Fetch municipality_assessment_ratios from Open NY APIs for all counties
    in the CNY_COUNTY_LIST.  Will get assessment ratios for all years from
    2009 to present, where data is available.
    """
    assessment_ratios = []
    current_year = datetime.now().year
    custom_logger(INFO_LOG_LEVEL, f"Starting fetching all municipality assessment ratios...")

    for year in range(OPEN_NY_EARLIEST_YEAR, current_year + 1):

        for county in CNY_COUNTY_LIST:
            ratio_results = fetch_municipality_assessment_ratio(app_token=app_token, rate_year=year, county_name=county)

            if ratio_results and isinstance(ratio_results, list):
                assessment_ratios.extend(ratio_results)
            else:
                # We have gotten to a year with no results, probably the current year, time to stop
                break

    custom_logger(INFO_LOG_LEVEL, f"Completed fetching municipality assessment ratios, {len(assessment_ratios)} found.")

    return assessment_ratios


@task
def save_municipality_assessment_ratios(all_ratios: List[dict]):
    """Task 2: Validate the municipality_assessment_ratios data and save valid data to database."""
    validated_ratio_data = []
    column_names = None

    for municipality_assessment_ratio in all_ratios:
        try:
            model = MunicipalityAssessmentRatio.model_validate(municipality_assessment_ratio)
        except ValidationError as err:
            custom_logger(
                ERROR_LOG_LEVEL,
                f"Failed to validate municipality assessment ratio: {municipality_assessment_ratio}. Error: {err}")
        else:
            ratio_data = json.loads(model.model_dump_json(by_alias=True))
            validated_ratio_data.append(tuple(ratio_data.values()))

            if not column_names:
                column_names = list(ratio_data.keys())

    rows_inserted, rows_failed = insert_into_database(ASSESSMENT_RATIOS_TABLE, column_names, validated_ratio_data)
    custom_logger(
        INFO_LOG_LEVEL,
        f"Completed saving municipality assessment ratios to database rows_inserted: {rows_inserted}, rows_failed: {rows_failed}.")

@flow
def cny_real_estate_data_workflow():
    """Main entry point for the ETL workflow."""
    open_ny_token = get_open_ny_app_token()

    if not open_ny_token:
        custom_logger(ERROR_LOG_LEVEL, "Cannot proceed, ending flow...")
        return 0

    # First see if we already have a database in s3 to add updated data to
    download_database_from_s3()

    # If we do not have a database file now then make one
    if not os.path.exists(db_local_path):
        create_database()

    mar_results = fetch_municipality_assessment_ratios(open_ny_token)
    save_municipality_assessment_ratios(mar_results)
    upload_database_to_s3()


if __name__ == "__main__":
    cny_real_estate_data_workflow.serve(name="etl-pipeline",
                      tags=["test-run"],
                      interval=60)
