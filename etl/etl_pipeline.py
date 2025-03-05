import os
from datetime import datetime

from etl.constants import DB_LOCAL_PATH
from etl.constants import ERROR_LOG_LEVEL
from etl.constants import INFO_LOG_LEVEL
from etl.db_utilities import create_database
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import upload_database_to_s3
from etl.fetch_municipality_assessment_ratios import fetch_municipality_assessment_ratios
from etl.fetch_municipality_assessment_ratios import save_municipality_assessment_ratios
from etl.fetch_properties_and_assessments_from_open_ny import fetch_properties_and_assessments_from_open_ny
from etl.fetch_properties_and_assessments_from_open_ny import save_property_assessments
from etl.log_utilities import custom_logger
from etl.property_utilities import get_assessment_year_to_query
from etl.property_utilities import get_open_ny_app_token


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
