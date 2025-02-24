import os
import sqlite3
from typing import List, Tuple

import boto3

from etl.log_utilities import custom_logger, INFO_LOG_LEVEL, ERROR_LOG_LEVEL

current_file_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_file_path))
extracted_data_dir = os.path.join(project_root, "extracted")
generated_data_dir = os.path.join(project_root, "generated")
sqlite_db_name = "cny-real-estate.db"
db_local_path = os.path.join(generated_data_dir, sqlite_db_name)
s3_bucket_name = "cny-realestate-data"

ASSESSMENT_RATIOS_TABLE = "municipality_assessment_ratios"
NY_PROPERTY_ASSESSMENTS_TABLE = "ny_property_assessments"
PROPERTIES_TABLE = "properties"


def ensure_data_directories_exist():
    """
    Ensure needed data directories exist.
    If directories already exist will not touch or raise errors.
    """
    os.makedirs(extracted_data_dir, exist_ok=True)
    os.makedirs(generated_data_dir, exist_ok=True)


def create_database():
    """
    Create a new SQLite database and initialize it with defined schema.
    """
    ensure_data_directories_exist()
    create_table_definitions_path = os.path.join(project_root, "sql", "create_table_definitions.sql")

    try:

        with open(create_table_definitions_path, "r") as sql_file:
            sql_script = sql_file.read()

        db_connection = sqlite3.connect(db_local_path)
        db_cursor = db_connection.cursor()
        db_cursor.executescript(sql_script)
        db_connection.commit()
        db_connection.close()
        custom_logger(INFO_LOG_LEVEL, f"Database created at {db_local_path}")

    except Exception as e:
        custom_logger(ERROR_LOG_LEVEL, f"Error creating the database: {str(e)}")


def insert_into_database(table_name: str, column_names: List[str], data: List[Tuple]) -> Tuple[int, int]:
    """
    Insert records into a specified SQLite database table with row-by-row error handling.

    Example Usage:
        populate_database(
            "properties",
            ["id", "swis_code", "print_key_code", "municipality_code"],
            [
                ("ABC 123", "ABC", "123", "XYZ"),
                ("ABD 124", "ABD", "124", "XYZ"),
                ("ACE 125", "ACE", "125", "RST"),
            ]
        )

    :param table_name: (str): Name of the target table
    :param column_names: (list of str): The list of columns to populate
    :param data: (list of tuple): List of data rows, where each row is a tuple of values
    :return: (tuple of int): A tuple containing count of rows inserted and count of rows failed.
    """
    rows_inserted: int = 0
    rows_failed: int = 0

    try:
        # Build the SQL query dynamically
        column_names_joined = ", ".join(column_names)
        value_placeholders = ", ".join(["?"] * len(column_names))
        sql_query = f"INSERT INTO {table_name} ({column_names_joined}) VALUES ({value_placeholders})"

        # Start the database connection
        with sqlite3.connect(db_local_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Insert each row of data one at a time so if any fail the rest will still be inserted
            for index, row in enumerate(data, start=1):

                try:
                    db_cursor.execute(sql_query, row)
                    db_connection.commit()
                    rows_inserted += 1
                except sqlite3.IntegrityError as ex:
                    custom_logger(
                        ERROR_LOG_LEVEL,
                        f"Row {index} failed to insert due to an integrity error: {ex}. Row data: {row}"
                    )
                    rows_failed += 1
                except sqlite3.Error as ex:
                    custom_logger(
                        ERROR_LOG_LEVEL,
                        f"Row {index} failed to insert due to a general database error: {ex}. Row data: {row}"
                    )
                    rows_failed += 1

        custom_logger(
            INFO_LOG_LEVEL,
            f"rows_inserted: {rows_inserted}, rows_failed: {rows_failed}"
        )

    except sqlite3.Error as ex:
        custom_logger(ERROR_LOG_LEVEL, f"Unexpected database error occurred: {ex}")
        rows_failed = len(data)

    return rows_inserted, rows_failed


def upload_database_to_s3():
    """
    Upload the SQLite database to S3, assuming needed environment variables are set.
    """
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.environ.get("AWS_REGION")

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
        custom_logger(ERROR_LOG_LEVEL, "Missing required AWS environment variables, skipping upload of db.")
        return

    aws_session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3_client = aws_session.client("s3")
    s3_client.upload_file(db_local_path, s3_bucket_name, sqlite_db_name)
    custom_logger(INFO_LOG_LEVEL, f"Uploaded {db_local_path} to s3://{s3_bucket_name}/{sqlite_db_name}")


# Test it out - TODO: remove this
if __name__ == "__main__":
    create_database()
    # Example data to insert
    # sample_data = [
    #     ("123 Maple St", 250000, 3, 2),
    #     ("456 Oak St", 350000, 4, 3),
    #     ("789 Pine St", 150000, 2, 1),
    # ]
    # populate_database(sample_data)
    # upload_database_to_s3()
