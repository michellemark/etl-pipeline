import gzip
import os
import shutil
import sqlite3
from typing import List
from typing import Tuple

import boto3

from etl.constants import CREATE_TABLE_DEFINITIONS_FILE_PATH
from etl.constants import DB_LOCAL_PATH
from etl.constants import EXTRACTED_DATA_DIR
from etl.constants import GENERATED_DATA_DIR
from etl.constants import GZIPPED_DB_LOCAL_PATH
from etl.constants import GZIPPED_DB_NAME
from etl.constants import INFO_LOG_LEVEL
from etl.constants import LOCAL_VERSION_PATH
from etl.constants import S3_BUCKET_NAME
from etl.constants import SQLITE_DB_NAME
from etl.constants import VERSION_FILE_NAME
from etl.constants import WARNING_LOG_LEVEL
from etl.constants import ZIPCODE_CACHE_KEY
from etl.constants import ZIPCODE_CACHE_LOCAL_PATH
from etl.log_utilities import custom_logger


def ensure_data_directories_exist():
    """
    Ensure needed data directories exist.
    If directories already exist will not touch or raise errors.
    """
    os.makedirs(EXTRACTED_DATA_DIR, exist_ok=True)
    os.makedirs(GENERATED_DATA_DIR, exist_ok=True)


def create_database():
    """
    Create a new SQLite database and initialize it with defined schema.
    """
    ensure_data_directories_exist()

    try:

        with open(CREATE_TABLE_DEFINITIONS_FILE_PATH, "r") as sql_file:
            sql_script = sql_file.read()

        db_connection = sqlite3.connect(DB_LOCAL_PATH)
        db_cursor = db_connection.cursor()
        db_cursor.executescript(sql_script)
        db_connection.commit()
        db_connection.close()
        custom_logger(INFO_LOG_LEVEL, f"Database created at {DB_LOCAL_PATH}")

    except Exception as e:
        custom_logger(WARNING_LOG_LEVEL, f"Error creating the database: {str(e)}")


def insert_or_replace_into_database(table_name: str, column_names: List[str], data: List[Tuple]) -> Tuple[int, int]:
    """
    Insert records into a specified SQLite database table with row-by-row error handling.
    Note: uses REPLACE INTO instead of INSERT INTO to avoid duplicate key errors.  This
    will cause existing rows to be deleted and replaced with new data.

    Example Usage:
        insert_or_replace_into_database(
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
        sql_query = f"REPLACE INTO {table_name} ({column_names_joined}) VALUES ({value_placeholders})"

        # Start the database connection
        with sqlite3.connect(DB_LOCAL_PATH) as db_connection:
            db_cursor = db_connection.cursor()

            # Insert each row of data one at a time so if any fail the rest will still be inserted
            for index, row in enumerate(data, start=1):

                try:
                    db_cursor.execute(sql_query, row)
                    db_connection.commit()
                    rows_inserted += 1
                except sqlite3.IntegrityError as ex:
                    custom_logger(
                        WARNING_LOG_LEVEL,
                        f"Row {index} failed to insert due to an integrity error: {ex}. Row data: {row}"
                    )
                    rows_failed += 1
                except sqlite3.Error as ex:
                    custom_logger(
                        WARNING_LOG_LEVEL,
                        f"Row {index} failed to insert due to a general database error: {ex}. Row data: {row}"
                    )
                    rows_failed += 1

        custom_logger(
            INFO_LOG_LEVEL,
            f"rows_inserted: {rows_inserted}, rows_failed: {rows_failed}"
        )

    except sqlite3.Error as ex:
        custom_logger(WARNING_LOG_LEVEL, f"Unexpected database error occurred: {ex}")
        rows_failed = len(data)

    return rows_inserted, rows_failed


def execute_db_query(
        query: str,
        params: Tuple | None = None,
        fetch_results: bool = True) -> List[Tuple] | int | bool | None:
    """
   Executes a raw SQL query. Depending on the query type, either fetches the results (for SELECT)
    or returns the number of affected rows (for UPDATE, INSERT, DELETE) or True for schema-altering queries.

    Example Usages:
    - SELECT queries:
        results = execute_db_query(
            "SELECT * FROM table WHERE column = ?",
            params=("value",),
            fetch_results=True)
    - Non-SELECT queries:
        rows_affected = execute_db_query(
            "UPDATE table SET column = ? WHERE column = ?",
            params=("new_value", "value"),
            fetch_results=False)


    :param query: str An SQL query to execute.
    :param params: Tuple | None Optional parameters for a parameterized query.
    :param fetch_results: bool Whether to fetch results for SELECT queries or return rows affected for others.
    :return: List[Tuple] | None Query results or None if there's an error.
    """
    result = None

    try:
        with sqlite3.connect(DB_LOCAL_PATH) as db_connection:
            db_cursor = db_connection.cursor()

            if params:
                db_cursor.execute(query, params)
            else:
                db_cursor.execute(query)

            if fetch_results:
                result = db_cursor.fetchall()
            else:
                # Schema-altering queries return True for success
                if query.strip().upper().startswith(("CREATE", "DROP", "ALTER")):
                    result = True
                else:
                    # Return number of rows affected
                    result = db_cursor.rowcount

    except sqlite3.Error as ex:
        custom_logger(
            WARNING_LOG_LEVEL,
            f"Query {query} failed, database error: {ex}."
        )

    return result


def get_s3_client():
    """
    Helper function to get an S3 client.
    """
    s3_client = None
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.environ.get("AWS_REGION")

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_REGION:
        try:
            aws_session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
            s3_client = aws_session.client("s3")
        except Exception as ex:
            custom_logger(
                WARNING_LOG_LEVEL,
                "Unable to create S3 client. Skipping operation.")
    else:
        if not AWS_ACCESS_KEY_ID:
            custom_logger(WARNING_LOG_LEVEL, "Missing AWS_ACCESS_KEY_ID environment variable.")
        if not AWS_SECRET_ACCESS_KEY:
            custom_logger(WARNING_LOG_LEVEL, "Missing AWS_SECRET_ACCESS_KEY environment variable.")
        if not AWS_REGION:
            custom_logger(WARNING_LOG_LEVEL, "Missing AWS_REGION environment variable.")

        custom_logger(WARNING_LOG_LEVEL, "Unable to create S3 client. Skipping operation.")

    return s3_client


def download_database_from_s3():
    """Download SQLite database from S3 bucket to local path."""
    s3_client = get_s3_client()

    if s3_client:
        ensure_data_directories_exist()

        try:
            s3_client.download_file(
                Bucket=S3_BUCKET_NAME,
                Key=GZIPPED_DB_NAME,
                Filename=GZIPPED_DB_LOCAL_PATH
            )
            compressed_size = os.path.getsize(GZIPPED_DB_LOCAL_PATH)

            # Decompress database to expected location
            with gzip.open(GZIPPED_DB_LOCAL_PATH, 'rb') as unzipped_db:
                with open(DB_LOCAL_PATH, 'wb+') as local_db:
                    shutil.copyfileobj(unzipped_db, local_db)

            decompressed_size = os.path.getsize(DB_LOCAL_PATH)

            custom_logger(
                INFO_LOG_LEVEL,
                f"Download complete: {compressed_size / (1024 ** 2):.2f} MB compressed → "
                f"{decompressed_size / (1024 ** 2):.2f} MB decompressed.")

            # Download current version file from AWS save as LOCAL_VERSION_PATH
            s3_client.download_file(S3_BUCKET_NAME, VERSION_FILE_NAME, LOCAL_VERSION_PATH)

        except Exception as ex:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed to download database from S3: {ex}")


def create_or_update_version_file_and_upload():
    """Create or update version file and upload to S3."""
    s3_client = get_s3_client()

    with open(LOCAL_VERSION_PATH, 'a+') as version_file:
        version_file.seek(0)
        local_version = version_file.read().strip()

        try:
            local_version = int(local_version) + 1
        except (TypeError, ValueError):
            local_version = 1

        version_file.write(str(local_version))

        try:
            s3_client.upload_file(
                Filename=LOCAL_VERSION_PATH,
                Bucket=S3_BUCKET_NAME,
                Key=VERSION_FILE_NAME
            )
        except Exception as ex:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed to upload version {local_version} to S3: {ex}")
        else:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Successfully uploaded version {local_version} to s3://{S3_BUCKET_NAME}/{VERSION_FILE_NAME}")


def upload_database_to_s3():
    """Upload SQLite database to S3."""
    s3_client = get_s3_client()

    if s3_client:
        custom_logger(INFO_LOG_LEVEL, "Uploading database to S3...")

        try:
            # Compress database before uploading
            with open(DB_LOCAL_PATH, 'rb') as local_db:
                with gzip.open(GZIPPED_DB_LOCAL_PATH, 'wb+') as zipped_db:
                    shutil.copyfileobj(local_db, zipped_db)

            s3_client.upload_file(
                Filename=GZIPPED_DB_LOCAL_PATH,
                Bucket=S3_BUCKET_NAME,
                Key=GZIPPED_DB_NAME
            )
            create_or_update_version_file_and_upload()

        except Exception as ex:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed to upload database to S3: {ex}")
        else:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Successfully uploaded {GZIPPED_DB_LOCAL_PATH} to s3://{S3_BUCKET_NAME}/{GZIPPED_DB_NAME}")


def download_zipcodes_cache_from_s3() -> dict | None:
    """Download zipcodes cache from S3 bucket to local path."""
    s3_client = get_s3_client()

    if s3_client:
        ensure_data_directories_exist()

        try:
            s3_client.download_file(
                Bucket=S3_BUCKET_NAME,
                Key=ZIPCODE_CACHE_KEY,
                Filename=ZIPCODE_CACHE_LOCAL_PATH
            )
            custom_logger(
                INFO_LOG_LEVEL,
                f"Successfully downloaded {ZIPCODE_CACHE_KEY} from s3://{S3_BUCKET_NAME}/{ZIPCODE_CACHE_KEY} to {ZIPCODE_CACHE_LOCAL_PATH}")
        except Exception as ex:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed to download zipcodes cache from S3: {ex}")


def upload_zipcodes_cache_to_s3():
    """Upload zipcodes cache to S3."""
    s3_client = get_s3_client()

    if s3_client:

        try:
            s3_client.upload_file(
                Filename=ZIPCODE_CACHE_LOCAL_PATH,
                Bucket=S3_BUCKET_NAME,
                Key=ZIPCODE_CACHE_KEY
            )
        except Exception as ex:
            custom_logger(
                WARNING_LOG_LEVEL,
                f"Failed to upload zipcodes cache to S3: {ex}")
        else:
            custom_logger(
                INFO_LOG_LEVEL,
                f"Successfully uploaded {ZIPCODE_CACHE_LOCAL_PATH} to s3://{S3_BUCKET_NAME}/{ZIPCODE_CACHE_KEY}")
