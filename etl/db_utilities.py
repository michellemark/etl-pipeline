import os
import sqlite3

import boto3

from etl.log_utilities import custom_logger, INFO_LOG_LEVEL, ERROR_LOG_LEVEL

current_file_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_file_path))
extracted_data_dir = os.path.join(project_root, "extracted")
generated_data_dir = os.path.join(project_root, "generated")
sqlite_db_name = "cny-real-estate.db"
db_local_path = os.path.join(generated_data_dir, sqlite_db_name)
s3_bucket_name = "cny-realestate-data"

NY_DB_TABLE = "ny_properties"


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


def populate_database(new_data):
    """
    Add sample records to the SQLite database.
    """
    # Establish connection
    conn = sqlite3.connect(db_local_path)
    cursor = conn.cursor()
    cursor.executemany(
        f"""INSERT INTO {NY_DB_TABLE} (address, price, bedrooms, bathrooms) VALUES (?, ?, ?, ?)""",
        new_data
    )
    conn.commit()
    conn.close()
    custom_logger(INFO_LOG_LEVEL, f"Data inserted into {NY_DB_TABLE} in database {sqlite_db_name}")


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
