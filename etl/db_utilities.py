import os
import sqlite3

import boto3


current_file_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_file_path))
extracted_data_dir = os.path.join(project_root, "extracted")
generated_data_dir = os.path.join(project_root, "generated")
sqlite_db_name = "cny-real-estate.db"
db_local_path = os.path.join(generated_data_dir, sqlite_db_name)

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
    Create a new SQLite database and initialize it with a basic schema.
    """
    # Ensure needed data directories exist, if they do not already
    ensure_data_directories_exist()

    # Connecting to SQLite DB creates the file if it does not exist
    conn = sqlite3.connect(db_local_path)
    cursor = conn.cursor()

    # Create table to store extracted NY real estate data
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {NY_DB_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT NOT NULL,
        price INTEGER NOT NULL,
        bedrooms INTEGER,
        bathrooms INTEGER
    );
    """)

    conn.commit()
    conn.close()
    print(f"Database created at {db_local_path}")


def populate_database(new_data):
    """
    Add sample records to the SQLite database.
    """
    # Establish connection
    conn = sqlite3.connect(db_local_path)
    cursor = conn.cursor()
    cursor.executemany(f"""
        INSERT INTO {NY_DB_TABLE} (address, price, bedrooms, bathrooms)
        VALUES (?, ?, ?, ?)
        """, new_data)
    conn.commit()
    conn.close()
    print(f"Data inserted into {NY_DB_TABLE} in database {sqlite_db_name}")


def upload_database_to_s3():
    """
    Upload the SQLite database to S3.
    """
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.environ.get("AWS_REGION")

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
        print("Missing required AWS environment variables. Skipping upload.")
        return

    aws_session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3_client = aws_session.client("s3")
    s3_bucket_name = "cny-realestate-data"
    s3_client.upload_file(db_local_path, s3_bucket_name, sqlite_db_name)
    print(f"Uploaded {db_local_path} to s3://{s3_bucket_name}/{sqlite_db_name}")


# Test it out - TODO: remove this
if __name__ == "__main__":
    create_database()
    # Example data to insert
    sample_data = [
        ("123 Maple St", 250000, 3, 2),
        ("456 Oak St", 350000, 4, 3),
        ("789 Pine St", 150000, 2, 1),
    ]
    populate_database(sample_data)
    upload_database_to_s3()
