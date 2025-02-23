from unittest.mock import patch, MagicMock
from etl.db_utilities import ensure_data_directories_exist, extracted_data_dir, generated_data_dir, NY_DB_TABLE, db_local_path, \
    create_database, populate_database, sqlite_db_name, upload_database_to_s3, s3_bucket_name
from etl.log_utilities import INFO_LOG_LEVEL, ERROR_LOG_LEVEL


def test_ensure_data_directories_exist():
    """Test ensure_data_directories_exist creates required directories."""

    with patch("os.makedirs") as mock_makedirs:
        ensure_data_directories_exist()

        mock_makedirs.assert_any_call(extracted_data_dir, exist_ok=True)
        mock_makedirs.assert_any_call(generated_data_dir, exist_ok=True)


def test_create_database():
    """Test create_database function makes all expected calls."""

    with patch("etl.db_utilities.ensure_data_directories_exist") as mock_ensure_dirs, \
        patch("sqlite3.connect") as mock_sqlite_connect, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger:

        mock_db_connection = MagicMock()
        mock_db_cursor = MagicMock()
        mock_sqlite_connect.return_value = mock_db_connection
        mock_db_connection.cursor.return_value = mock_db_cursor

        create_database()

        mock_ensure_dirs.assert_called_once()
        mock_sqlite_connect.assert_called_once_with(db_local_path)
        mock_db_cursor.executescript.assert_called_once()
        mock_db_connection.commit.assert_called_once()
        mock_db_connection.close.assert_called_once()
        mock_custom_logger.assert_called_once_with(INFO_LOG_LEVEL, f"Database created at {db_local_path}")


def test_populate_database():
    """Test populate_database makes expected calls."""
    # TODO: will need updating when schema / function changes)
    test_data = [
        ("123 Test St", 250000, 3, 2),
        ("456 Example Ave", 300000, 4, 3),
    ]

    with patch("sqlite3.connect") as mock_sqlite_connect, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger:

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_sqlite_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        populate_database(test_data)

        mock_sqlite_connect.assert_called_once_with(db_local_path)
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
        mock_custom_logger.assert_called_once()


def test_upload_database_to_s3_success():
    """Test upload_database_to_s3 when all environment variables are present and upload succeeds."""

    with patch("os.environ.get") as mock_env_get, \
        patch("boto3.Session") as mock_boto3_session, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger:

        mock_env_get.side_effect = lambda key: {
            "AWS_ACCESS_KEY_ID": "mock_access_key",
            "AWS_SECRET_ACCESS_KEY": "mock_secret_key",
            "AWS_REGION": "mock_region"
        }.get(key)
        mock_session = MagicMock()
        mock_boto3_session.return_value = mock_session
        mock_s3_client = MagicMock()
        mock_session.client.return_value = mock_s3_client

        upload_database_to_s3()

        mock_env_get.assert_any_call("AWS_ACCESS_KEY_ID")
        mock_env_get.assert_any_call("AWS_SECRET_ACCESS_KEY")
        mock_env_get.assert_any_call("AWS_REGION")
        mock_boto3_session.assert_called_once_with(
            aws_access_key_id="mock_access_key",
            aws_secret_access_key="mock_secret_key",
            region_name="mock_region"
        )
        mock_s3_client.upload_file.assert_called_once_with(db_local_path, s3_bucket_name, sqlite_db_name)
        mock_custom_logger.assert_called_once_with(
            INFO_LOG_LEVEL,
            f"Uploaded {db_local_path} to s3://{s3_bucket_name}/{sqlite_db_name}"
        )


def test_upload_database_to_s3_missing_env_vars():
    """Test upload_database_to_s3 when all required environment variables are missing."""
    with patch("os.environ.get") as mock_env_get, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("boto3.Session") as mock_boto3_session:

        # No environment variables set
        mock_env_get.return_value = None

        upload_database_to_s3()

        mock_env_get.assert_any_call("AWS_ACCESS_KEY_ID")
        mock_env_get.assert_any_call("AWS_SECRET_ACCESS_KEY")
        mock_env_get.assert_any_call("AWS_REGION")
        mock_custom_logger.assert_called_once_with(
            ERROR_LOG_LEVEL,
            "Missing required AWS environment variables, skipping upload of db."
        )

        # Ensure no upload attempt was made
        mock_boto3_session.assert_not_called()


def test_upload_database_to_s3_partial_env_vars():
    """Test upload_database_to_s3 with some environment variables missing."""
    with patch("os.environ.get") as mock_env_get, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("boto3.Session") as mock_boto3_session:

        # AWS_REGION is missing
        mock_env_get.side_effect = lambda key: {
            "AWS_ACCESS_KEY_ID": "mock_access_key",
            "AWS_SECRET_ACCESS_KEY": "mock_secret_key",
        }.get(key)

        upload_database_to_s3()

        mock_custom_logger.assert_called_once_with(
            ERROR_LOG_LEVEL,
            "Missing required AWS environment variables, skipping upload of db."
        )

        # Ensure no upload attempt was made
        mock_boto3_session.assert_not_called()
