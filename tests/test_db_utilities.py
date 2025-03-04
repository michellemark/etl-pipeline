from unittest.mock import patch, MagicMock
from etl.constants import *
from etl.db_utilities import create_database
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import ensure_data_directories_exist
from etl.db_utilities import get_s3_client
from etl.db_utilities import upload_database_to_s3


def test_ensure_data_directories_exist():
    """Test ensure_data_directories_exist creates required directories."""

    with patch("os.makedirs") as mock_makedirs:
        ensure_data_directories_exist()

        mock_makedirs.assert_any_call(EXTRACTED_DATA_DIR, exist_ok=True)
        mock_makedirs.assert_any_call(GENERATED_DATA_DIR, exist_ok=True)


def test_create_database_success():
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
        mock_sqlite_connect.assert_called_once_with(DB_LOCAL_PATH)
        mock_db_cursor.executescript.assert_called_once()
        mock_db_connection.commit.assert_called_once()
        mock_db_connection.close.assert_called_once()
        mock_custom_logger.assert_called_once_with(INFO_LOG_LEVEL, f"Database created at {DB_LOCAL_PATH}")


def test_create_database_fails():
    """Test create_database function when an error occurs."""

    with patch("etl.db_utilities.ensure_data_directories_exist") as mock_ensure_dirs, \
        patch("sqlite3.connect") as mock_sqlite_connect, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger:
        mock_db_connection = MagicMock()
        mock_db_cursor = MagicMock()
        mock_sqlite_connect.return_value = mock_db_connection
        mock_db_connection.cursor.return_value = mock_db_cursor
        mock_db_cursor.executescript.side_effect = Exception("Mock error")

        create_database()

        mock_custom_logger.assert_called_once_with(ERROR_LOG_LEVEL, "Error creating the database: Mock error")


def test_get_s3_client_success():
    """Test get_s3_client when all environment variables are present."""
    with patch("os.environ.get") as mock_env_get, \
        patch("boto3.Session") as mock_boto3_session:
        mock_env_get.side_effect = lambda key: {
            "AWS_ACCESS_KEY_ID": "mock_access_key",
            "AWS_SECRET_ACCESS_KEY": "mock_secret_key",
            "AWS_REGION": "mock_region"
        }.get(key)
        mock_session = MagicMock()
        mock_boto3_session.return_value = mock_session
        mock_s3_client = MagicMock()
        mock_session.client.return_value = mock_s3_client

        s3_client = get_s3_client()

        assert s3_client == mock_s3_client
        mock_env_get.assert_any_call("AWS_ACCESS_KEY_ID")
        mock_env_get.assert_any_call("AWS_SECRET_ACCESS_KEY")
        mock_env_get.assert_any_call("AWS_REGION")
        mock_boto3_session.assert_called_once_with(
            aws_access_key_id="mock_access_key",
            aws_secret_access_key="mock_secret_key",
            region_name="mock_region"
        )
        mock_session.client.assert_called_once_with("s3")


def test_get_s3_client_missing_env_vars():
    """Test get_s3_client when all required environment variables are missing."""
    with patch("os.environ.get") as mock_env_get, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("boto3.Session") as mock_boto3_session:

        # No environment variables set
        mock_env_get.return_value = None

        s3_client = get_s3_client()

        assert s3_client is None
        mock_env_get.assert_any_call("AWS_ACCESS_KEY_ID")
        mock_env_get.assert_any_call("AWS_SECRET_ACCESS_KEY")
        mock_env_get.assert_any_call("AWS_REGION")
        mock_custom_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            "Missing AWS_ACCESS_KEY_ID environment variable."
        )
        mock_custom_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            "Missing AWS_SECRET_ACCESS_KEY environment variable."
        )
        mock_custom_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            "Missing AWS_REGION environment variable."
        )
        mock_custom_logger.assert_any_call(
            ERROR_LOG_LEVEL,
            "Unable to create S3 client. Skipping operation."
        )

        # Ensure no upload attempt was made
        mock_boto3_session.assert_not_called()


def test_get_s3_client_partial_env_vars():
    """Test get_s3_client with some environment variables missing."""
    with patch("os.environ.get") as mock_env_get, \
        patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("boto3.Session") as mock_boto3_session:

        # AWS_REGION is missing
        mock_env_get.side_effect = lambda key: {
            "AWS_ACCESS_KEY_ID": "mock_access_key",
            "AWS_SECRET_ACCESS_KEY": "mock_secret_key",
        }.get(key)

        s3_client = get_s3_client()

        assert s3_client is None
        mock_custom_logger.assert_any_call(
            WARNING_LOG_LEVEL,
            "Missing AWS_REGION environment variable."
        )
        mock_custom_logger.assert_any_call(
            ERROR_LOG_LEVEL,
            "Unable to create S3 client. Skipping operation."
        )

        # Ensure no upload attempt was made
        mock_boto3_session.assert_not_called()


def test_download_database_from_s3_success():
    """Test download database from s3 when download succeeds."""
    with patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("etl.db_utilities.get_s3_client") as mock_get_s3_client:
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client

        download_database_from_s3()

        mock_s3_client.download_file.assert_called_once_with(
            Bucket=S3_BUCKET_NAME,
            Key=SQLITE_DB_NAME,
            Filename=DB_LOCAL_PATH
        )
        mock_custom_logger.assert_called_once_with(
            INFO_LOG_LEVEL,
            f"Successfully downloaded {SQLITE_DB_NAME} from s3://{S3_BUCKET_NAME}/{SQLITE_DB_NAME} to {DB_LOCAL_PATH}"
        )


def test_download_database_from_s3_fail():
    """Test download database from s3 when download fails."""
    with patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("etl.db_utilities.get_s3_client") as mock_get_s3_client:
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client
        mock_s3_client.download_file.side_effect = Exception("Mock download error")

        download_database_from_s3()

        mock_custom_logger.assert_called_once_with(
            WARNING_LOG_LEVEL,
            "Failed to download database from S3: Mock download error"
        )


def test_upload_database_to_s3_success():
    """Test upload_database_to_s3 when upload succeeds."""

    with patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("etl.db_utilities.get_s3_client") as mock_get_s3_client:
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client

        upload_database_to_s3()

        mock_s3_client.upload_file.assert_called_once_with(
            Filename=DB_LOCAL_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=SQLITE_DB_NAME
        )
        mock_custom_logger.assert_called_once_with(
            INFO_LOG_LEVEL,
            f"Successfully uploaded {DB_LOCAL_PATH} to s3://{S3_BUCKET_NAME}/{SQLITE_DB_NAME}"
        )


def test_upload_database_to_s3_fail():
    """Test upload_database_to_s3 when upload fails."""
    with patch("etl.db_utilities.custom_logger") as mock_custom_logger, \
        patch("etl.db_utilities.get_s3_client") as mock_get_s3_client:
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client
        mock_s3_client.upload_file.side_effect = Exception("Mock download error")

        upload_database_to_s3()

        mock_custom_logger.assert_called_once_with(
            ERROR_LOG_LEVEL,
            "Failed to upload database to S3: Mock download error"
        )
