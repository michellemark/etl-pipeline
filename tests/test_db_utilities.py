import sqlite3
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import mock_open
from unittest.mock import patch

from etl.constants import DB_LOCAL_PATH
from etl.constants import EXTRACTED_DATA_DIR
from etl.constants import GENERATED_DATA_DIR
from etl.constants import GZIPPED_DB_LOCAL_PATH
from etl.constants import GZIPPED_DB_NAME
from etl.constants import INFO_LOG_LEVEL
from etl.constants import LOCAL_VERSION_PATH
from etl.constants import S3_BUCKET_NAME
from etl.constants import VERSION_FILE_NAME
from etl.constants import WARNING_LOG_LEVEL
from etl.constants import ZIPCODE_CACHE_KEY
from etl.constants import ZIPCODE_CACHE_LOCAL_PATH
from etl.db_utilities import create_database
from etl.db_utilities import create_or_update_version_file_and_upload
from etl.db_utilities import download_database_from_s3
from etl.db_utilities import download_zipcodes_cache_from_s3
from etl.db_utilities import ensure_data_directories_exist
from etl.db_utilities import execute_db_query
from etl.db_utilities import get_s3_client
from etl.db_utilities import insert_or_replace_into_database
from etl.db_utilities import upload_database_to_s3
from etl.db_utilities import upload_zipcodes_cache_to_s3


class TestEnsureDataDirectoriesExist:

    @patch('os.path.exists')
    @patch('etl.db_utilities.os.makedirs')
    def test_directory_already_exists(self, mock_makedirs, mock_exists):
        mock_exists.return_value = True

        ensure_data_directories_exist()

        mock_makedirs.assert_any_call(EXTRACTED_DATA_DIR, exist_ok=True)
        mock_makedirs.assert_any_call(GENERATED_DATA_DIR, exist_ok=True)

    @patch('os.path.exists')
    @patch('etl.db_utilities.os.makedirs')
    def test_directory_does_not_exist(self, mock_makedirs, mock_exists):
        mock_exists.return_value = False

        ensure_data_directories_exist()

        mock_makedirs.assert_any_call(EXTRACTED_DATA_DIR, exist_ok=True)
        mock_makedirs.assert_any_call(GENERATED_DATA_DIR, exist_ok=True)


class TestCreateDatabase:

    @patch('etl.db_utilities.ensure_data_directories_exist')
    @patch('etl.db_utilities.sqlite3')
    def test_create_database_success(self, mock_sql, mock_ensure_dirs):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_conn

        create_database()

        mock_ensure_dirs.assert_called()
        mock_sql.connect.assert_called_once_with(DB_LOCAL_PATH)
        mock_conn.cursor.assert_called_once()
        mock_cursor.executescript.assert_called()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('etl.db_utilities.custom_logger')
    @patch('etl.db_utilities.ensure_data_directories_exist')
    @patch('etl.db_utilities.sqlite3')
    def test_create_database_exception(self, mock_sql, mock_ensure_dirs, mock_custom_logger):
        db_error = "Database error"
        mock_sql.connect.side_effect = sqlite3.Error(db_error)
        create_database()
        mock_custom_logger.assert_called_once_with(WARNING_LOG_LEVEL, f"Error creating the database: {db_error}")


class TestInsertOrReplaceIntoDatabase:

    @patch('etl.db_utilities.custom_logger')
    @patch('etl.db_utilities.sqlite3')
    def test_insert_or_replace_success(self, mock_sql, mock_custom_logger):
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_cursor = MagicMock(spec=sqlite3.Cursor)
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_conn
        table_name = "test_table"
        col1 = "column1"
        col2 = "column2"
        column_names = [col1, col2]
        data_row = ("value1", "value2",)
        data = [data_row]

        actual_inserted, actual_failed = insert_or_replace_into_database(table_name, column_names, data)

        assert actual_inserted == 1
        assert actual_failed == 0
        mock_custom_logger.assert_called_once_with(INFO_LOG_LEVEL, f"rows_inserted: 1, rows_failed: 0")
        mock_sql.connect.assert_called_with(DB_LOCAL_PATH)
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_with(f'REPLACE INTO {table_name} ({col1}, {col2}) VALUES (?, ?)', data_row)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_not_called()

    @patch('etl.db_utilities.custom_logger')
    @patch('etl.db_utilities.sqlite3.connect')
    def test_insert_or_replace_connection_error(self, mock_connect, mock_custom_logger):
        mock_connect.side_effect = sqlite3.Error("Database connection failed")
        column_names = ["column1", "column2"]
        table_name = "test_table"
        data_row = ("value1", "value2",)
        data = [data_row]
        actual_inserted, actual_failed = insert_or_replace_into_database(table_name, column_names, data)

        assert actual_inserted == 0
        assert actual_failed == 1
        mock_custom_logger.assert_called_with(WARNING_LOG_LEVEL, "Unexpected database error occurred: Database connection failed")

    @patch('etl.db_utilities.custom_logger')
    @patch('etl.db_utilities.sqlite3')
    def test_insert_or_replace_empty_data(self, mock_sql, mock_custom_logger):
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_cursor = MagicMock(spec=sqlite3.Cursor)
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_conn
        table_name = "test_table"

        actual_inserted, actual_failed = insert_or_replace_into_database(table_name, [], [])

        assert actual_inserted == 0
        assert actual_failed == 0
        mock_custom_logger.assert_called_once_with(INFO_LOG_LEVEL, f"rows_inserted: 0, rows_failed: 0")
        mock_sql.assert_not_called()

    @patch('etl.db_utilities.custom_logger')
    @patch('etl.db_utilities.sqlite3')
    def test_insert_or_replace_different_data_types(self, mock_sql, mock_custom_logger):
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_cursor = MagicMock(spec=sqlite3.Cursor)
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_conn
        table_name = "test_table"
        col1 = "int_col"
        col2 = "float_col"
        col3 = "str_col"
        col4 = "none_col"
        col5 = "bool_col"
        column_names = [col1, col2, col3, col4, col5]
        data_row = (1, 2.5, "text", None, True,)
        data = [data_row]

        actual_inserted, actual_failed = insert_or_replace_into_database(table_name, column_names, data)

        assert actual_inserted == 1
        assert actual_failed == 0
        mock_custom_logger.assert_called_once_with(INFO_LOG_LEVEL, f"rows_inserted: 1, rows_failed: 0")
        mock_sql.connect.assert_called_once_with(DB_LOCAL_PATH)
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_with(
            f'REPLACE INTO {table_name} ({col1}, {col2}, {col3}, {col4}, {col5}) VALUES (?, ?, ?, ?, ?)',
            data_row)
        mock_conn.commit.assert_called_once()


class TestExecuteDbQuery:

    @patch('etl.db_utilities.sqlite3')
    def test_execute_db_query_success(self, mock_sql):
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_cursor = MagicMock(spec=sqlite3.Cursor)
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_sql.connect.return_value = mock_conn
        mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
        query = "SELECT * FROM test_table"

        result = execute_db_query(query)

        assert result == [("result1",), ("result2",)]
        mock_sql.connect.assert_called_once_with(DB_LOCAL_PATH)
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchall.assert_called_once()

    @patch("etl.db_utilities.sqlite3.connect")
    def test_execute_schema_altering_query(self, mock_connect):
        """Test that schema-altering queries return True."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"

        result = execute_db_query(query, fetch_results=False)

        assert result is True
        mock_cursor.execute.assert_called_once_with(query)

    @patch("etl.db_utilities.sqlite3.connect")
    def test_execute_update_query_rowcount(self, mock_connect):
        """Test that non-schema queries return the correct row count."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5  # Simulate 5 rows affected
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        query = "UPDATE test_table SET name = ? WHERE id = ?"
        params = ("John Doe", 1)

        result = execute_db_query(query, params=params, fetch_results=False)

        assert result == 5
        mock_cursor.execute.assert_called_once_with(query, params)

    @patch('etl.db_utilities.sqlite3.connect')
    def test_execute_db_query_with_parameters(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("result1",)]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        query = "SELECT * FROM test_table WHERE id = ?"
        parameters = (1,)

        result = execute_db_query(query, parameters)

        assert result == [("result1",)]
        mock_connect.assert_called_once_with(DB_LOCAL_PATH)
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query, parameters)
        mock_cursor.fetchall.assert_called_once()

    @patch('etl.db_utilities.custom_logger')
    @patch('etl.db_utilities.sqlite3.connect')
    def test_execute_db_query_exception(self, mock_connect, mock_custom_logger):
        ex = "Database error"
        mock_connect.side_effect = sqlite3.Error(ex)
        query = "SELECT * FROM test_table"

        result = execute_db_query(query)
        assert result is None
        mock_connect.assert_called_once_with(DB_LOCAL_PATH)
        mock_custom_logger.assert_called_once_with(WARNING_LOG_LEVEL, f"Query {query} failed, database error: {ex}.")


class TestGetS3Client:

    def test_get_s3_client_success(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "mock_access_key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "mock_secret_key")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        mock_s3_client = MagicMock()

        with patch('boto3.Session') as mock_boto3_session:
            mock_session_instance = MagicMock()
            mock_session_instance.client.return_value = mock_s3_client
            mock_boto3_session.return_value = mock_session_instance

            result = get_s3_client()

            assert result == mock_s3_client
            mock_boto3_session.assert_called_once_with(
                aws_access_key_id="mock_access_key",
                aws_secret_access_key="mock_secret_key",
                region_name="us-east-1"
            )
            mock_session_instance.client.assert_called_once_with('s3')

    @patch("etl.db_utilities.boto3.Session")
    @patch("etl.db_utilities.custom_logger")
    def test_get_s3_client_logs_error_and_returns_none(self, mock_logger, mock_boto3_session, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "mock_access_key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "mock_secret_key")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        mock_boto3_session.side_effect = Exception("S3 client error")

        result = get_s3_client()

        assert result is None
        mock_logger.assert_called_with(WARNING_LOG_LEVEL, "Unable to create S3 client. Skipping operation.")

    @patch("etl.db_utilities.custom_logger")
    def test_missing_aws_env_vars(self, mock_logger, monkeypatch):
        """Test behavior when AWS_ACCESS_KEY_ID is missing."""
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_REGION", raising=False)

        s3_client = get_s3_client()

        assert s3_client is None
        mock_logger.assert_any_call(
            WARNING_LOG_LEVEL, "Missing AWS_ACCESS_KEY_ID environment variable."
        )
        mock_logger.assert_any_call(
            WARNING_LOG_LEVEL, "Missing AWS_SECRET_ACCESS_KEY environment variable."
        )
        mock_logger.assert_any_call(
            WARNING_LOG_LEVEL, "Missing AWS_REGION environment variable."
        )
        mock_logger.assert_any_call(
            WARNING_LOG_LEVEL, "Unable to create S3 client. Skipping operation."
        )


class TestDownloadDatabaseFromS3:

    @patch('etl.db_utilities.get_s3_client')
    @patch('etl.db_utilities.ensure_data_directories_exist')
    @patch('os.path.getsize')
    @patch('os.path.exists')
    @patch('gzip.open')
    @patch('builtins.open', new_callable=mock_open)
    @patch('shutil.copyfileobj')
    def test_download_from_s3_not_cached(self, mock_copyfileobj, mock_file_open, mock_gzip_open,
                                         mock_exists, mock_getsize, mock_ensure_dirs, mock_get_s3_client):
        """
        Test downloading the database from S3 when it is not cached locally.
        """
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3
        mock_exists.return_value = False
        mock_getsize.side_effect = lambda path: {
            GZIPPED_DB_LOCAL_PATH: 38 * 1024 ** 2,  # 38 MB (gzipped file size)
            DB_LOCAL_PATH: 250 * 1024 ** 2  # 250 MB (decompressed file size)
        }.get(path, 0)  # Return 0 if the path is not matched
        mock_gzip_context = MagicMock()
        mock_gzip_open.return_value.__enter__.return_value = mock_gzip_context
        mock_file_context = MagicMock()
        mock_file_open.return_value.__enter__.return_value = mock_file_context

        download_database_from_s3()

        mock_ensure_dirs.assert_called_once()
        mock_get_s3_client.assert_called_once()
        mock_s3.download_file.assert_has_calls([
            call(Bucket=S3_BUCKET_NAME, Key=GZIPPED_DB_NAME, Filename=GZIPPED_DB_LOCAL_PATH),
            call(S3_BUCKET_NAME, VERSION_FILE_NAME, LOCAL_VERSION_PATH),
        ])
        mock_gzip_open.assert_called_once_with(GZIPPED_DB_LOCAL_PATH, 'rb')
        mock_file_open.assert_called_once_with(DB_LOCAL_PATH, 'wb+')
        mock_copyfileobj.assert_called_once_with(mock_gzip_context, mock_file_context)

    @patch('etl.db_utilities.get_s3_client')
    @patch('etl.db_utilities.ensure_data_directories_exist')
    @patch('os.path.exists')
    @patch('etl.db_utilities.custom_logger')
    def test_download_from_s3_s3_download_failure(self, mock_logger, mock_exists, mock_ensure_dirs, mock_get_s3_client):
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3
        mock_exists.return_value = False
        mock_s3.download_file.side_effect = Exception("S3 download error")  # Simulate exception during download

        download_database_from_s3()

        mock_ensure_dirs.assert_called_once()
        mock_logger.assert_called_with(WARNING_LOG_LEVEL, "Failed to download database from S3: S3 download error")

    @patch('etl.db_utilities.get_s3_client')
    @patch('etl.db_utilities.ensure_data_directories_exist')
    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('gzip.open')
    @patch('etl.db_utilities.custom_logger')
    def test_download_from_s3_gzip_open_failure(
            self, mock_custom_logger, mock_gzip_open, mock_getsize,
            mock_exists, mock_ensure_dirs, mock_get_s3_client
    ):
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3
        mock_exists.side_effect = lambda path: path == GZIPPED_DB_LOCAL_PATH  # Gzip file "exists"
        mock_getsize.return_value = 1024 * 1024  # Mock 1MB size for the gzip file
        mock_gzip_open.side_effect = OSError("Error opening Gzip file")

        download_database_from_s3()

        mock_ensure_dirs.assert_called_once()
        mock_s3.download_file.assert_called_once_with(
            Bucket=S3_BUCKET_NAME,
            Key=GZIPPED_DB_NAME,
            Filename=GZIPPED_DB_LOCAL_PATH
        )
        mock_gzip_open.assert_called_once_with(GZIPPED_DB_LOCAL_PATH, 'rb')
        mock_custom_logger.assert_called_once_with(
            WARNING_LOG_LEVEL, "Failed to download database from S3: Error opening Gzip file"
        )


class TestCreateOrUpdateVersionFileAndUpload:

    @patch("etl.db_utilities.get_s3_client")
    @patch("builtins.open", new_callable=mock_open)
    def test_increment_existing_version(self, mock_open, mock_get_s3_client):
        """Test incrementing version when the file already exists and is valid."""
        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.return_value = "5"
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3

        create_or_update_version_file_and_upload()

        mock_open.assert_called_once_with(LOCAL_VERSION_PATH, "a+")
        mock_file.seek.assert_called_once_with(0)
        mock_file.read.assert_called_once()
        mock_file.write.assert_called_once_with("6")
        mock_s3.upload_file.assert_called_once_with(
            Filename=LOCAL_VERSION_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=VERSION_FILE_NAME,
        )

    @patch("etl.db_utilities.get_s3_client")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_new_version_file(self, mock_open, mock_get_s3_client):
        """Test creating a new version file when it doesn't have valid data."""
        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.return_value = ""
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3

        create_or_update_version_file_and_upload()

        mock_open.assert_called_once_with(LOCAL_VERSION_PATH, "a+")
        mock_file.seek.assert_called_once_with(0)
        mock_file.read.assert_called_once()
        mock_file.write.assert_called_once_with("1")
        mock_s3.upload_file.assert_called_once_with(
            Filename=LOCAL_VERSION_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=VERSION_FILE_NAME,
        )

    @patch("etl.db_utilities.get_s3_client")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.db_utilities.custom_logger")
    def test_handle_invalid_version(self, mock_logger, mock_open, mock_get_s3_client):
        """Test handling an invalid version (e.g., corrupt file contents)."""
        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.return_value = "invalid_version"
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3

        create_or_update_version_file_and_upload()

        mock_open.assert_called_once_with(LOCAL_VERSION_PATH, "a+")
        mock_file.seek.assert_called_once_with(0)
        mock_file.read.assert_called_once()
        mock_file.write.assert_called_once_with("1")
        mock_s3.upload_file.assert_called_once_with(
            Filename=LOCAL_VERSION_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=VERSION_FILE_NAME,
        )
        mock_logger.assert_called_with(INFO_LOG_LEVEL,
                                       f'Successfully uploaded version 1 to s3://{S3_BUCKET_NAME}/{VERSION_FILE_NAME}')

    @patch("etl.db_utilities.get_s3_client")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.db_utilities.custom_logger")
    def test_s3_upload_failure(self, mock_logger, mock_open, mock_get_s3_client):
        """Test exception handling when the S3 upload fails."""
        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.return_value = "5"
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = Exception("Simulated S3 upload failure")  # Simulate upload failure
        mock_get_s3_client.return_value = mock_s3

        create_or_update_version_file_and_upload()

        mock_open.assert_called_once_with(LOCAL_VERSION_PATH, "a+")
        mock_file.seek.assert_called_once_with(0)
        mock_file.read.assert_called_once()
        mock_file.write.assert_called_once_with("6")
        mock_s3.upload_file.assert_called_once_with(
            Filename=LOCAL_VERSION_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=VERSION_FILE_NAME,
        )
        mock_logger.assert_any_call(WARNING_LOG_LEVEL, "Failed to upload version 6 to S3: Simulated S3 upload failure")

    @patch("etl.db_utilities.get_s3_client")
    @patch("etl.db_utilities.custom_logger")
    def test_no_s3_client_available(self, mock_logger, mock_get_s3_client):
        """Test behavior when no S3 client is available."""
        mock_get_s3_client.return_value = None

        with patch("builtins.open", mock_open()):
            create_or_update_version_file_and_upload()

        mock_logger.assert_called_once_with(
            WARNING_LOG_LEVEL,
            "Failed to upload version 1 to S3: 'NoneType' object has no attribute 'upload_file'"
        )


class TestUploadDatabaseToS3:

    @patch('etl.db_utilities.create_or_update_version_file_and_upload')
    @patch('etl.db_utilities.get_s3_client')
    @patch('os.path.exists')
    @patch('gzip.open', new_callable=mock_open)
    @patch('builtins.open', new_callable=mock_open)
    @patch('shutil.copyfileobj')
    @patch('etl.db_utilities.custom_logger')
    def test_upload_database_success(
            self,
            mock_logger,
            mock_copyfileobj,
            mock_file_open,
            mock_gzip_open,
            mock_path_exists,
            mock_get_s3_client,
            mock_create_version_file,
    ):
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client
        mock_path_exists.side_effect = lambda filepath: filepath == DB_LOCAL_PATH  # Only DB_LOCAL_PATH "exists"
        mock_file_open_instance = mock_file_open.return_value.__enter__.return_value
        mock_gzip_open_instance = mock_gzip_open.return_value.__enter__.return_value

        upload_database_to_s3()

        mock_logger.assert_any_call(INFO_LOG_LEVEL, "Uploading database to S3...")
        mock_file_open.assert_called_once_with(DB_LOCAL_PATH, 'rb')
        mock_gzip_open.assert_called_once_with(GZIPPED_DB_LOCAL_PATH, 'wb+')
        mock_copyfileobj.assert_called_once_with(mock_file_open_instance, mock_gzip_open_instance)
        mock_s3_client.upload_file.assert_called_once_with(
            Filename=GZIPPED_DB_LOCAL_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=GZIPPED_DB_NAME,
        )
        mock_create_version_file.assert_called_once()
        mock_logger.assert_any_call(
            INFO_LOG_LEVEL,
            f"Successfully uploaded {GZIPPED_DB_LOCAL_PATH} to s3://{S3_BUCKET_NAME}/{GZIPPED_DB_NAME}",
        )

    @patch('etl.db_utilities.create_or_update_version_file_and_upload')
    @patch('etl.db_utilities.get_s3_client')
    @patch('gzip.open')
    @patch('builtins.open', new_callable=mock_open)
    @patch('shutil.copyfileobj')
    @patch('etl.db_utilities.custom_logger')
    def test_upload_database_s3_upload_failure(
            self,
            mock_logger,
            mock_copyfileobj,
            mock_file_open,
            mock_gzip_open,
            mock_get_s3_client,
            mock_create_version_file,
    ):
        mock_s3_client = MagicMock()
        mock_s3_client.upload_file.side_effect = Exception("Simulated S3 upload failure")
        mock_get_s3_client.return_value = mock_s3_client

        upload_database_to_s3()

        mock_logger.assert_any_call(WARNING_LOG_LEVEL, "Failed to upload database to S3: Simulated S3 upload failure")

    @patch('etl.db_utilities.get_s3_client')
    @patch('gzip.open')
    @patch('builtins.open', new_callable=mock_open)
    @patch('shutil.copyfileobj')
    @patch('etl.db_utilities.custom_logger')
    def test_upload_database_gzip_failure(
            self,
            mock_logger,
            mock_copyfileobj,
            mock_file_open,
            mock_gzip_open,
            mock_get_s3_client,
    ):
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client
        mock_gzip_open.side_effect = OSError("Simulated Gzip failure")

        upload_database_to_s3()

        mock_logger.assert_any_call(WARNING_LOG_LEVEL, "Failed to upload database to S3: Simulated Gzip failure")


class TestDownloadZipcodesCacheFromS3:

    @patch("etl.db_utilities.custom_logger")
    @patch("etl.db_utilities.ensure_data_directories_exist")
    @patch("etl.db_utilities.get_s3_client")
    def test_download_success(
            self, mock_get_s3_client, mock_ensure_dirs, mock_logger
    ):
        """Test if zipcodes cache is successfully downloaded from S3."""
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client

        download_zipcodes_cache_from_s3()

        mock_ensure_dirs.assert_called_once()
        mock_s3_client.download_file.assert_called_once_with(
            Bucket=S3_BUCKET_NAME,
            Key=ZIPCODE_CACHE_KEY,
            Filename=ZIPCODE_CACHE_LOCAL_PATH,
        )
        mock_logger.assert_called_once_with(
            INFO_LOG_LEVEL,
            f"Successfully downloaded {ZIPCODE_CACHE_KEY} from s3://{S3_BUCKET_NAME}/{ZIPCODE_CACHE_KEY} to {ZIPCODE_CACHE_LOCAL_PATH}",
        )

    @patch("etl.db_utilities.custom_logger")
    @patch("etl.db_utilities.ensure_data_directories_exist")
    @patch("etl.db_utilities.get_s3_client")
    def test_download_failure(
            self, mock_get_s3_client, mock_ensure_dirs, mock_logger
    ):
        """Test behavior when the download fails."""
        mock_s3_client = MagicMock()
        mock_s3_client.download_file.side_effect = Exception("S3 download error")
        mock_get_s3_client.return_value = mock_s3_client

        download_zipcodes_cache_from_s3()

        mock_ensure_dirs.assert_called_once()
        mock_logger.assert_called_once_with(
            WARNING_LOG_LEVEL,
            "Failed to download zipcodes cache from S3: S3 download error",
        )

    @patch("etl.db_utilities.custom_logger")
    @patch("etl.db_utilities.ensure_data_directories_exist")
    @patch("etl.db_utilities.get_s3_client")
    def test_no_s3_client(self, mock_get_s3_client, mock_ensure_dirs, mock_logger):
        """Test behavior when no S3 client is available."""
        mock_get_s3_client.return_value = None

        download_zipcodes_cache_from_s3()

        mock_logger.assert_not_called()


class TestUploadZipcodesCacheToS3:

    @patch("etl.db_utilities.custom_logger")
    @patch("etl.db_utilities.get_s3_client")
    def test_upload_success(self, mock_get_s3_client, mock_logger):
        """Test successful upload of zipcodes cache."""
        mock_s3_client = MagicMock()
        mock_get_s3_client.return_value = mock_s3_client

        upload_zipcodes_cache_to_s3()

        mock_s3_client.upload_file.assert_called_once_with(
            Filename=ZIPCODE_CACHE_LOCAL_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=ZIPCODE_CACHE_KEY,
        )
        mock_logger.assert_called_once_with(
            INFO_LOG_LEVEL,
            f"Successfully uploaded {ZIPCODE_CACHE_LOCAL_PATH} to s3://{S3_BUCKET_NAME}/{ZIPCODE_CACHE_KEY}",
        )

    @patch("etl.db_utilities.custom_logger")
    @patch("etl.db_utilities.get_s3_client")
    def test_upload_failure(self, mock_get_s3_client, mock_logger):
        """Test behavior when upload fails due to an exception."""
        mock_s3_client = MagicMock()
        mock_s3_client.upload_file.side_effect = Exception("S3 upload error")
        mock_get_s3_client.return_value = mock_s3_client

        upload_zipcodes_cache_to_s3()

        mock_s3_client.upload_file.assert_called_once_with(
            Filename=ZIPCODE_CACHE_LOCAL_PATH,
            Bucket=S3_BUCKET_NAME,
            Key=ZIPCODE_CACHE_KEY,
        )
        mock_logger.assert_called_once_with(
            WARNING_LOG_LEVEL,
            "Failed to upload zipcodes cache to S3: S3 upload error",
        )

    @patch("etl.db_utilities.custom_logger")
    @patch("etl.db_utilities.get_s3_client")
    def test_no_s3_client(self, mock_get_s3_client, mock_logger):
        """Test behavior when no S3 client is available."""
        mock_get_s3_client.return_value = None

        upload_zipcodes_cache_to_s3()

        mock_logger.assert_not_called()
