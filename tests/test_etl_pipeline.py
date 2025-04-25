from unittest.mock import patch

from etl.constants import ERROR_LOG_LEVEL
from etl.etl_pipeline import cny_real_estate_etl_workflow


@patch("etl.etl_pipeline.custom_logger")
@patch("etl.etl_pipeline.get_open_ny_app_token")
def test_workflow_when_open_ny_app_token_fails(
    mock_token,
    mock_logger
):
    """Test workflow when token retrieval fails."""
    mock_token.return_value = None

    cny_real_estate_etl_workflow()

    mock_logger.assert_called_once_with(
        ERROR_LOG_LEVEL,
        "Cannot proceed, unable to get Open NY app token, ending ETL workflow.")


@patch('etl.etl_pipeline.get_zipcodes_cache_as_json')
@patch("os.path.exists")
@patch("etl.etl_pipeline.custom_logger")
@patch("etl.etl_pipeline.get_open_ny_app_token")
@patch("etl.etl_pipeline.download_database_from_s3")
@patch("etl.etl_pipeline.create_database")
@patch("etl.etl_pipeline.fetch_property_assessments")
@patch("etl.etl_pipeline.fetch_municipality_assessment_ratios")
@patch("etl.etl_pipeline.get_assessment_year_to_query")
@patch("etl.etl_pipeline.save_municipality_assessment_ratios")
@patch("etl.etl_pipeline.upload_database_to_s3")
def test_workflow_with_municipal_assessment_ratios_not_fetched(
    mock_upload,
    mock_save,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists,
    mock_get_zipcodes_cache_as_json
):
    """Test workflow with valid token but database not already in s3 calls to create db."""
    mock_assessment_year.return_value = 2025
    mock_token.return_value = "valid_token"
    mock_path_exists.return_value = False
    mock_fetch.return_value = None
    mock_fetch_properties_and_assessments.return_value = None

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_called_once()
    mock_fetch.assert_called_once_with(app_token="valid_token", query_year=2025)
    mock_fetch_properties_and_assessments.assert_called_once_with(app_token="valid_token", query_year=2025)
    mock_save.assert_not_called()
    mock_get_zipcodes_cache_as_json.assert_not_called()
    mock_upload.assert_called_once()


@patch('etl.etl_pipeline.upload_database_to_s3')
@patch('etl.etl_pipeline.fetch_property_assessments')
@patch('etl.etl_pipeline.save_municipality_assessment_ratios')
@patch('etl.etl_pipeline.fetch_municipality_assessment_ratios')
@patch('etl.etl_pipeline.create_database')
@patch('etl.etl_pipeline.download_database_from_s3')
@patch('etl.etl_pipeline.get_assessment_year_to_query')
@patch('etl.etl_pipeline.get_open_ny_app_token')
@patch('etl.etl_pipeline.custom_logger')
@patch('os.path.exists')
def test_workflow_when_database_creation_fails(
    mock_path_exists,
    mock_logger,
    mock_token,
    mock_assessment_year,
    mock_download,
    mock_create_db,
    mock_fetch,
    mock_save_ratios,
    mock_fetch_properties_and_assessments,
    mock_upload):
    """Test workflow when database creation fails."""
    mock_token.return_value = "mock_token"
    mock_assessment_year.return_value = 2023
    mock_path_exists.return_value = False
    mock_data = [{"mock": "data"}]
    mock_fetch.return_value = mock_data
    mock_fetch_properties_and_assessments.return_value = mock_data

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_called_once()
    mock_logger.assert_called_with(
        ERROR_LOG_LEVEL,
        "Cannot proceed, database creation failed, ending ETL workflow.")
    mock_fetch.assert_not_called()
    mock_save_ratios.assert_not_called()
    mock_fetch_properties_and_assessments.assert_not_called()
    mock_upload.assert_not_called()


@patch('etl.etl_pipeline.get_zipcodes_cache_as_json')
@patch('etl.etl_pipeline.upload_database_to_s3')
@patch('etl.etl_pipeline.fetch_property_assessments')
@patch('etl.etl_pipeline.save_municipality_assessment_ratios')
@patch('etl.etl_pipeline.fetch_municipality_assessment_ratios')
@patch('etl.etl_pipeline.create_database')
@patch('etl.etl_pipeline.download_database_from_s3')
@patch('etl.etl_pipeline.get_assessment_year_to_query')
@patch('etl.etl_pipeline.get_open_ny_app_token')
@patch('etl.etl_pipeline.custom_logger')
@patch('os.path.exists')
def test_workflow_with_successful_database_creation(
    mock_path_exists,
    mock_logger,
    mock_token,
    mock_assessment_year,
    mock_download,
    mock_create_db,
    mock_fetch,
    mock_save_ratios,
    mock_fetch_properties_and_assessments,
    mock_upload,
    mock_get_zipcodes_cache_as_json):
    """Test workflow with successful database creation."""
    mock_token.return_value = "mock_token"
    mock_assessment_year.return_value = 2023
    mock_get_zipcodes_cache_as_json.return_value = {}

    # Simulate database doesn't exist initially but is created successfully
    mock_path_exists.side_effect = [False, True]
    mock_data = [{"mock": "data"}]
    mock_fetch.return_value = mock_data
    mock_fetch_properties_and_assessments.return_value = mock_data

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_called_once()
    mock_fetch.assert_not_called()
    mock_save_ratios.assert_not_called()
    mock_fetch_properties_and_assessments.assert_not_called()
    mock_upload.assert_not_called()


@patch('etl.etl_pipeline.get_zipcodes_cache_as_json')
@patch("os.path.exists")
@patch("etl.etl_pipeline.custom_logger")
@patch("etl.etl_pipeline.get_open_ny_app_token")
@patch("etl.etl_pipeline.download_database_from_s3")
@patch("etl.etl_pipeline.create_database")
@patch("etl.etl_pipeline.fetch_property_assessments")
@patch("etl.etl_pipeline.fetch_municipality_assessment_ratios")
@patch("etl.etl_pipeline.get_assessment_year_to_query")
@patch("etl.etl_pipeline.save_municipality_assessment_ratios")
@patch("etl.etl_pipeline.upload_database_to_s3")
def test_workflow_with_municipal_assessment_ratios_not_fetched(
    mock_upload,
    mock_save,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists,
    mock_get_zipcodes_cache_as_json
):
    """Test workflow when no assessment ratios are fetched."""
    mock_assessment_year.return_value = 2025
    mock_token.return_value = "valid_token"
    mock_path_exists.return_value = True
    mock_fetch.return_value = None
    mock_fetch_properties_and_assessments.return_value = 0
    mock_get_zipcodes_cache_as_json.return_value = {}

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_called_once()
    mock_fetch.assert_called_once_with(app_token="valid_token", query_year=2025)
    mock_fetch_properties_and_assessments.assert_called_once_with(app_token="valid_token", query_year=2025)
    mock_save.assert_not_called()
    mock_upload.assert_called_once()
    mock_get_zipcodes_cache_as_json.assert_not_called()


@patch('etl.etl_pipeline.get_zipcodes_cache_as_json')
@patch("os.path.exists")
@patch("etl.etl_pipeline.custom_logger")
@patch("etl.etl_pipeline.get_open_ny_app_token")
@patch("etl.etl_pipeline.download_database_from_s3")
@patch("etl.etl_pipeline.create_database")
@patch("etl.etl_pipeline.fetch_property_assessments")
@patch("etl.etl_pipeline.fetch_municipality_assessment_ratios")
@patch("etl.etl_pipeline.get_assessment_year_to_query")
@patch("etl.etl_pipeline.save_municipality_assessment_ratios")
@patch("etl.etl_pipeline.upload_database_to_s3")
def test_workflow_with_no_municipal_assessment_ratios_properties_found(
    mock_upload,
    mock_save_ratios,
    mock_assessment_year,
    mock_fetch,
    mock_fetch_properties_and_assessments,
    mock_create_db,
    mock_download,
    mock_token,
    mock_logger,
    mock_path_exists,
    mock_get_zipcodes_cache_as_json
):
    """Test workflow when assessment ratios are fetched."""
    mock_assessment_year.return_value = 2027
    mock_data = [{"a": "b"}, {"c": "d"}]
    mock_token.return_value = "valid_token"
    mock_path_exists.return_value = True
    mock_fetch.return_value = mock_data
    mock_fetch_properties_and_assessments.return_value = mock_data
    mock_get_zipcodes_cache_as_json.return_value = {}

    cny_real_estate_etl_workflow()

    mock_download.assert_called_once()
    mock_create_db.assert_called_once()
    mock_fetch.assert_called_once_with(app_token="valid_token", query_year=2027)
    mock_save_ratios.assert_called_once_with(mock_data)
    mock_fetch_properties_and_assessments.assert_called_once_with(app_token="valid_token", query_year=2027)
    mock_get_zipcodes_cache_as_json.assert_called_once()
    mock_upload.assert_called_once()
