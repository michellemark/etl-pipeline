import os

# ******* Real estate API values ********************************
CNY_COUNTY_LIST = ["Cayuga", "Cortland", "Madison", "Onondaga", "Oswego"]
OPEN_NY_BASE_URL = "data.ny.gov"
OPEN_NY_EARLIEST_YEAR = 2009
OPEN_NY_ASSESSMENT_RATIOS_API_ID = "bsmp-6um6"
OPEN_NY_CALLS_PER_PERIOD = 3
OPEN_NY_RATE_LIMIT_PERIOD = 60

# ******* File paths and names ***********************************
CURRENT_FILE_PATH = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_FILE_PATH))
EXTRACTED_DATA_DIR = os.path.join(PROJECT_ROOT, "extracted")
GENERATED_DATA_DIR = os.path.join(PROJECT_ROOT, "generated")
SQLITE_DB_NAME = "cny-real-estate.db"
DB_LOCAL_PATH = os.path.join(GENERATED_DATA_DIR, SQLITE_DB_NAME)
S3_BUCKET_NAME = "cny-realestate-data"
CREATE_TABLE_DEFINITIONS_FILE_PATH = os.path.join(
    PROJECT_ROOT,
    "sql",
    "create_table_definitions.sql"
)

# ******* Table names *********************************************
ASSESSMENT_RATIOS_TABLE = "municipality_assessment_ratios"
NY_PROPERTY_ASSESSMENTS_TABLE = "ny_property_assessments"
PROPERTIES_TABLE = "properties"

# ******* Log levels **********************************************
CRITICAL_LOG_LEVEL = "critical"
ERROR_LOG_LEVEL = "error"
WARNING_LOG_LEVEL = "warning"
INFO_LOG_LEVEL = "info"
DEBUG_LOG_LEVEL = "debug"
ALLOWED_LOG_LEVELS = [
    CRITICAL_LOG_LEVEL,
    ERROR_LOG_LEVEL,
    WARNING_LOG_LEVEL,
    INFO_LOG_LEVEL,
    DEBUG_LOG_LEVEL
]
