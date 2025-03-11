from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from etl.constants import MINIMUM_ASSESSMENT_YEAR
from etl.constants import WARNING_LOG_LEVEL
from etl.property_utilities import get_assessment_year_to_query
from etl.property_utilities import get_ny_property_category_for_property_class
from etl.property_utilities import get_ny_property_classes_for_where_clause
from etl.property_utilities import get_open_ny_app_token

# Mock Constants so only testing functionality not definitions, which may change, nbd
SINGLE_FAMILY_HOUSE = "SFHTEST"
MULTI_FAMILY_RESIDENCE = "MFRTEST"
COMMERCIAL_PROPERTY = "CPTEST"
OTHER_PROPERTY_CATEGORY = "OPTEST"
SFH_CLASS = 210
MFR_CLASS = 220
CP_CLASS = 300
OPEN_NY_PROPERTY_CLASS_MAP = [
    {
        "property_class": SFH_CLASS,
        "property_class_description": "One Family Year-Round Residence",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": MFR_CLASS,
        "property_class_description": "Two Family Year-Round Residence",
        "property_category": MULTI_FAMILY_RESIDENCE
    },
    {
        "property_class": CP_CLASS,
        "property_class_description": "Vacant Land",
        "property_category": COMMERCIAL_PROPERTY
    },
]
DESIRED_PROPERTY_CATEGORIES = [SINGLE_FAMILY_HOUSE, MULTI_FAMILY_RESIDENCE]
SFH_DESCRIPTION = "Single Family House"
MFR_DESCRIPTION = "Multi Family Residence"
CP_DESCRIPTION = "Commercial Property"
OPC_DESCRIPTION = "Other Property Category"
PROPERTY_CATEGORY_DESCRIPTIONS = {
    SINGLE_FAMILY_HOUSE: SFH_DESCRIPTION,
    MULTI_FAMILY_RESIDENCE: MFR_DESCRIPTION,
    COMMERCIAL_PROPERTY: CP_DESCRIPTION,
    OTHER_PROPERTY_CATEGORY: OPC_DESCRIPTION
}


@pytest.fixture(autouse=True, scope="function")
def patch_constants(monkeypatch):
    monkeypatch.setattr("etl.property_utilities.OPEN_NY_PROPERTY_CLASS_MAP", OPEN_NY_PROPERTY_CLASS_MAP)
    monkeypatch.setattr("etl.property_utilities.OTHER_PROPERTY_CATEGORY", OTHER_PROPERTY_CATEGORY)
    monkeypatch.setattr("etl.property_utilities.DESIRED_PROPERTY_CATEGORIES", DESIRED_PROPERTY_CATEGORIES)
    monkeypatch.setattr("etl.property_utilities.PROPERTY_CATEGORY_DESCRIPTIONS", PROPERTY_CATEGORY_DESCRIPTIONS)


def test_ny_property_category_for_property_class_valid_property_class():
    """Test property_class in ny class map returns mapped category."""
    assert get_ny_property_category_for_property_class(SFH_CLASS) == SFH_DESCRIPTION
    assert get_ny_property_category_for_property_class(MFR_CLASS) == MFR_DESCRIPTION
    assert get_ny_property_category_for_property_class(CP_CLASS) == CP_DESCRIPTION


def test_ny_property_category_for_property_class_invalid_property_class():
    """Test property_class does not exist in ny class map category defaults to other."""
    assert get_ny_property_category_for_property_class(999) == OPC_DESCRIPTION


def test_ny_property_category_for_property_class_missing_category():
    """Test when property_category key is missing in ny class map category defaults to other."""
    OPEN_NY_PROPERTY_CLASS_MAP.append({"property_class": 400})
    assert get_ny_property_category_for_property_class(400) == OPC_DESCRIPTION
    OPEN_NY_PROPERTY_CLASS_MAP.pop()  # Clean up


def test_ny_property_category_for_property_class_empty_property_class_map(monkeypatch):
    """Test when ny class map is empty category defaults to other."""
    monkeypatch.setattr("etl.property_utilities.OPEN_NY_PROPERTY_CLASS_MAP", [])
    assert get_ny_property_category_for_property_class(SFH_CLASS) == OPC_DESCRIPTION


def test_ny_property_category_for_property_class_valid_where_clause():
    """Test valid WHERE clause created based on DESIRED_PROPERTY_CATEGORIES."""
    result = get_ny_property_classes_for_where_clause()
    assert result == 'property_class IN ("210", "220")'


def test_ny_property_category_for_property_class_no_matching_categories(monkeypatch):
    """Test no matching categories from DESIRED_PROPERTY_CATEGORIES returns empty string."""
    monkeypatch.setattr("etl.property_utilities.DESIRED_PROPERTY_CATEGORIES", ["NON_EXISTENT_CATEGORY"])
    result = get_ny_property_classes_for_where_clause()
    assert result == ""


def test_ny_property_category_for_property_class_empty_open_ny_map(monkeypatch):
    """Test when OPEN_NY_PROPERTY_CLASS_MAP is empty returns empty string."""
    monkeypatch.setattr("etl.property_utilities.OPEN_NY_PROPERTY_CLASS_MAP", [])
    result = get_ny_property_classes_for_where_clause()
    assert result == ""


def test_ny_property_category_for_property_class_empty_desired_categories(monkeypatch):
    """Test when DESIRED_PROPERTY_CATEGORIES is empty returns empty string."""
    monkeypatch.setattr("etl.property_utilities.DESIRED_PROPERTY_CATEGORIES", [])
    result = get_ny_property_classes_for_where_clause()
    assert result == ""


def test_get_open_ny_app_token_success():
    """Test when the token is in the right environment variable."""
    with patch("os.environ.get") as mock_env_get, \
        patch("etl.property_utilities.custom_logger") as mock_custom_logger:
        mock_env_get.side_effect = lambda key: {"OPEN_DATA_APP_TOKEN": "mock_app_token"}.get(key)
        token = get_open_ny_app_token()
        assert token == "mock_app_token"
        mock_custom_logger.assert_not_called()


def test_get_open_ny_app_token_fails():
    """Test when the token is not in the right environment variable."""
    with patch("os.environ.get") as mock_env_get, \
        patch("etl.property_utilities.custom_logger") as mock_custom_logger:
        mock_env_get.return_value = None
        token = get_open_ny_app_token()
        assert token is None
        mock_custom_logger.assert_called_once_with(
            WARNING_LOG_LEVEL, "Missing OPEN_DATA_APP_TOKEN environment variable.")


def test_get_assessment_year_to_query_before_august_minimum_assessment_year():
    """Test get_assessment_year_to_query returns previous year when current month is before August,
    except in the case where the current year is the minimum assessment year."""
    with patch("etl.property_utilities.datetime") as mock_datetime:
        now = MagicMock()
        now.year = MINIMUM_ASSESSMENT_YEAR
        now.month = 4
        mock_datetime.now.return_value = now
        assessment_year = get_assessment_year_to_query()
        assert assessment_year == MINIMUM_ASSESSMENT_YEAR


def test_get_assessment_year_to_query_before_august():
    """Test get_assessment_year_to_query returns previous year when current month is before August."""
    with patch("etl.property_utilities.datetime") as mock_datetime:
        now = MagicMock()
        now.year = 2025
        now.month = 6
        mock_datetime.now.return_value = now
        assessment_year = get_assessment_year_to_query()
        assert assessment_year == 2024


def test_get_assessment_year_to_query_after_august():
    """Test get_assessment_year_to_query returns current year when current month is after August."""
    with patch("etl.property_utilities.datetime") as mock_datetime:
        now = MagicMock()
        now.year = 2025
        now.month = 8
        mock_datetime.now.return_value = now
        assessment_year = get_assessment_year_to_query()
        assert assessment_year == 2025
