import json

import pytest
from pydantic import ValidationError

from etl.constants import ASSESSMENT_YEAR_SOUGHT
from etl.validation_models import MunicipalityAssessmentRatio


def test_valid_municipality_assessment_ratio_without_village_name():
    valid_data = {
        "rate_year": "2024",
        "swis_code": "050100",
        "type": "City",
        "county_name": "Cayuga",
        "municipality_name": "Auburn",
        "residential_assessment_ratio": "88.00"
    }
    mar_model = MunicipalityAssessmentRatio(**valid_data)

    assert mar_model.rate_year == 2024
    assert mar_model.swis_code == "050100"
    assert mar_model.type == "City"
    assert mar_model.county_name == "Cayuga"
    assert mar_model.municipality_name == "Auburn"
    assert mar_model.residential_assessment_ratio == 88.00


def test_valid_municipality_assessment_ratio_with_village_name():
    valid_data = {
        "rate_year": "2024",
        "swis_code": "050100",
        "type": "City",
        "county_name": "Cayuga",
        "municipality_name": "Auburn",
        "village_name": "Auburn",
        "residential_assessment_ratio": "88.00"
    }
    mar_model = MunicipalityAssessmentRatio(**valid_data)

    assert mar_model.rate_year == 2024
    assert mar_model.swis_code == "050100"
    assert mar_model.type == "City"
    assert mar_model.county_name == "Cayuga"
    assert mar_model.municipality_name == "Auburn"
    assert mar_model.residential_assessment_ratio == 88.00
    assert mar_model.village_name == "Auburn"


def test_valid_municipality_assessment_ratio_without_village_dumps_to_expected_json():
    valid_data = {
        "rate_year": "2024",
        "swis_code": "050100",
        "type": "City",
        "county_name": "Cayuga",
        "municipality_name": "Auburn",
        "residential_assessment_ratio": "88.00"
    }
    mar_model = MunicipalityAssessmentRatio(**valid_data)

    ratio_data = json.loads(mar_model.model_dump_json(by_alias=True))
    assert list(ratio_data.keys()) == [
        "rate_year",
        "municipality_code",
        "county_name",
        "municipality_name",
        "residential_assessment_ratio"
    ]
    assert tuple(ratio_data.values()) == (
        2024,
        "050100",
        "Cayuga",
        "Auburn",
        88.00
    )


def test_valid_municipality_assessment_ratio_with_village_dumps_to_expected_json():
    valid_data = {
        "rate_year": "2024",
        "swis_code": "050100",
        "type": "City",
        "county_name": "Cayuga",
        "municipality_name": "Auburn",
        "village_name": "Auburn",
        "residential_assessment_ratio": "88.00"
    }
    mar_model = MunicipalityAssessmentRatio(**valid_data)

    ratio_data = json.loads(mar_model.model_dump_json(by_alias=True))
    assert list(ratio_data.keys()) == [
        "rate_year",
        "municipality_code",
        "county_name",
        "municipality_name",
        "residential_assessment_ratio"
    ]
    assert tuple(ratio_data.values()) == (
        2024,
        "050100",
        "Cayuga",
        "Auburn",
        88.00
    )


def test_invalid_rate_year():
    invalid_data = {
        "rate_year": "1980",
        "swis_code": "050100",
        "type": "City",
        "county_name": "Cayuga",
        "municipality_name": "Auburn",
        "residential_assessment_ratio": "88.00"
    }

    try:
        MunicipalityAssessmentRatio(**invalid_data).model_validate()
    except ValidationError as exc_info:
        for error in exc_info.errors():
            assert error["msg"] == f"Input should be greater than or equal to {ASSESSMENT_YEAR_SOUGHT}"
            assert error["type"] == "greater_than_equal"
            assert error["loc"][0] == "rate_year"
