import json

from pydantic import ValidationError

from etl.constants import MINIMUM_ASSESSMENT_YEAR
from etl.validation_models import MunicipalityAssessmentRatio, NYPropertyAssessment


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
    assert "type" not in mar_model
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
    assert mar_model.county_name == "Cayuga"
    assert mar_model.municipality_name == "Auburn"
    assert mar_model.residential_assessment_ratio == 88.00
    assert "type" not in mar_model
    assert "village_name" not in mar_model


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


def test_invalid_municipality_assessment_ratio_rate_year():
    invalid_data = {
        "rate_year": "1980",
        "swis_code": "050100",
        "type": "City",
        "county_name": "Cayuga",
        "municipality_name": "Auburn",
        "residential_assessment_ratio": "88.00"
    }

    try:
        MunicipalityAssessmentRatio(**invalid_data)
    except ValidationError as exc_info:
        for error in exc_info.errors():
            assert error["msg"] == f"Input should be greater than or equal to {MINIMUM_ASSESSMENT_YEAR}"
            assert error["type"] == "greater_than_equal"
            assert error["loc"][0] == "rate_year"


def test_valid_ny_property_record_outputs_expected_data_for_two_tables():
    valid_data = {
        "roll_year": "2024",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "swis_code": "311500",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "print_key_code": "001.1-01-21.0",
        "parcel_address_number": "833",
        "parcel_address_street": "Hiawatha",
        "parcel_address_suff": "Blvd",
        "front": "29",
        "depth": "111.7",
        "full_market_value": "9760",
        "assessment_land": "7550",
        "assessment_total": "0"
    }
    model = NYPropertyAssessment(**valid_data)
    properties_record = model.to_properties_row()
    assert properties_record["id"] == "311500 001.1-01-21.0"
    assert properties_record["swis_code"] == "311500"
    assert properties_record["print_key_code"] == "001.1-01-21.0"
    assert properties_record["municipality_code"] == "311500"
    assert properties_record["municipality_name"] == "Syracuse"
    assert properties_record["county_name"] == "Onondaga"
    assert properties_record["school_district_code"] == "311500"
    assert properties_record["school_district_name"] == "Syracuse"
    assert properties_record["address_street"] == "833 Hiawatha Blvd"
    assert properties_record["address_state"] == NYPropertyAssessment.STATE
    assert len(properties_record) == 10

    onypa_record = model.to_ny_property_assessments_row()
    assert onypa_record["property_id"] == "311500 001.1-01-21.0"
    assert onypa_record["roll_year"] == 2024
    assert onypa_record["property_class"] == 311
    assert onypa_record["property_class_description"] == "Residential Vacant Land"
    assert onypa_record["property_category"] == "Lots and Land"
    assert onypa_record["front"] == 29
    assert onypa_record["depth"] == 111.7
    assert onypa_record["full_market_value"] == 9760
    assert onypa_record["assessment_land"] == 7550
    assert onypa_record["assessment_total"] == 0
    assert len(onypa_record) == 10


def test_valid_ny_property_record_outputs_expected_data_for_two_tables_missing_assessment_totals():
    valid_data = {
        "roll_year": "2024",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "swis_code": "311500",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "print_key_code": "001.1-01-21.0",
        "parcel_address_number": "833",
        "parcel_address_street": "Hiawatha",
        "parcel_address_suff": "Blvd",
        "front": "29",
        "depth": "111.7",
        "full_market_value": "9760"
    }
    model = NYPropertyAssessment(**valid_data)
    properties_record = model.to_properties_row()
    assert properties_record["id"] == "311500 001.1-01-21.0"
    assert properties_record["swis_code"] == "311500"
    assert properties_record["print_key_code"] == "001.1-01-21.0"
    assert properties_record["municipality_code"] == "311500"
    assert properties_record["municipality_name"] == "Syracuse"
    assert properties_record["county_name"] == "Onondaga"
    assert properties_record["school_district_code"] == "311500"
    assert properties_record["school_district_name"] == "Syracuse"
    assert properties_record["address_street"] == "833 Hiawatha Blvd"
    assert properties_record["address_state"] == NYPropertyAssessment.STATE
    assert len(properties_record) == 10

    onypa_record = model.to_ny_property_assessments_row()
    assert onypa_record["property_id"] == "311500 001.1-01-21.0"
    assert onypa_record["roll_year"] == 2024
    assert onypa_record["property_class"] == 311
    assert onypa_record["property_class_description"] == "Residential Vacant Land"
    assert onypa_record["property_category"] == "Lots and Land"
    assert onypa_record["front"] == 29
    assert onypa_record["depth"] == 111.7
    assert onypa_record["full_market_value"] == 9760
    assert onypa_record["assessment_land"] is None
    assert onypa_record["assessment_total"] is None
    assert len(onypa_record) == 10


def test_valid_ny_property_record_missing_optional_parcel_address_parts():
    """When parcel_address_number and parcel_address_suff are not present still valid and still produces address_street."""
    valid_data = {
        "roll_year": "2024",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "swis_code": "311500",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "print_key_code": "001.1-01-21.0",
        "parcel_address_street": "833 Hiawatha Blvd",
        "front": "29",
        "depth": "111.7",
        "full_market_value": "9760"
    }
    model = NYPropertyAssessment(**valid_data)
    properties_record = model.to_properties_row()
    assert properties_record["address_street"] == "833 Hiawatha Blvd"
    assert len(properties_record) == 10


def test_ny_property_assessment_invalid_role_year():
    invalid_data = {
        "roll_year": "1980",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "swis_code": "311500",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "print_key_code": "001.1-01-21.0",
        "parcel_address_number": "833",
        "parcel_address_street": "Hiawatha",
        "parcel_address_suff": "Blvd",
        "front": "29",
        "depth": "111.7",
        "full_market_value": "9760"
    }

    try:
        NYPropertyAssessment(**invalid_data)
    except ValidationError as exc_info:
        for error in exc_info.errors():
            assert error["msg"] == f"Input should be greater than or equal to {MINIMUM_ASSESSMENT_YEAR}"
            assert error["type"] == "greater_than_equal"
            assert error["loc"][0] == "roll_year"


def test_ny_property_assessment_missing_required_values():
    invalid_data = {
        "roll_year": "2024",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "swis_code": "311500",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "parcel_address_number": "833",
        "parcel_address_street": "Hiawatha Blvd",
        "parcel_address_suff": "",
        "front": "29",
        "depth": "111.7"
    }
    try:
        NYPropertyAssessment(**invalid_data)
    except ValidationError as exc_info:
        all_errors = exc_info.errors()
        assert all_errors[0]["msg"] == "Field required"
        assert all_errors[0]["type"] == "missing"
        assert all_errors[0]["loc"][0] == "print_key_code"
        assert all_errors[1]["msg"] == "Field required"
        assert all_errors[1]["type"] == "missing"
        assert all_errors[1]["loc"][0] == "full_market_value"


def test_ny_property_assessment_required_primary_key_value():
    invalid_data = {
        "roll_year": "2024",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "print_key_code": "001.1-01-21.0",
        "parcel_address_number": "833",
        "parcel_address_street": "Hiawatha",
        "parcel_address_suff": "Blvd",
        "front": "29",
        "depth": "111.7",
        "full_market_value": "9760"
    }
    try:
        NYPropertyAssessment(**invalid_data)
    except ValidationError as exc_info:
        all_errors = exc_info.errors()
        assert all_errors[0]["msg"] == "Field required"
        assert all_errors[0]["type"] == "missing"
        assert all_errors[0]["loc"][0] == "swis_code"
        assert len(all_errors) == 1


def test_ny_property_assessment_invalid_primary_key_values():
    invalid_data = {
        "roll_year": "2024",
        "county_name": "Onondaga",
        "municipality_code": "311500",
        "municipality_name": "Syracuse",
        "school_district_code": "311500",
        "school_district_name": "Syracuse",
        "swis_code": "",
        "property_class": "311",
        "property_class_description": "Residential Vacant Land",
        "parcel_address_number": "833",
        "parcel_address_street": "Hiawatha",
        "parcel_address_suff": "Blvd",
        "front": "29",
        "depth": "111.7",
        "full_market_value": "9760"
    }
    try:
        NYPropertyAssessment(**invalid_data)
    except ValidationError as exc_info:
        all_errors = exc_info.errors()
        assert all_errors[0]["msg"] == "String should have at least 6 characters"
        assert all_errors[0]["type"] == "string_too_short"
        assert all_errors[0]["loc"][0] == "swis_code"
        assert all_errors[1]["msg"] == "Field required"
        assert all_errors[1]["type"] == "missing"
        assert all_errors[1]["loc"][0] == "print_key_code"
        assert len(all_errors) == 2
