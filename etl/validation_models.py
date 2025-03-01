from decimal import Decimal
from typing import ClassVar
from typing import Optional

from pydantic import BaseModel, Field

from etl.constants import ALL_PROPERTIES_STATE
from etl.constants import MINIMUM_ASSESSMENT_YEAR


class MunicipalityAssessmentRatio(BaseModel):
    rate_year: int = Field(
        ge=MINIMUM_ASSESSMENT_YEAR,
        description="Rate Year of assessment ratio"
    )
    swis_code: str = Field(
        min_length=6,
        serialization_alias="municipality_code",
        description="Six-digit code that uniquely identifies each municipality"
    )
    county_name: str = Field(
        min_length=6,
        description="Name of County Municipality is in"
    )
    municipality_name: str = Field(
        min_length=2,
        description="Name of Municipality"
    )
    residential_assessment_ratio: Decimal = Field(
        decimal_places=2,
        description="Ratio of assessed value compared to full value of that property"
    )

    class Config:
        # Ensure Decimal values are dumped as floats
        json_encoders = {
            Decimal: lambda v: float(v)
        }

        # Ignore all fields not defined
        extra = "ignore"


class NYPropertyAssessment(BaseModel):
    STATE: ClassVar[str] = ALL_PROPERTIES_STATE

    roll_year: int = Field(
        ge=MINIMUM_ASSESSMENT_YEAR,
        description="Calendar year in which assessment was filed."
    )
    county_name: str = Field(
        min_length=6,
        description="Name of county in which parcel resides."
    )
    municipality_code: str = Field(
        min_length=6,
        description="Six-digit code that uniquely identifies each municipality"
    )
    municipality_name: str = Field(
        min_length=2,
        description="Name of city or town in which parcel resides"
    )
    school_district_code: str = Field(
        min_length=6,
        description="Six-digit code that uniquely identifies each school district"
    )
    school_district_name: str = Field(
        min_length=2,
        description="Name of school district in which parcel resides"
    )
    swis_code: str = Field(
        min_length=6,
        description="Six-digit code that identifies subsections of a city or town, such as a village that resides within"
    )
    property_class: str = Field(
        min_length=3,
        description="A three digit code, established by ORPS for categorizing property by use."
    )
    property_class_description: str = Field(
        min_length=1,
        description="Literal text description of property class assigned to parcel."
    )
    print_key_code: str = Field(
        min_length=10,
        description="Unique identifier for the parcel within a given municipality."
    )
    parcel_address_number: Optional[str] = Field(
        default=None,
        description="Street number associated with physical location of property."
    )
    parcel_address_street: str = Field(
        min_length=1,
        description="Street name associated with physical location of property."
    )
    parcel_address_suff: Optional[str] = Field(
        default=None,
        description="Street suffix associated with physical location of property."
    )
    front: int | float = Field(
        ge=0,
        description="The width of the parcel from the front in feet."
    )
    depth: int | float = Field(
        ge=0,
        description="Measurement from the front to the rear line of the parcel in feet."
    )
    full_market_value: int = Field(
        ge=0,
        description=(
            "Hypothetical price a property would bring if exposed for sale in the open market, "
            "in an arm's length transaction between a willing seller and a willing buyer, "
            "both of whom are knowledgeable concerning all the uses to which it is adapted and "
            "for which it is capable of being used.")
    )

    def generate_properties_id(self) -> str:
        """
        Generate the primary key `id` for the `properties` table in the format:
        `{swis_code} {print_key_code}`
        """
        return f"{self.swis_code} {self.print_key_code}"

    def generate_address_street(self) -> str:
        """
        Generate `address_street` for `properties` table in the format:
        `{parcel_address_number} {parcel_address_street} {parcel_address_suff}`.
        Concatenate the values, filtering out any fields with empty strings.
        Ensures the final string has no extra spaces, even if some fields are empty.

        Returns:
            str: The concatenated address string.
        """
        components = [self.parcel_address_number, self.parcel_address_street, self.parcel_address_suff]
        result = " ".join(filter(None, components))

        return result.strip()

    @staticmethod
    def generate_address_state() -> str:
        """All properties are in New York."""
        return NYPropertyAssessment.STATE

    def to_properties_row(self) -> dict:
        """
        Return data for a row in `properties` table from full record.
        Example:
         {
             "id": "311500 004.-03-34.0",
             "swis_code": "311500",
             "print_key_code": "004.-03-34.0",
             "municipality_code": "311500",
             "municipality_name": "Syracuse",
             "county_name": "Onondaga",
             "school_district_code": "311500",
             "school_district_name": "Syracuse",
             "address_street": "1325 Lemoyne Ave",
             "address_state": "NY"
         }

        """
        return {
            "id": self.generate_properties_id(),
            "swis_code": self.swis_code,
            "print_key_code": self.print_key_code,
            "municipality_code": self.municipality_code,
            "municipality_name": self.municipality_name,
            "county_name": self.county_name,
            "school_district_code": self.school_district_code,
            "school_district_name": self.school_district_name,
            "address_street": self.generate_address_street(),
            "address_state": self.generate_address_state()
        }

    def to_ny_property_assessments_row(self) -> dict:
        """
        Return data for a row in `ny_property_assessments` table from full record.
        Example:
         {
             "property_id": "311500 004.-03-34.0",
             "roll_year": 2024,
             "property_class": "210",
             "property_class_description": "One Family Year-Round Residence",
             "front": 59.97,
             "depth": 125,
             "full_market_value": 124800
         }
        """
        return {
            "property_id": self.generate_properties_id(),
            "roll_year": self.roll_year,
            "property_class": self.property_class,
            "property_class_description": self.property_class_description,
            "front": self.front,
            "depth": self.depth,
            "full_market_value": self.full_market_value
        }

    class Config:
        # Ignore all fields not defined
        extra = "ignore"
