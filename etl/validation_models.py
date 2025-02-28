from decimal import Decimal

from pydantic import BaseModel, Field

from etl.constants import ASSESSMENT_YEAR_SOUGHT


class MunicipalityAssessmentRatio(BaseModel):
    rate_year: int = Field(
        ge=ASSESSMENT_YEAR_SOUGHT,
        description="Rate Year of assessment ratio"
    )
    swis_code: str = Field(
        min_length=6,
        serialization_alias="municipality_code"
    )
    type: str = Field(
        min_length=4,
        description="Type of Municipality ie: City, Town, Village, County",
        exclude=True
    )
    county_name: str = Field(
        min_length=6,
        description="Name of County Municipality is in"
    )
    municipality_name: str = Field(
        min_length=2,
        description="Name of Municipality"
    )
    village_name: str | None = Field(
        None,
        description="Name of Village, usually not present in data",
        exclude = True
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
