import os

from requests import ReadTimeout
from requests import RequestException
from requests import Timeout
from requests.exceptions import ChunkedEncodingError

from urllib3.exceptions import ProtocolError

from etl.exceptions import SocrataRateLimitError

# ******* Real estate API values ********************************
CNY_COUNTY_LIST = ["Cayuga", "Cortland", "Madison", "Onondaga", "Oswego"]
OPEN_NY_BASE_URL = "data.ny.gov"
MINIMUM_ASSESSMENT_YEAR = 2024
OPEN_NY_ASSESSMENT_RATIOS_API_ID = "bsmp-6um6"
OPEN_NY_PROPERTY_ASSESSMENTS_API_ID = "7vem-aaz7"
OPEN_NY_LIMIT_PER_PAGE = 1000
OPEN_NY_CALLS_PER_PERIOD = 3
OPEN_NY_RATE_LIMIT_PERIOD = 60
ALL_PROPERTIES_STATE = "NY"
RETRYABLE_ERRORS = (
    ConnectionError,  # Base class for connection-related errors
    ConnectionResetError,  # Connection reset by peer
    TimeoutError,  # Request timed out
    Timeout,  # Requests timeout
    ConnectionError,  # Requests connection problems
    ReadTimeout,  # Reading from server timed out
    ChunkedEncodingError,  # Error with chunked transfer encoding
    RequestException,  # Base class for requests exceptions
    ProtocolError,  # Low-level protocol errors
    SocrataRateLimitError  # Rate limit exceeded
)
US_CENSUS_BUREAU_BATCH_SIZE = 10000
US_CENSUS_BUREAU_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/returntype/addressbatch"
US_CENSUS_BUREAU_CALLS_PER_PERIOD = 4
US_CENSUS_BUREAU_RATE_LIMIT_PERIOD = 60
CREATE_BATCH_JOBS_TABLE_NAME = "batch_geocoding_jobs"

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
ERROR_LOG_LEVEL = "error"
WARNING_LOG_LEVEL = "warning"
INFO_LOG_LEVEL = "info"
DEBUG_LOG_LEVEL = "debug"

# ****** Property Class Map *******************************
# Property categories to use for filtering no matter the source
# Use simple category code for ease of querying / code purposes
SINGLE_FAMILY_HOUSE = "SFH"
MULTI_FAMILY_RESIDENCE = "MFR"
APARTMENT_TOWNHOUSE = "ATC"
COMMERCIAL_PROPERTY = "CP"
LOTS_AND_LAND = "LAL"
MANUFACTURED_HOMES = "MH"
OTHER_PROPERTY_CATEGORY = "OP"
DESIRED_PROPERTY_CATEGORIES = [
    SINGLE_FAMILY_HOUSE,
    MULTI_FAMILY_RESIDENCE,
    APARTMENT_TOWNHOUSE,
    COMMERCIAL_PROPERTY,
    LOTS_AND_LAND,
    MANUFACTURED_HOMES
]
# Map UI display text to simple category code
PROPERTY_CATEGORY_DESCRIPTIONS = {
    SINGLE_FAMILY_HOUSE: "Single Family House",
    MULTI_FAMILY_RESIDENCE: "Multi Family Residence",
    APARTMENT_TOWNHOUSE: "Apartment, Townhouse, Condo, Row House",
    COMMERCIAL_PROPERTY: "Commercial Property",
    LOTS_AND_LAND: "Lots and Land",
    MANUFACTURED_HOMES: "Manufactured Homes",
    OTHER_PROPERTY_CATEGORY: "Other Property Category"
}
# Map these categories to values returned by Open NY property assessments API
OPEN_NY_PROPERTY_CLASS_MAP = [
    {
        "property_class": 100,
        "property_class_description": "Agricultural",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 105,
        "property_class_description": "Agricultural Vacant Land (Productive)",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 110,
        "property_class_description": "Livestock and Products",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 111,
        "property_class_description": "Poultry and Poultry Products: eggs,chickens, turkeys, ducks and geese",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 112,
        "property_class_description": "Dairy Products: milk, butter and cheese",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 113,
        "property_class_description": "Cattle, Calves, Hogs",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 114,
        "property_class_description": "Sheep and Wool",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 115,
        "property_class_description": "Honey and Beeswax",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 116,
        "property_class_description": "Other Livestock: donkeys, goats",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 117,
        "property_class_description": "Horse Farms",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 120,
        "property_class_description": "Field Crops",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 129,
        "property_class_description": "Acquired Development Rights",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 130,
        "property_class_description": "Truck Crops - Mucklands",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 140,
        "property_class_description": "Truck Crops - Not Mucklands",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 150,
        "property_class_description": "Orchard Crops",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 151,
        "property_class_description": "Apples, Pears, Peaches, Cherries, etc.",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 152,
        "property_class_description": "Vineyards",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 160,
        "property_class_description": "Other Fruits",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 170,
        "property_class_description": "Nursery and Greenhouse",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 180,
        "property_class_description": "Specialty Farms",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 183,
        "property_class_description": "Aquatic: oysterlands, fish and aquatic plants",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 184,
        "property_class_description": "Livestock: deer, moose, llamas, buffalo, etc.",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 190,
        "property_class_description": "Fish, Game and Wildlife Preserves",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 200,
        "property_class_description": "Residential",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 210,
        "property_class_description": "One Family Year-Round Residence",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 215,
        "property_class_description": "One Family Year-Round Residence with Accessory Apartment",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 220,
        "property_class_description": "Two Family Year-Round Residence",
        "property_category": MULTI_FAMILY_RESIDENCE
    },
    {
        "property_class": 230,
        "property_class_description": "Three Family Year-Round Residence",
        "property_category": MULTI_FAMILY_RESIDENCE
    },
    {
        "property_class": 240,
        "property_class_description": "Rural Residence with Acreage",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 241,
        "property_class_description": "Primarily residential, also used in agricultural production",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 242,
        "property_class_description": "Recreational use",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 250,
        "property_class_description": "Estate",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 260,
        "property_class_description": "Seasonal Residences",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 270,
        "property_class_description": "Mobile Home",
        "property_category": MANUFACTURED_HOMES
    },
    {
        "property_class": 271,
        "property_class_description": "Multiple Mobile Homes",
        "property_category": MANUFACTURED_HOMES
    },
    {
        "property_class": 280,
        "property_class_description": "Residential - Multi_Purpose/Multi-Structure",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 281,
        "property_class_description": "Multiple Residences",
        "property_category": MULTI_FAMILY_RESIDENCE
    },
    {
        "property_class": 283,
        "property_class_description": "Residence with Incidental Commercial Use",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 300,
        "property_class_description": "Vacant Land",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 310,
        "property_class_description": "Residential",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 311,
        "property_class_description": "Residential Vacant Land",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 312,
        "property_class_description": "Residential Land Including a Small Improvement",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 314,
        "property_class_description": "Rural Vacant Lots of 10 Acres or less",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 315,
        "property_class_description": "Underwater Vacant Land",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 320,
        "property_class_description": "Rural",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 321,
        "property_class_description": "Abandoned Agricultural Land",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 322,
        "property_class_description": "Residential Vacant Land Over 10 Acres",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 323,
        "property_class_description": "Other Rural Vacant Lands",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 330,
        "property_class_description": "Vacant Land Located in Commercial Areas",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 331,
        "property_class_description": "Commercial Vacant with minor improvements",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 340,
        "property_class_description": "Vacant Land Located in Industrial Areas",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 341,
        "property_class_description": "Industrial Vacant with minor improvements",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 351,
        "property_class_description": "Shell building - residential",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 352,
        "property_class_description": "Shell building - commercial",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 380,
        "property_class_description": "Public Utility Vacant Land",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 400,
        "property_class_description": "Commercial",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 410,
        "property_class_description": "Living Accomodations",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 411,
        "property_class_description": "Apartments",
        "property_category": APARTMENT_TOWNHOUSE
    },
    {
        "property_class": 414,
        "property_class_description": "Hotel",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 415,
        "property_class_description": "Motel",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 416,
        "property_class_description": "Mobile Home Parks",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 417,
        "property_class_description": "Camps, Cottages, Bungalows",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 418,
        "property_class_description": "Inns, Lodges, Boarding Houses, Tourist Homes, Fraternity and Sorority Homes",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 420,
        "property_class_description": "Dining Establishments",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 421,
        "property_class_description": "Restaurants",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 422,
        "property_class_description": "Diners and Luncheonettes",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 423,
        "property_class_description": "Snack Bars, Drive-Ins, Ice Cream Bars",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 424,
        "property_class_description": "Night Clubs",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 425,
        "property_class_description": "Bar",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 426,
        "property_class_description": "Fast Food Franchises",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 430,
        "property_class_description": "Motor Vehicle Services",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 431,
        "property_class_description": "Auto Dealers - Sales and Service",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 432,
        "property_class_description": "Service and Gas Stations",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 433,
        "property_class_description": "Auto Body, Tire Shops, Other Related Auto Sales",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 434,
        "property_class_description": "Automatic Car Wash",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 435,
        "property_class_description": "Manual Car Wash",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 436,
        "property_class_description": "Self-Service Car Wash",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 437,
        "property_class_description": "Parking Garage",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 438,
        "property_class_description": "Parking Lot",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 439,
        "property_class_description": "Small Parking Garage",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 440,
        "property_class_description": "Storage, Warehouse and Distribution Facilities",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 441,
        "property_class_description": "Fuel Storage and Distribution Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 442,
        "property_class_description": "Mini Warehouse (Self-Service Storage)",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 443,
        "property_class_description": "Grain and Feed Elevators, Mixers, Sales Outlets",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 444,
        "property_class_description": "Lumber Yards, Sawmills",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 446,
        "property_class_description": "Cold Storage Facilities",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 447,
        "property_class_description": "Trucking Terminals",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 448,
        "property_class_description": "Piers, Wharves, Docks and Related Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 449,
        "property_class_description": "Other Storage, Warehouse and Distribution Facilities",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 450,
        "property_class_description": "Retail Services",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 451,
        "property_class_description": "Regional Shopping Centers",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 452,
        "property_class_description": "Area of Neighborhood Shopping Centers",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 453,
        "property_class_description": "Large Retail Outlets",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 454,
        "property_class_description": "Large Retail Food Stores",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 455,
        "property_class_description": "Dealerships - Sales and Service (other than auto)",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 456,
        "property_class_description": "Medium Retail",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 457,
        "property_class_description": "Small Retail",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 460,
        "property_class_description": "Banks and Office Buildings",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 461,
        "property_class_description": "Standard Bank/Single Occupant",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 462,
        "property_class_description": "Drive-In Branch Bank",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 463,
        "property_class_description": "Bank Complex with Office Building",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 464,
        "property_class_description": "Office Building",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 465,
        "property_class_description": "Professional Building",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 470,
        "property_class_description": "Miscellaneous Services",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 471,
        "property_class_description": "Funeral Homes",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 472,
        "property_class_description": "Dog Kennels, Veterinary Clinics",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 473,
        "property_class_description": "Greenhouses",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 474,
        "property_class_description": "Billboards",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 475,
        "property_class_description": "Junkyards",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 480,
        "property_class_description": "Multiple Use or Multipurpose",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 481,
        "property_class_description": "Downtown Row Type (with common wall)",
        "property_category": APARTMENT_TOWNHOUSE
    },
    {
        "property_class": 482,
        "property_class_description": "Downtown Row Type (detached)",
        "property_category": SINGLE_FAMILY_HOUSE
    },
    {
        "property_class": 483,
        "property_class_description": "Converted Residence",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 484,
        "property_class_description": "One Story Small Structure",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 485,
        "property_class_description": "One Story Small Structure - Multi occupant",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 486,
        "property_class_description": "Minimart",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 500,
        "property_class_description": "Recreation and Entertainment",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 510,
        "property_class_description": "Entertainment Assembly",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 511,
        "property_class_description": "Legitimate Theaters",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 512,
        "property_class_description": "Motion Picture Theaters (excludes drive-in theaters)",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 513,
        "property_class_description": "Drive-In Theaters",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 514,
        "property_class_description": "Auditoriums, Exhibition and Exposition Halls",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 515,
        "property_class_description": "Radio, T.V. and Motion Picture Studios",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 521,
        "property_class_description": "Stadiums, Arenas, Armories, Field Houses",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 522,
        "property_class_description": "Racetracks",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 530,
        "property_class_description": "Amusement Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 531,
        "property_class_description": "Fairgrounds",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 532,
        "property_class_description": "Amusement Parks",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 533,
        "property_class_description": "Game Farms",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 534,
        "property_class_description": "Social Organizations",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 541,
        "property_class_description": "Bowling Centers",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 542,
        "property_class_description": "Ice or Roller Skating Rinks",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 544,
        "property_class_description": "Health Spas",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 545,
        "property_class_description": "Indoor Swimming Pools",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 546,
        "property_class_description": "Other Indoor Sports",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 551,
        "property_class_description": "Skiing Centers",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 552,
        "property_class_description": "Public Golf Courses",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 553,
        "property_class_description": "Private Golf Country Clubs",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 554,
        "property_class_description": "Outdoor Swimming Pools",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 555,
        "property_class_description": "Riding Stables",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 557,
        "property_class_description": "Other Outdoor Sports",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 560,
        "property_class_description": "Improved Beaches",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 570,
        "property_class_description": "Marinas",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 580,
        "property_class_description": "Camps, Camping Facilities and Resorts",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 581,
        "property_class_description": "Camps",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 582,
        "property_class_description": "Camping Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 583,
        "property_class_description": "Resort Complexes",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 590,
        "property_class_description": "Parks",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 591,
        "property_class_description": "Playgrounds",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 592,
        "property_class_description": "Athletic Fields",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 593,
        "property_class_description": "Picnic Grounds",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 610,
        "property_class_description": "Education",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 612,
        "property_class_description": "Schools",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 613,
        "property_class_description": "Colleges and Universities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 614,
        "property_class_description": "Special Schools and Institutions",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 615,
        "property_class_description": "Other Educational Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 620,
        "property_class_description": "Religious",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 632,
        "property_class_description": "Benevolent and Moral Associations",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 633,
        "property_class_description": "Homes for the Aged",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 640,
        "property_class_description": "Health",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 641,
        "property_class_description": "Hospitals",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 642,
        "property_class_description": "All Other Health Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 651,
        "property_class_description": "Highway Garage",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 652,
        "property_class_description": "Office Building",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 661,
        "property_class_description": "Army, Navy, Air Force, Marine and Coast Guard Installations, Radar, etc.",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 662,
        "property_class_description": "Police and Fire Protection, Electrical Signal Equipment and Other Facilities for Fire, Police, Civil Defense, etc.",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 670,
        "property_class_description": "Correctional",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 680,
        "property_class_description": "Cultural and Recreational",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 681,
        "property_class_description": "Cultural Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 682,
        "property_class_description": "Recreational Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 690,
        "property_class_description": "Miscellaneous",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 691,
        "property_class_description": "Professional Associations",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 692,
        "property_class_description": "Roads, Streets, Highways and Parkways, Express or Otherwise Including Adjoining Land",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 694,
        "property_class_description": "Animal Welfare Shelters",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 695,
        "property_class_description": "Cemeteries",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 700,
        "property_class_description": "Industrial",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 710,
        "property_class_description": "Manufacturing and Processing",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 712,
        "property_class_description": "High Tech. Manufacturing and Processing",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 714,
        "property_class_description": "Light Industrial Manufacturing and Processing",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 720,
        "property_class_description": "Mining and Quarrying",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 733,
        "property_class_description": "Gas (for production)",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 741,
        "property_class_description": "Gas",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 743,
        "property_class_description": "Brine",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 744,
        "property_class_description": "Petroleum Products",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 749,
        "property_class_description": "Other",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 821,
        "property_class_description": "Flood Control",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 822,
        "property_class_description": "Water Supply",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 823,
        "property_class_description": "Water Treatment Facilities",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 830,
        "property_class_description": "Communication",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 831,
        "property_class_description": "Telephone Facility",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 832,
        "property_class_description": "Telegraph",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 833,
        "property_class_description": "Radio",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 834,
        "property_class_description": "Television other than Community Antenna Television",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 835,
        "property_class_description": "Community Antenna Television (CATV) Facility",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 836,
        "property_class_description": "Telephone Outside Plant",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 837,
        "property_class_description": "Cellular Telephone Towers",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 840,
        "property_class_description": "Transportation",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 841,
        "property_class_description": "Motor Vehicle",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 842,
        "property_class_description": "Ceiling Railroad",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 843,
        "property_class_description": "Nonceiling Railroad",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 844,
        "property_class_description": "Air",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 850,
        "property_class_description": "Waste Disposal",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 852,
        "property_class_description": "Landfills and Dumps",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 853,
        "property_class_description": "Sewage Treatment and Water Pollution Control",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 872,
        "property_class_description": "Electric Substation",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 873,
        "property_class_description": "Gas Measuring and Regulating Station",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 874,
        "property_class_description": "Electric Power Generation Facility - Hydro",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 875,
        "property_class_description": "Electric Power Generation Facility - Fossil Fuel",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 877,
        "property_class_description": "Electric Power Generation Facility - Other Fuel",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 878,
        "property_class_description": "Electric Power Generation Facility - Solar",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 882,
        "property_class_description": "Electric Transmission",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 883,
        "property_class_description": "Gas Transmission",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 885,
        "property_class_description": "Gas Distribution (Outside Plant Property)",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 910,
        "property_class_description": "Private Wild and Forest Lands except for Private Hunting and Fishing Clubs",
        "property_category": LOTS_AND_LAND
    },
    {
        "property_class": 911,
        "property_class_description": "Forest Land under Section 480 of the Real Property Tax Law",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 912,
        "property_class_description": "Forest Land under Section 480-a of the Real Property Tax Law",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 920,
        "property_class_description": "Private Hunting and Fishing Clubs",
        "property_category": COMMERCIAL_PROPERTY
    },
    {
        "property_class": 930,
        "property_class_description": "State Owned Forest Lands",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 932,
        "property_class_description": "State Owned Land Other Than Forest Preserve Covered under Section 532-b,c,d,e,f,g  of the Real Property Tax Law",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 942,
        "property_class_description": "County Owned Reforested Land",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 970,
        "property_class_description": "Other Wild or Conservation Lands",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 971,
        "property_class_description": "Wetlands, Either Privately of Governmentally Owned, Subject to Specific Restrictions as to Use",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 972,
        "property_class_description": "Land Under Water, Either Privately of Governmentally Owned (other than residential - more property classified as code 315)",
        "property_category": OTHER_PROPERTY_CATEGORY
    },
    {
        "property_class": 980,
        "property_class_description": "Taxable State Owned Conservation Easements",
        "property_category": OTHER_PROPERTY_CATEGORY
    }
]
