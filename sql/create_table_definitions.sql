-- ------- Open NY Data Tables -------
-- Table: municipality_assessment_ratios
CREATE TABLE municipality_assessment_ratios
(
    municipality_code            TEXT    NOT NULL,
    rate_year                    INTEGER NOT NULL,
    municipality_name            TEXT    NOT NULL,
    county_name                  TEXT    NOT NULL,
    residential_assessment_ratio REAL    NOT NULL,
    PRIMARY KEY (municipality_code, rate_year) -- Compound primary key
);

-- Add an index to optimize JOIN operations between properties and municipality_assessment_ratios
CREATE INDEX idx_municipality_assessment_ratios_code
    ON municipality_assessment_ratios (municipality_code);

-- Add an index to optimize queries that filter or sort by rate_year
CREATE INDEX idx_municipality_assessment_ratios_year
    ON municipality_assessment_ratios (rate_year);

-- Add an index to optimize queries that filter or sort by county_name
CREATE INDEX idx_municipality_assessment_ratios_county
    ON municipality_assessment_ratios (county_name);

-- Optimize direct lookup for county_name and rate_year
CREATE INDEX idx_municipality_ratios_county_year
    ON municipality_assessment_ratios (county_name, rate_year);

-- Optimize direct lookup for municipality_code and rate_year
CREATE INDEX idx_municipality_ratios_code_year
    ON municipality_assessment_ratios (municipality_code, rate_year);


-- Table: properties
CREATE TABLE properties
(
    id                   TEXT PRIMARY KEY NOT NULL, -- swis_code<space>print_key_code
    swis_code            TEXT             NOT NULL,
    print_key_code       TEXT             NOT NULL,
    municipality_code    TEXT             NOT NULL, -- Foreign key to municipalities.municipality_code
    municipality_name    TEXT             NOT NULL,
    county_name          TEXT             NOT NULL,
    school_district_code TEXT             NOT NULL,
    school_district_name TEXT             NOT NULL,
    address_street       TEXT             NOT NULL, -- NY concatenated parcel street address ATTOM address.line1
    address_state        TEXT             NOT NULL, -- always NY
    address_zip          TEXT             NULL, -- From other sources where possible
    latitude             REAL             NULL, -- Latitude, from other sources where possible
    longitude            REAL             NULL, -- Longitude, from other sources where possible
    FOREIGN KEY (municipality_code) REFERENCES municipality_assessment_ratios (municipality_code)
);

-- Add an index to optimize filtering by zip code
CREATE INDEX idx_properties_zip
    ON properties (address_zip);

-- Add an index to optimize filtering by school district code
CREATE INDEX idx_properties_school_district_code
    ON properties (school_district_code);

-- Add an index to optimize filtering by school district name
CREATE INDEX idx_properties_school_district_name
    ON properties (school_district_name);

-- Add an index to optimize filtering by municipality code
CREATE INDEX idx_properties_municipality_code
    ON properties (municipality_code);

-- Optimize combined filtering by address_zip and joining with municipalities
CREATE INDEX idx_properties_zip_municipality
    ON properties (address_zip, municipality_code);

-- Optimize filtering by school_district_code and joining with municipalities
CREATE INDEX idx_properties_school_district_code_municipality
    ON properties (school_district_code, municipality_code);

-- Optimize filtering by latitude and longitude
CREATE INDEX idx_properties_latitude_longitude
    ON properties (latitude, longitude);


-- Table: ny_property_assessments
CREATE TABLE ny_property_assessments
(
    property_id                TEXT    NOT NULL, -- matches id in properties
    roll_year                  INTEGER NOT NULL,
    property_class             INTEGER NOT NULL,
    property_class_description TEXT    NOT NULL,
    property_category          TEXT    NOT NULL, -- Look up from constants file mapping
    front                      REAL    NOT NULL,
    depth                      REAL    NOT NULL,
    full_market_value          INTEGER NOT NULL,
    assessment_land            INTEGER NULL,
    assessment_total           INTEGER NULL,
    PRIMARY KEY (property_id, roll_year), -- Compound primary key
    FOREIGN KEY (property_id) REFERENCES properties (id)
);

-- Add an index to optimize filtering by property class
CREATE INDEX idx_ny_property_assessments_property_class
    ON ny_property_assessments (property_class);

-- Add an index to optimize filtering by property description
CREATE INDEX idx_ny_property_assessments_property_description
    ON ny_property_assessments (property_class_description);

-- Add an index to optimize filtering by property category
CREATE INDEX idx_ny_property_assessments_property_category
    ON ny_property_assessments (property_category);

-- Add an index to optimize filtering by roll_year
CREATE INDEX idx_ny_property_assessments_roll_year
    ON ny_property_assessments (roll_year);

-- Add an index to optimize filtering by full_market_value
CREATE INDEX idx_ny_property_assessments_full_market_value
    ON ny_property_assessments (full_market_value);

-- Optimize filtering by property_class and roll_year
CREATE INDEX idx_ny_property_assessments_class_year
    ON ny_property_assessments (property_class, roll_year);

-- Optimize filtering by property_class and joining with properties
CREATE INDEX idx_ny_property_assessments_class_property
    ON ny_property_assessments (property_class, property_id);

-- Optimize join with properties on property_id
CREATE INDEX idx_ny_property_assessments_class_year_property
    ON ny_property_assessments (property_class, roll_year, property_id);


-- Zillow Home Value Index (Single-Family Homes, County-Level)
CREATE TABLE zillow_home_value_index_sfh
(
    region_id        INTEGER NOT NULL,
    county_name      TEXT    NOT NULL,
    state_name       TEXT    NOT NULL,
    state_code       TEXT    NOT NULL,
    metro            TEXT,
    size_rank        INTEGER,
    date             TEXT    NOT NULL, -- stored as YYYY-MM
    home_value_index REAL,
    PRIMARY KEY (region_id, date)
);

-- Optimize search by county name and date
CREATE INDEX idx_zillow_hvi_sfh_county_date
    ON zillow_home_value_index_sfh (county_name, date);


-- Zillow Home Value Index (All Homes, County-Level)
CREATE TABLE zillow_home_value_index_all_homes
(
    region_id        INTEGER NOT NULL,
    county_name      TEXT    NOT NULL,
    state_name       TEXT    NOT NULL,
    state_code       TEXT    NOT NULL,
    metro            TEXT,
    size_rank        INTEGER,
    date             TEXT    NOT NULL, -- stored as YYYY-MM
    home_value_index REAL,
    PRIMARY KEY (region_id, date)
);

-- Optimize search by county name and date
CREATE INDEX idx_zillow_hvi_allhomes_county_date
    ON zillow_home_value_index_all_homes (county_name, date);


-- Median Sale Price (County-Level)
CREATE TABLE zillow_median_sale_price
(
    region_id         INTEGER NOT NULL,
    county_name       TEXT    NOT NULL,
    state_name        TEXT    NOT NULL,
    state_code        TEXT    NOT NULL,
    metro             TEXT,
    size_rank         INTEGER,
    date              TEXT    NOT NULL, -- stored as YYYY-MM
    median_sale_price REAL,
    PRIMARY KEY (region_id, date)
);

-- Optimize search by county name and date
CREATE INDEX idx_zillow_median_sale_price_county_date
    ON zillow_median_sale_price (county_name, date);


-- Zillow Observed Rent Index (ZORI, County-Level)
CREATE TABLE zillow_observed_rent_index
(
    region_id   INTEGER NOT NULL,
    county_name TEXT    NOT NULL,
    state_name  TEXT    NOT NULL,
    state_code  TEXT    NOT NULL,
    metro       TEXT,
    size_rank   INTEGER,
    date        TEXT    NOT NULL, -- stored as YYYY-MM
    rent_index  REAL,
    PRIMARY KEY (region_id, date)
);

-- Optimize search by county name and date
CREATE INDEX idx_zillow_zori_county_date
    ON zillow_observed_rent_index (county_name, date);


-- Zillow Home Value Forecast (1-Year, County-Level)
CREATE TABLE zillow_home_value_forecast
(
    region_id         INTEGER NOT NULL,
    county_name       TEXT    NOT NULL,
    state_name        TEXT    NOT NULL,
    state_code        TEXT    NOT NULL,
    metro             TEXT,
    size_rank         INTEGER,
    date              TEXT    NOT NULL, -- stored as YYYY-MM
    forecasted_growth REAL,             -- expected annual percentage growth
    PRIMARY KEY (region_id, date)
);

-- Optimize search by county name and date
CREATE INDEX idx_zillow_hvf_county_date
    ON zillow_home_value_forecast (county_name, date);
