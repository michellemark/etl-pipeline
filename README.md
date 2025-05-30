# ETL Pipeline

- Part of 2 part capstone project
- Author: Michelle Mark
- For: Bachelors of Science in Computer Science, SUNY Poly
- Copyright: All rights reserved, 2025

## ETL Pipeline Overview 

### Gather Data For Central New York Real Estate Analytics App

### Goal 1:

This repository is designed to implement an **ETL Pipeline** that:"_

- **Extracts** free real estate data from public sources specific to the Central New York area.
- **Transforms** the raw data into a normalized SQLite database format.
- **Loads** the processed data into cloud storage for later use with a user-friendly dashboard for analysis and visualization.

[Another repository](https://github.com/michellemark/central-ny-realestate-trends-dashboard) will 
focus on downloading the data from cloud storage and presenting 
analytics in a user-friendly interface that provides graphs for visualization and filtering.

Options to filter data in UI will include at least:
1. Property type, (ie: single family vs multifamily, etc.)
2. School district
3. ~~Zip code~~ (see [Zipcode Update Limitations Found](#zipcode-update-limitations-found))

So data for all these filters needs to be gathered for every property here.

### Goal 2:

After each ETL pipeline run new data is available to the user interface.

### Goal 3:

Optionally, make the app available publicly with no ongoing cost.

### Implementation Choices Made

#### What NY counties will make up CNY for purposes of this project?

The [Central New York Regional Planning & Development Board](https://www.cnyrpdb.org/region.asp) defines Central New York as

- Cayuga
- Cortland
- Madison
- Onondaga
- Oswego

#### Open NY APIs

New York State makes a wide variety of data available to the public via APIs, including 
the property assessment data we will use for the main bulk of the data imported by this
project, as it is free.

https://dev.socrata.com/consumers/getting-started.html

The ETL pipeline will be collecting data for CNY Counties from:
- [Property Assessment Data from Local Assessment Rolls](https://dev.socrata.com/foundry/data.ny.gov/7vem-aaz7)
- [Residential Assessment Ratios](https://dev.socrata.com/foundry/data.ny.gov/bsmp-6um6)

To get an app token, so you can run this yourself, simply:
- [Sign up for a free Socrata account](https://support.socrata.com/hc/en-us/articles/115004055807-How-to-Sign-Up-for-a-Tyler-Data-Insights-ID)
- [Create a free app token](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys)

### Zillow Home Value Index (ZHVI) for Single-Family Homes

#### Update: Zillow Data Collection Disabled

Initially, this pipeline included functionality to scrape data for the **Zillow Home Value Index (ZHVI)** for single-family
homes (SFH) to provide insights into housing market trends in Central New York. However, due to changes in the structure of
Zillow’s website and the absence of a public API, scraping Zillow data is no longer feasible.
This portion of the ETL pipeline has been **disabled** in the final implementation, but the related code and unit tests remain in
the repository to document the work that was completed toward this feature.
As a result:

- **Zillow research data page scraping code** remains in the codebase but is no longer executed.
- The database table used to store Zillow data is not loaded or updated in the pipeline. `zillow_home_value_index_sfh`
- Zillow-related workflows are omitted from the final functioning ETL process

#### GitHub Actions

GitHub Actions usage is free for standard GitHub-hosted runners in public repositories, such as this.  
The pipeline is designed to be run in GitHub actions free runners, where it can be executed for 
no ongoing cost. It is set up to be manually executed to save execution time, which is limited in 
free runners, [but could easily be scheduled](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows#schedule).  With 
a better budget this code could really be run most places, in fact, and could easily become a 
fully automated pipeline.

### Database Design / Decisions

![CNY Real Estate Database Diagram](cny-real-estate-db-diagram.png)

The data collected is saved into an SQLite database designed to hold data from multiple assessment 
years with a minimum of redundancy, only keeping fields from the data intended for use in the 
end dashboard.  The data from property assessments that will remain the same every year is held in the 
main table named properties.  Anything about the property that might have changed from one 
assessment year to the next is held in the ny_property_assessments table.  This includes things like 
full market value, but also the property class and property class description assigned by NY State, 
as, for example, a previously empty lot might one year become a single family home.  

Speaking of property class values, NY State has hundreds of them, and I wished to only provide a 
set number of property categories to have users filter on including:

* Single Family House
* Multi Family Residence
* Apartment, Townhouse, Condo, Row House
* Commercial Property
* Lots and Land
* Manufactured Homes

I solved this in a few steps.  First, I created a mapping between all the property classes returned 
from the API endpoint and the category I wished to be assigned to them, for purposes of filtering in 
the dashboard. The data returned from the property assessments endpoint potentially also includes 
things like large government-owned properties, ports and more, that I did not want included in the data. 
These properties, many worth many millions of dollars, would skew the analytics while not really 
providing insights relevant to most real estate investment decisions.

To filter out the parcels with property classes I did not wish to import at all, 
I mapped those to OTHER_PROPERTY_CATEGORY.  I created a function to build a WHERE clause for 
the API calls to request only desired property classes.  Then in the pydantic validation 
model I added functionality to return for each record the mapped property category I wanted 
assigned to it for purposes of filtering in the dashboard, and that is saved in each 
ny_property_assessments record.

Each municipality has their own rules for how they assess properties, and part of the equation is the 
municipality residential assessment ratio.  There are fairly complicated rules around how market 
value is determined, but knowing the municipality's residential assessment ratio seemed to me like a 
frequently asked question the data should be able to answer.  [See more on this here.](https://www.tax.ny.gov/research/property/equal/rar/index.htm)
So the ETL pipeline also collects this data and stores it in the municipality assessment ratios 
table, related to the properties table on the municipality_code unique key.  In this way every 
record in the dashboard is able to provide this value, alongside the other data, with a simple join.

#### AWS Data Storage

The SQLite database generated by this ETL pipeline is securely stored in an AWS S3 bucket, ensuring scalability and reliability
while maintaining a minimal ongoing cost. Below is a breakdown of the estimated costs and optimizations made to ensure
cost-effectiveness:

**Storage Costs:**
The database is estimated to remain under 250 MB, resulting in the following ongoing cost estimates based on the **S3 Standard
Storage Rate** of : **$0.023 per GB per month**

- **Conversion to GB:**
    - 250 MB ÷ 1024 ≈ 0.244 GB

- **Monthly Storage Cost:**
    - 0.244 GB × 0.023 ≈ **0.0056/month** (~half a cent per month)

- **Yearly Storage Cost:**
    - 0.0056 × 12 ≈ **0.07/year**

**Data Transfer Costs:**
While data transfers **into S3** are free, outbound data transfers are charged at 0.09 per GB. During initial testing of the ETL
pipeline and Streamlit dashboard, data transfer resulted in a surprise $9 bill one month. To address this, several
optimizations were made:

1. **Database Compression:**
   The SQLite database is now gzipped, reducing its size from ~250 MB to ~38 MB.
2. **Version Control for Downloads:**
   A simple version control mechanism was added. The Streamlit dashboard checks for an updated version to be available before 
   downloading the database, ensuring that unnecessary downloads do not occur. These changes substantially reduce outbound 
   data transfer costs.

**Updated Cost Expectations:**
With these optimizations, data transfer costs are significantly reduced. Assuming the gzipped database is downloaded only 
occasionally, estimated costs can be easily managed. By using AWS S3 for storage and implementing cost-saving measures, this 
project ensures a high level of performance and accessibility without incurring significant expenses.

For more information about S3 pricing, see the [S3 Pricing Guide](https://aws.amazon.com/s3/pricing/).

#### Zipcode Update Limitations Found

_Challenge:_

- Open NY property assessment data only includes the property owner's mailing address zip code, which does not
  always reflect the assessed property's location.
- **Owner occupancy**: the owner's mailing address is compared to the parcel address to decide if the property is 
  owner-occupied. If owner-occupied, the mailing zip code is used; otherwise, the zip code is omitted.
- Many zip codes for non-owner-occupied properties remain missing, impacting the ability to filter properties by zip code in the
  user interface.

_Partial Solutions:_

- **Owner Occupancy Logic Adjustment**: Through validation, zip codes are only included in the database if the property is
  determined to be owner-occupied (based on matching parcel and mailing addresses). For non-owner-occupied properties, zip code
  data is purposefully excluded to avoid introducing inaccuracies.
- **US Census Bureau Geocoder API** provided partial coverage (~30%). However, due to API limitations, many zip codes could not be
  retrieved. [documentation](https://geocoding.geo.census.gov/geocoder/returntype/addressbatch)
- **Open Addresses GeoJSON Data** yielded a small subset of updated zip codes. While functional, it is not a repeatable or
  complete solution. [documentation](https://batch.openaddresses.io/job/559314#map=0/0/0)

_Conclusion:_  
A greater budget allowance could enable the integration of more accurate geocoding APIs, such as Google Geocoder, for
comprehensive zip code data. Additionally, further refining the owner-occupancy logic and leveraging larger datasets could help
improve zip code coverage in future implementations.

## Development

This repository uses Python 3.12, managed by [pyenv](https://github.com/pyenv/pyenv) and dependencies managed
with [Poetry](https://python-poetry.org/docs/).

Before you start, make sure you have:

- `pyenv` installed and Python version 3.12.8 set as the current global or local Python version.
- `poetry` installed for dependency management.

To install dependencies using Poetry (including dev dependencies):

```shell
poetry install --with dev
```

### Activating Virtual Environment

To activate the virtual environment managed by Poetry, run the following command (recommended):

```shell
poetry env activate
```

Alternatively, you can run individual commands without activating the shell explicitly:

```shell
poetry run <command>
```

### Running Unit Tests

Unit testing is configured with `pytest` and `tox`. Simply activate the environment and run tests using:
```shell
poetry env activate
tox
```

Alternatively, directly via poetry (without activating the environment explicitly)
```shell
poetry run tox
```

### Local Development

#### Set required environment variables:

- `AWS_ACCESS_KEY_ID`: AWS user credentials with permission on the targeted S3 bucket
- `AWS_SECRET_ACCESS_KEY`: associated AWS secret key
- `AWS_REGION`: AWS region where your S3 bucket resides.
- `OPEN_DATA_APP_TOKEN`: API key/token for accessing Open NY APIs

```shell
export AWS_ACCESS_KEY_ID="<your-access-key>"
export AWS_SECRET_ACCESS_KEY="<your-secret-access-key>"
export AWS_REGION="<your-region>"
export OPEN_DATA_APP_TOKEN="<your-open-data-token>"
```

**Note:**
If not interested in uploading to AWS you may safely use dummy values, but Open NY API token still needs a
real value
```shell
export AWS_ACCESS_KEY_ID="dummy"
export AWS_SECRET_ACCESS_KEY="dummy"
export AWS_REGION="dummy"
export OPEN_DATA_APP_TOKEN="<your-open-data-token>"
```

### Running Workflows Locally

Run ETL workflow

```shell
python -m etl.etl_pipeline
```

Then, only after ETL completes successfully, run update zip codes workflow separately:

```shell
python -m etl.update_property_zipcodes
```

# Deployment

This app is intended to either be run locally or run using GitHub actions, and therefore 
requires no deployment.  To run in GitHub actions requires being logged in as the 
repository owner, and manually visting the repository, going to the Actions tab,
clicking the workflow you wish to run and clicking Run Workflow.

The main ETL Workflow offers a checkbox to flag the system to `FORCE_REFRESH`.  If 
selected all data will be replaced in the database, causing all API calls to 
happen again.  Without the FORCE_REFRESH option if the database already exists in 
s3 it will skip pulling property assessment data for counties that already have 
records in the properties table for the roll year.
