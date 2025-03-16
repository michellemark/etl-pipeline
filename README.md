# ETL Pipeline

- Part of 2 part capstone project
- Author: Michelle Mark
- For: Bachelors of Science in Computer Science, SUNY Poly
- Copyright: All rights reserved, 2025

## ETL Pipeline Overview 

### Gather Data For Central New York Real Estate Analytics App

### Goal 1:
The purpose of this repository is to produce an **E**xtract **T**ransform **L**oad (ETL) 
pipeline to pull in available free data on real estate in the Central New York area.  Clean and 
normalize the data into an SQLite database and upload it into cloud storage.  

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

#### What NY counties will make up CNY for purposes of this project?

The [Central New York Regional Planning & Development Board](https://www.cnyrpdb.org/region.asp) defines Central New York as

- Cayuga
- Cortland
- Madison
- Onondaga
- Oswego

#### GitHub Actions

GitHub Actions usage is free for standard GitHub-hosted runners in public repositories, such as this.  
The pipeline is designed to be run in GitHub actions free runners, where it can be executed for 
no ongoing cost. It is set up to be manually executed to save execution time, which is limited in 
free runners, [but could easily be scheduled](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows#schedule).  With 
a better budget this code could really be run most places, in fact, and could easily become a 
fully automated pipeline.

#### AWS Data Storage

The generated SQLite database is stored in an AWS s3 bucket for a trivially small ongoing cost.
Database estimated to stay below 250Mb in size, so an estimate of ongoing costs would be:

**S3 Standard Storage Rates**: **$0.023 per GB per month**

    1 GB = 1024 MB so 250 MB is 250 ÷ 1024 or about 0.2441 GB

**Ongoing Cost:**

    0.2441 * 0.023 ≈ 0.0056 per month (about half of a cent)
    0.0056 * 12 ≈ 0.07 cents per year

#### Zipcode Update Limitations Found

The Open NY property assessment data only returns the zipcode for the mailing address of the property 
owner.  However, it is very common that the owner of the property does not live at the address of the 
parcel.  Far too often the only zipcode provided is for an address not even in the same state as the 
parcel we have assessment data for.

Given the purpose of collecting zipcodes is so we can filter by zipcode in a later user interface and view 
related statistics on CNY properties, I created a secondary workflow to go back and get the missing zipcodes 
and update the database.  It uses batch calls to the 
[US Census Bureau Geocoder API](https://geocoding.geo.census.gov/geocoder/returntype/addressbatch) to
send csv files with 10,000 addresses at a time.  **This works, except that API was only able to 
accurately match 95,699 out of 311964 records, leaving almost 70%, without a zipcode.**

There are other services that would be better for gathering zipcode information, and given a 
better budget something like Google's Geocoder API, for example, could fill in all those data gaps. For now 
the user interface I will be building will not be able to filter by zipcodes due to this issue.

Another attempt was made to get zipcodes from Open Addresses. A GeoJSON file was downloaded with all 
Open Addresses data available for NY State from [here](https://batch.openaddresses.io/job/559314#map=0/0/0)
The GeoJSON file was enormous, so I wrote a small script to pull out only data for the CNY counties 
this project focuses on and remove obvious duplicates and wrote it to a new json file. The result was 
still too large to check into the repository, so I added it manually to s3 and wrote a workflow to 
update zipcodes from this data. Unfortunately, this only yielded about 13,000 updated zipcodes in
the database, and the workflow is not truly repeatable for anyone not having access to my file in s3.

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
