# Capstone Project
- Author: Michelle Mark
- For: Bachelors of Science in Computer Science, SUNY Poly
- Copyright: All rights reserved, 2025

## Central New York Real Estate Trends and Analytics App

### Goal 1:
The purpose of this repository is to produce an **E**xtract **T**ransform **L**oad (ETL) 
pipeline to pull in available free data on real estate in the Central New York area.  Clean and 
normalize the data into an SQLite database and upload it into cloud storage.  

Another repository will focus on downloading the data from cloud storage and presenting 
analytics in a user-friendly interface that provides graphs for visualization and filtering.

Options to filter data in UI will include at least:
1. Property type, (ie: single family vs multifamily, etc.)
2. Zip code
3. School district

So data for all these filters needs to be gathered for every property here.

### Goal 2:

After each ETL pipeline run new data is available to the user interface.

### Goal 3:

Optionally, make the app available publicly with no ongoing cost.

## Implementation Choices Made

## Open NY APIs

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



### What NY counties will make up CNY for purposes of this project?

The [Central New York Regional Planning & Development Board](https://www.cnyrpdb.org/region.asp) defines Central New York as

- Cayuga
- Cortland
- Madison
- Onondaga
- Oswego

### GitHub Actions

GitHub Actions usage is free for standard GitHub-hosted runners in public repositories, such as this.  
The pipeline is designed to be run in GitHub actions free runners, where it can be executed for 
no ongoing cost. It is set up to be manually executed to save execution time, which is limited in 
free runners, [but could easily be scheduled](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows#schedule).  With 
a better budget this code could really be run most places, in fact, and could easily become a 
fully automated pipeline.


### AWS Data Storage

The generated SQLite database is stored in an AWS s3 bucket for a trivially small ongoing cost.
Database estimated to stay below 250Mb in size, so an estimate of ongoing costs would be:

**S3 Standard Storage Rates**: **$0.023 per GB per month**

    1 GB = 1024 MB so 250 MB is 250 ÷ 1024 or about 0.2441 GB

**Ongoing Cost:**

    0.2441 * 0.023 ≈ 0.0056 per month (about half of a cent)
    0.0056 * 12 ≈ 0.07 cents per year

## Development
This repository has been developed using Python 3.12.  
To install all required modules in your Python 3.12 virtual environment run command:
```
pip install -r requirements.txt
```

### Unit Tests

Unit testing has been set up using pytest and tox and should be run with the command
```
tox
```

### Local development:

To run the workflow locally:

- Activate your python 3.12 venv with `requirements.txt` installed
- Set needed environment variables:
``` python
AWS_ACCESS_KEY_ID # For an AWS user with permissions on your desired s3 bucket to use
AWS_SECRET_ACCESS_KEY # For an AWS user with permissions on your desired s3 bucket to use
AWS_REGION # Region the s3 bucket you wish to use is located in
OPEN_DATA_APP_TOKEN # Your free app token for Open NY API access
```
Note: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION must be set, but if you 
do not care about backing up the database to AWS and just want to run this locally you can
set these with dummy values and see a local database generated.

Then run the ETL workflow with:
```
python -m etl.etl_pipeline
```
