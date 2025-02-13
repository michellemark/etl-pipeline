"""
Gather samples of results from various Attom APIs.
https://api.developer.attomdata.com/docs
"""
import json
import os

import requests


ATTOM_API_KEY = os.environ.get('ATTOM_API_KEY')
urls_sampled = [
    {
        "api_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0",
        "endpoint": "/property/address",
        "query_params": "postalcode=13044&page=1&pagesize=100",
        "full_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0/property/address?postalcode=13044&page=1&pagesize=100",
        "sample_name": "property-by-zipcode.json"
    },
    {
        "api_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0",
        "endpoint": "/assessment/snapshot",
        "query_params": "postalcode=13044&startcalendardate=2024-01-01&endcalendardate=2024-12-31",
        "full_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0/assessment/snapshot?postalcode=13044&startcalendardate=2024-01-01&endcalendardate=2024-12-31",
        "sample_name": "assessment-snapshot-by-zipcode.json"
    },
    {
        "api_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0",
        "endpoint": "/avm/snapshot",
        "query_params": "postalcode=13044",
        "full_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0/avm/snapshot?postalcode=13044",
        "sample_name": "avm-snapshot-by-zipcode.json"
    },
    {
        "api_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0",
        "endpoint": "/enumerations/detail",
        "query_params": "fieldNames=all",
        "full_url": "https://api.gateway.attomdata.com/propertyapi/v1.0.0/enumerations/detail?fieldNames=all",
        "sample_name": "enumerations-all.json"
    }
]
RESEARCH_DIRECTORY = 'research'
ATTOM_SAMPLE_DIRECTORY = 'sample_data'
current_directory = os.getcwd()

for api_to_sample in urls_sampled:
    headers = {
        'apikey': ATTOM_API_KEY,
        'accept': 'application/json'
    }
    response = requests.get(api_to_sample.get('full_url'), headers=headers)

    if response.ok:
        sample_filename = os.path.join(
            current_directory,
            RESEARCH_DIRECTORY,
            ATTOM_SAMPLE_DIRECTORY,
            f"{api_to_sample.get('sample_name')}"
        )

        with open(sample_filename, 'w+') as sample_file:
            json.dump(response.json(), sample_file, indent=4)
