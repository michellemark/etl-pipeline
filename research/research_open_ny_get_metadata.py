"""
Generate metadata on all the NY APIs I am considering using and save each into a CSV file.
https://data.ny.gov/
"""
import csv
import os

from sodapy import Socrata

OPEN_DATA_APP_TOKEN = os.environ.get('OPEN_DATA_APP_TOKEN')
RESEARCH_DIRECTORY = 'research'
METADATA_DIRECTORY = 'open_ny_metadata'
current_directory = os.getcwd()

dataset_dict = {
    '7vem-aaz7': 'property_assessment',
    'y6cw-5z7p': 'cities_towns_villages_per_county',
    'iq85-sdzs': 'tax_rate_per_municipality',
    'bsmp-6um6': 'assessment_ratios',
    'bybh-67nt': 'onondaga_county_facilities_and_services',
    'nq2i-9jge': 'manufactured_home_park_registrations',
    'tnwc-mx3q': 'parcel_counts_by_type'
}

with Socrata('data.ny.gov', app_token=OPEN_DATA_APP_TOKEN, timeout=60) as client:

    for dataset_id, meta_name in dataset_dict.items():
        meta = client.get_metadata(dataset_id)
        meta_filename = os.path.join(
            current_directory,
            RESEARCH_DIRECTORY,
            METADATA_DIRECTORY,
            f"{meta_name}.csv"
        )

        with open(meta_filename, 'w+', newline='') as csv_file:
            meta_writer = csv.writer(csv_file, delimiter=',', quotechar='"')
            meta_writer.writerow(['Dataset Name', meta.get('name')])
            meta_writer.writerow(['Dataset Source', meta.get('attribution')])
            meta_writer.writerow(['Description', meta.get('description')])
            meta_writer.writerow(['All Columns'])
            meta_writer.writerow([
                'Column ID',
                'Column Field Name',
                'Column Name',
                'Column Type',
                'Column Description'
            ])

            for column in meta.get('columns'):
                meta_writer.writerow([
                    column.get('id'),
                    column.get('fieldName'),
                    column.get('name'),
                    column.get('dataTypeName'),
                    column.get('description')
                ])
