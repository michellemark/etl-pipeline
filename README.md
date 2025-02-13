# Capstone Project
- Author: Michelle Mark
- For: Bachelors of Science in Computer Science, SUNY Poly
- Copyright: All rights reserved, 2025

## Central New York Real Estate Trends and Analytics App

### Goal 1:
Build an automated ETL pipeline to pull in available free data on
real estate in the Central New York area.  Clean and normalize the
data into an SQLite database.  Train a machine learning
model to draw further trends and statistics from the data,
and add values to the database.  Present analytics in a user-friendly 
interface that provides both graphs for visualization and filtering.  

Options to filter data should include at least:
1. Property type, (ie: single family vs multifamily, etc.)
2. Zip code
3. School district

### Goal 2:

After each automated ETL / ML pipeline run data is updated in user interface.

### Goal 3:
Saving a change to main branch of repository will deploy to production environment.

### Goal 4:
Optionally, make the app available publicly with no ongoing cost.


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

### Prefect

This project uses Prefect for workflow orchestraion, and is planned to be run on Prefect cloud.
https://www.prefect.io/

https://www.prefect.io/cloud

### Local development:

Start prefect server running locally with command:
```
prefect server start
```
