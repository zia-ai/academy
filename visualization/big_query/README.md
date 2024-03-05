# Uploading humanfirst query end points results to big query

## Pre-requisite
Have a workspace with labelled and unlabelled data and NLU engine trained and inferred

## Steps
- Run coverage.py script on the workspace
- It produces 3 files JSON, JSONL and CSV
- CSV contains extracted information
- Whereas JSON and JSONL contains the raw information from HumanFirst query end point
- Create a project in GCP
- Go to Big Query
- Click the kebab menu (vertical 3 dots icon) beside the project name
- Create a dataset (select single region). Can give any region if uploading data from local file. If uploading data from Google Cloud Storage(GCS), then provide the region where the GCS exists.
- Now click the kebab menu beside the dataset name and create a table.
- Create table from - choose upload if uploading from local or else choose GCS or drive
- Select file/dataset URI
    - Can upload a file size of upto 100 mb if uploading from local
    - Should need to go for GCS or drive if file size is more than 100mb
    - Provide URI if going for GCS or drive
- File format JSONL/CSV depening on the type of file uploading
- Under destination
    - Select project and dataset
    - Provide a table name
    - Table type - Native table
- Under schema
    - Edit text
    - copy paste the schema.json
    - Modify the schema if there is any update in the metadata fields
    - Create table
- Run the query from query.sql after editing the table path


