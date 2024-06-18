"""
python google_drive_list.py

Repeats google_sheets_read.py but for google drive.
Lists a directory or recursively lists and runs a function on a file

Outputs a data frame with the tab from the sheet you want.

Based on https://developers.google.com/drive/api/quickstart/python

Uses file search: https://developers.google.com/drive/api/guides/search-files

"""
# ******************************************************************************************************************120

# standard imports
import re
import os.path

# 3rd party imports
import click
import pandas
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# custom imports
import google_sheets_read
import google_drive_list

DRIVE_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/drive.metadata.readonly"
SHEETS_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
SHEETS_READWRITE_SCOPE = "https://www.googleapis.com/auth/spreadsheets"

# Where credentials come from
GOOGLE_PROJECT_CREDENTIALS_FILE = ".google-credentials.json"
TOKEN_FILE = ".token.json"

# Port that will start interactive authentication session.
AUTH_SERVER_PORT = 33589

# MimeTypes
GOOGLE_SHEET_FILE_MINE = "application/vnd.google-apps.spreadsheet"

DAYS = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

@click.command()
@click.option('-d', '--drive_id', type=str, required=True,
              help='The ID of the google drive you want to query')
@click.option('-f', '--folder_id', type=str, required=True,
              help='The ID of the directory in your google drive you want to list')
@click.option('-u', '--users', type=str, required=True,
              help='Comman separated list of users to compile timesheets for')
@click.option('-y', '--year', type=str, required=True,
              help='Year to compile timesheets for')
@click.option('-w', '--week', type=int, required=False, default=None,
              help='week to compile timesheets for 1-53')
@click.option('-m', '--month', type=int, required=False, default=None,
              help='week to compile timesheets for 1-12')
def main(
        drive_id: str,
        folder_id: str,
        users: str,
        year: str,
        week: str,
        month: str
    ):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """

    # Must have week or month
    if not week and not month:
        raise RuntimeError("One of week or month must be specified")
    if week and month:
        raise RuntimeError("Only one of week and month is allowed") 
    if week and not (week in range(1,54)):
        raise RuntimeError("Week must be in range 1-53")
    if month and not (month in range(1,13)):
        raise RuntimeError("Month must be in range 1-12")

    # auth
    scopes = [DRIVE_READ_ONLY_SCOPE,SHEETS_READ_ONLY_SCOPE]
    print(f'These are the scopes: {scopes}')
    creds = google_sheets_read.check_creds(scopes,GOOGLE_PROJECT_CREDENTIALS_FILE,TOKEN_FILE)

    # get all timesheets
    try:
        service = build("drive", "v3", credentials=creds)
        items = []
        items = google_drive_list.read_directory(service, drive_id, folder_id, GOOGLE_SHEET_FILE_MINE)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

    # check the format and get the dataframes
    users = users.split(",")
    re_checkfilename=re.compile(f'^({"|".join(users)})-{year}-{week}$')
    service = build("sheets", "v4", credentials=creds)
    df = pandas.DataFrame()
    for item in items:
        matches = re_checkfilename.match(item["name"])
        if matches:
            print(f'Downloading: {item["name"]}')
            df_timesheet = google_sheets_read.read_sheet(service,item['id'],'timesheet')
            i = 0
            for i, row in df_timesheet.iterrows():
                if row["client_code"] == "" and row["task_code"] == "":
                    break
            # slice the bit we want
            df_timesheet = df_timesheet.loc[0:i-1,["client_code","task_code"] + DAYS]
            # add the name
            df_timesheet["name"] = matches[1]

            # zero all the empty cells
            for day in DAYS:
                df_timesheet.loc[df_timesheet[day]=="",day] = 0

            # work out total
            df_timesheet["total"] = 0
            for day in DAYS:
                df_timesheet[day] = df_timesheet[day].astype(float)
                df_timesheet["total"] = df_timesheet["total"] + df_timesheet[day]

            df = pandas.concat([df,df_timesheet])

    # output
    output_filename = f'consolidated-{year}-{week}.csv'
    output_filename = os.path.join("data","timesheets",output_filename)
    print(df)
    df.to_csv(output_filename,index=False)
    print(f'Wrote to: {output_filename}')










if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
