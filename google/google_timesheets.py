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
import calendar
from dateutil import parser
import json

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
GOOGLE_PROJECT_CREDENTIALS_FILE = "google/.google-credentials.json"
TOKEN_FILE = "google/.token.json"

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
@click.option('-y', '--year', type=int, required=True,
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
    if week and not (week in range(1,54)): # pylint: disable=superfluous-parens
        raise RuntimeError("Week must be in range 1-53")
    if month and not (month in range(1,13)): # pylint: disable=superfluous-parens
        raise RuntimeError("Month must be in range 1-12")

    # auth
    scopes = [DRIVE_READ_ONLY_SCOPE,SHEETS_READ_ONLY_SCOPE]
    print(f'These are the scopes: {scopes}')

    # frig the refresh_token so it alwaysso it doesn't fail if it expects 
    # you to have one but you don't
    if os.path.isfile(TOKEN_FILE):
        token_file_in = open(TOKEN_FILE,encoding="utf8",mode="r")
        token_dict = json.load(token_file_in)
        token_file_in.close()
        if not "refresh_token" in token_dict.keys():
            token_dict["refresh_token"] = ""
            token_file_out = open(TOKEN_FILE,encoding="utf8",mode="w")
            json.dump(token_dict,token_file_out)
            token_file_out.close()
            print("Added refresh token")

    creds = google_sheets_read.check_creds(scopes,GOOGLE_PROJECT_CREDENTIALS_FILE,TOKEN_FILE)

    # get all timesheets
    try:
        service = build("drive", "v3", credentials=creds)
        items = []
        items = google_drive_list.read_directory(service, drive_id, folder_id, GOOGLE_SHEET_FILE_MINE)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

    # Work out users interested in
    users = users.split(",")

    # Work out weeks interested in
    weeks = []
    if month:
        cal = calendar.monthcalendar(year,month)
        week1st = parser.parse(f'{year}-{month}-01').isocalendar()[1] # get weeknumber for first week
        for i,w in enumerate(cal):
            weeks.append(f'{week1st + i:02}')
        print(f'{year}-{month} has week numbers: ')
        print(weeks)

        # turn cal into a lookup dict
        cal_dict = {}
        for i,w in enumerate(weeks):
            cal_dict[w] = cal[i]
        print(cal_dict)
    if week:
        weeks.append(str(week))
        print(weeks)

    # create google service
    service = build("sheets", "v4", credentials=creds)

    # if week get just that week
    if week:
        df = get_consolidated_df_for_week(items, users, year, week, service)  

    # if month loop through all the weeks 
    if month:
        df = pandas.DataFrame()
        # for each week get the datsheet
        for i,w in enumerate(weeks):

            # get the weeks dataframe
            df_week = get_consolidated_df_for_week(items, users, year, w, service)

            # if it's empty skip it
            if df_week.shape[0] == 0:
                continue

            # for each day rename it's column to the month
            for j,day in enumerate(DAYS):

                # if 0 drop it as non-unique and not required
                if cal_dict[w][j] == 0:
                    df_week.drop(columns=[day],inplace=True)
                # otherwise rename it to the date
                else:
                    mapper = {
                        day:str(cal_dict[w][j])
                    }
                    df_week.rename(columns=mapper,inplace=True)

            # drop the total
            df_week.drop(columns=['total'],inplace=True)

            # set the indexes
            df_week.set_index(['client_code','task_code','name'],inplace=True,drop=True)

            # fill NaN
            df_week.fillna(0,inplace=True)

            # join week onto month consolidation
            if i == 0:
                df = df_week
            else:
                df = df.join(df_week,how='outer')

        # work out total as final column
        df["total"] = df.sum(axis=1)

        # eliminate any zero rows

        # df = df.loc[df["total"]>0,:]
        print(df)

    if week:
        # output
        output_filename = f'consolidated-week-{year}-{week:02}.csv'
    if month:
        output_filename = f'consolidated-month-{year}-{month:02}.csv'
    output_filename = os.path.join("data","timesheets",output_filename)
    print(df)
    if week:
        df.to_csv(output_filename,index=False)
    if month:
        df.to_csv(output_filename,index=True)
    print(f'Wrote to: {output_filename}')

def get_consolidated_df_for_week(items: list, users: list, year: int, week: str, service) -> pandas.DataFrame:
    """Downloads all the files matching the week for all users and consolidates"""
    df = pandas.DataFrame()

    # regex to check whether we care about the file
    re_checkfilename=re.compile(f'^({"|".join(users)})-{year}-{week:02}$')
    print(f"Starting on week: {year}-{week}")
    print(f'Regex compiled as {re_checkfilename}')

    # cycle through all items downloading what we need
    for item in items:
        matches = re_checkfilename.match(item["name"])
        if matches:
            print(f'Downloading: {item["name"]}')
            df_timesheet = google_sheets_read.read_sheet(service,item['id'],'timesheet')
            i = 0
            for i, row in df_timesheet.iterrows():
                if row["client_code"] == "" and row["task_code"] == "":
                    break
                if row["client_code"] == None and row["task_code"] == None:
                    break
                if pandas.isna(row["client_code"]) and pandas.isna(row["task_code"]):
                    break

            # slice the bit we want
            df_timesheet = df_timesheet.loc[0:i-1,["client_code","task_code"] + DAYS]

            # add the name of the user
            df_timesheet["name"] = matches[1]

            # zero all the empty cells
            for day in DAYS:
                df_timesheet.loc[(df_timesheet[day]=="") | (df_timesheet[day].isna()),day] = 0

            # work out total
            df_timesheet["total"] = 0
            for day in DAYS:
                df_timesheet[day] = df_timesheet[day].astype(float)
                df_timesheet["total"] = df_timesheet["total"] + df_timesheet[day]


            # drop any zero total rows
            df_timesheet = df_timesheet.loc[df_timesheet["total"]>0,:]
            print(df_timesheet)

            # Consolidate duplicate rows by summing them
            df_timesheet = df_timesheet.groupby(["client_code", "task_code", "name"]).agg({day: 'sum' for day in DAYS})
            df_timesheet.reset_index(inplace=True)

            # Calculate the total again after aggregation
            df_timesheet["total"] = df_timesheet[DAYS].sum(axis=1)

            # Drop any rows with zero total
            df_timesheet = df_timesheet.loc[df_timesheet["total"] > 0, :]

            # Concatenate the consolidated DataFrame with the main DataFrame
            df = pandas.concat([df, df_timesheet])
            print(f'Concatenated df.shape: {df.shape}')

    return df








if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
