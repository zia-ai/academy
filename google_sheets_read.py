"""
python google_sheets_read.py

Based on this very unquick quick start
https://developers.google.com/sheets/api/quickstart/python

Shows how to authenticate project via OAuth and Sheet via interactive logon.
Deal with typical uri redirect issues with using a VM like academy
How to take in the id of the google sheet and deal with tabs and ranges
How to deal with the discovery API and use see to see methods and where to
find documentation on methods.

Outputs a data frame with the tab from the sheet you want.

"""
# ******************************************************************************************************************120

# standard imports
import os.path
import see
import pandas

# 3rd party imports
import click
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# custom imports

# Scope constants
# https://developers.google.com/sheets/api/scopes
READ_ONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
READWRITE_SCOPE = "https://www.googleapis.com/auth/spreadsheets"

ALPHA_TRANSLATE = ["A","B","C","D","E","F","G",
                   "H","I","J","K",
                   "L","M","N","O","P",
                   "Q","R","S",
                   "T","U","V",
                   "W","X","Y","Z",
                   "AA","AB","AC","AD","AE","AF","AG",
                   "AH","AI","AJ","AK",
                   "AL","AM","AN","AO","AP",
                   "AQ","AR","AS",
                   "AT","AU","AV",
                   "AW","AX","AY","AZ"]

GOOGLE_PROJECT_CREDENTIALS_FILE = ".google-credentials.json"
TOKEN_FILE = ".token.json"

SUPPORTED_SHEET_TYPES = ["GRID"]
# "SHEET_TYPE_UNSPECIFIED","GRID","OBJECT","DATA_SOURCE" v4
# https://sheets.googleapis.com/$discovery/rest?version=v4

@click.command()
@click.option('-s', '--sheet_id', type=str, required=True, help='Google Sheet ID')
@click.option('-t', '--spreedsheet_tab', type=str, required=True, help="Name of tab")
@click.option('-w', '--write_mode', is_flag=True, type=bool, required=False, default=False,
              help='Whether to open sheet in write mode')
def main(sheet_id: str,
         spreedsheet_tab: str,
         write_mode: bool = False) -> None: # pylint: disable=unused-argument
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """

    scopes = [READ_ONLY_SCOPE]
    if write_mode:
        scopes.append(READWRITE_SCOPE)
    print(f'These are the scopes: {scopes}')

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time. It contains the scopes it was created with.
    if os.path.exists(TOKEN_FILE):
        # read from token.json (.gitignored)
        creds = Credentials.from_authorized_user_file(TOKEN_FILE)
        # check if it has the scopes we need.
        if write_mode:
            if not READWRITE_SCOPE in creds.scopes:
                creds = None

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # This is an OAuth Client Setup and then the key downloaded
            # This says you are allowed to contact the Google API for sheets
            # via a project
            # it doesn't control access to the actual sheet
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_PROJECT_CREDENTIALS_FILE, scopes
            )
            # Control the port that is being used here
            # you will need to add this to your redirect URIs
            # it is very fussy about trailing chars.
            # http%3A%2F%2Flocalhost%3A33589%2F is
            # http://localhost:33589/
            creds = flow.run_local_server(port=33589)

        # Save the credentials for the next run
        with open(TOKEN_FILE,mode="w", encoding="utf8") as token:
            token.write(creds.to_json())

    try:
        # https://developers.google.com/sheets/api/reference/rest
        # https://sheets.googleapis.com/$discovery/rest?version=v4
        service = build("sheets", "v4", credentials=creds)

        # Call the Discovery API to get a sheets API method
        # This is really annoying that Visual Studio can't detect the methods because of the discovery doc
        # TODO: is there an extension?
        sheet = service.spreadsheets() # pylint: disable=no-member


        print("These are the methods that can be called on that API object")
        print(str(see.see(sheet)) + "\n")

        # Get a dict of the spreadsheet wihch has multiple tabs in under sheets
        spreadsheet = sheet.get(spreadsheetId=sheet_id).execute()
        for tab in spreadsheet['sheets']:
            # get an object which tells you it's size
            # {
            # "properties": {
            #     "sheetId": 1878245818,
            #     "title": "timesheet",
            #     "index": 0,
            #     "sheetType": "GRID",
            #     "gridProperties": {
            #     "rowCount": 1000,
            #     "columnCount": 27
            #     }
            # }
            # }
            # can assemble range  - ord to get unicode value of char
            # or just do as range up to

            # Look for the tab we are interested in.
            if tab["properties"]["title"] == spreedsheet_tab:

                # Asset it's a grid data
                if not tab["properties"]["sheetType"] in SUPPORTED_SHEET_TYPES:
                    raise RuntimeError(f'Only these sheetTypes are supported {",".join(SUPPORTED_SHEET_TYPES)}')

                # Get the range of the data for A1 notation
                # https://developers.google.com/sheets/api/guides/concepts#expandable-1
                print(len(ALPHA_TRANSLATE))
                max_col = ALPHA_TRANSLATE[tab["properties"]["gridProperties"]["columnCount"]-1]
                max_row = tab["properties"]["gridProperties"]["rowCount"]
                range_cells = f'{spreedsheet_tab}!A1:{max_col}{max_row}'
                print(f'range_cells of data is: {range_cells}')

                # Retrieve those values as a dict with property values
                values = sheet.values().get(spreadsheetId=sheet_id,range=range_cells).execute()["values"]
                df = pandas.DataFrame(values)

                # assume a header row
                new_header = df.iloc[0] # Grab the first row for the header
                df = df[1:]             # Take the data less the header row
                df.columns = new_header # Set the header row as the df header
                print(df)

    except HttpError as err:
        print(err)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
