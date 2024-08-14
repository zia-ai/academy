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
# import see
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

GOOGLE_PROJECT_CREDENTIALS_FILE = "google/.google-credentials.json"
TOKEN_FILE = "google/.token.json"

SUPPORTED_SHEET_TYPES = ["GRID"]
# "SHEET_TYPE_UNSPECIFIED","GRID","OBJECT","DATA_SOURCE" v4
# https://sheets.googleapis.com/$discovery/rest?version=v4

@click.command()
@click.option('-s', '--sheet_id', type=str, required=True, help='Google Sheet ID')
@click.option('-t', '--spreadsheet_tab', type=str, required=True, help="Name of tab")
@click.option('-w', '--write_mode', is_flag=True, type=bool, required=False, default=False,
              help='Whether to open sheet in write mode')
def main(sheet_id: str,
         spreadsheet_tab: str,
         write_mode: bool = False) -> None: # pylint: disable=unused-argument
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """

    # Decide which scopes we want
    scopes = [READ_ONLY_SCOPE]
    if write_mode:
        scopes.append(READWRITE_SCOPE)
    print(f'Scopes: {scopes}')

    # authenticate
    # check downloaded .googe-credentials file
    # https://console.cloud.google.com/apis/credentials
    creds = check_creds(scopes=scopes,
                        credential_location=GOOGLE_PROJECT_CREDENTIALS_FILE,
                        token_location=TOKEN_FILE)

    # Build service
    # https://developers.google.com/sheets/api/reference/rest
    # https://sheets.googleapis.com/$discovery/rest?version=v4
    service = build("sheets", "v4", credentials=creds)

    # read sheet
    df = read_sheet(service, sheet_id, spreadsheet_tab)

    # print it
    print(df)


def check_creds(scopes: list, credential_location: str,
                token_location: str) -> dict:
    """Authenticates Google API checking scopes against existing"""


    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time. It contains the scopes it was created with.
    if os.path.exists(token_location):

        # Extremely annoying thing here, if you get refresh_token not present
        # This is because you deleted a token with one in then generated a new one
        # whilst having already consented.
        # to fix this logout of your google session
        # delete token.json
        # rerun

        # read from token.json (.gitignored)
        creds = Credentials.from_authorized_user_file(token_location)
        # check if it has the scopes we need.
        for s in scopes:
            if not s in set(creds.scopes):
                print(f'Clearing creds as missing scope: {s}')
                creds = None
                break

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
                credential_location, scopes
            )
            # Control the port that is being used here
            # you will need to add this to your redirect URIs
            # it is very fussy about trailing chars.
            # http%3A%2F%2Flocalhost%3A33589%2F is
            # http://localhost:33589/
            creds = flow.run_local_server(port=33589)

        # Save the credentials for the next run
        with open(token_location, mode="w", encoding="utf8") as token:
            token.write(creds.to_json())
            print(f'Wrote new token file to {token_location}')

    return creds

def read_sheet(service, sheet_id: str, spreadsheet_tab: str) -> pandas.DataFrame:
    "Read the sheet and tab passed with a range of A1:A1000"
    try:

        # Call the Discovery API to get a sheets API method
        # This is really annoying that Visual Studio can't detect the methods because of the discovery doc
        # TODO: is there an extension?
        sheet = service.spreadsheets() # pylint: disable=no-member

        # print("These are the methods that can be called on that API object")
        # print(str(see.see(sheet)) + "\n")

        # Get a dict of the spreadsheet wihch has multiple tabs in under sheets
        spreadsheet = sheet.get(spreadsheetId=sheet_id).execute()
        found_tab = False
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
            if tab["properties"]["title"] == spreadsheet_tab:
                found_tab = True

                # Asset it's a grid data
                if not tab["properties"]["sheetType"] in SUPPORTED_SHEET_TYPES:
                    raise RuntimeError(f'Only these sheetTypes are supported {",".join(SUPPORTED_SHEET_TYPES)}')

                # Get the range of the data for A1 notation
                # https://developers.google.com/sheets/api/guides/concepts#expandable-1
                max_col = ALPHA_TRANSLATE[tab["properties"]["gridProperties"]["columnCount"]-1]
                max_row = tab["properties"]["gridProperties"]["rowCount"]
                range_cells = f'{spreadsheet_tab}!A1:{max_col}{max_row}'
                # print(f'range_cells of data is: {range_cells}')

                # Retrieve those values as a dict with property values
                values = sheet.values().get(spreadsheetId=sheet_id,range=range_cells).execute()["values"]
                df = pandas.DataFrame(values)

                # assume a header row
                new_header = df.iloc[0].to_list() # Grab the first row for the header
                df = df[1:]                       # Take the data less the header row
                df.reset_index(drop=True,inplace=True,names=["sheet_row"])
                for i,col in enumerate(new_header):
                    df.rename(inplace=True,columns={i:col})
                return df

        if not found_tab:
            print(f"Could't locate tab: {spreadsheet_tab}")
            return pandas.DataFrame()

    except HttpError as err:
        print(err)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
