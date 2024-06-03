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

# If modifying these scopes, delete the file token.json.
READ_ONLY_SCOPE = "https://www.googleapis.com/auth/drive.metadata.readonly"
READWRITE_SCOPE = ""

# Where credentials come from
GOOGLE_PROJECT_CREDENTIALS_FILE = ".google-credentials.json"
TOKEN_FILE = ".token.json"

# Port that will start interactive authentication session.
AUTH_SERVER_PORT = 33589

# MimeTypes
GOOGLE_DOC_FILE_MINE = "application/vnd.google-apps.document"
GOOGLE_SHEET_FILE_MINE = "application/vnd.google-apps.sheet"
GOOGLE_FOLDER_MIME = "application/vnd.google-apps.folder"

@click.command()
@click.option('-d', '--drive_id', type=str, required=True,
              help='The ID of the google drive you want to query')
@click.option('-f', '--folder_id', type=str, required=True,
              help='The ID of the directory in your google drive you want to list')
@click.option('-w', '--write_mode', is_flag=True, type=bool, required=False, default=False,
              help='Whether to open sheet in write mode')
def main(
        drive_id: str,
        folder_id: str,
        write_mode: bool
    ):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    scopes = [READ_ONLY_SCOPE]
    if write_mode:
        scopes.append(READWRITE_SCOPE)
    print(f'These are the scopes: {scopes}')

    creds = None
    if os.path.exists(".token.json"):
        creds = Credentials.from_authorized_user_file(".token.json", scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                ".google-credentials.json", scopes
            )
            creds = flow.run_local_server(port=AUTH_SERVER_PORT)
        # Save the credentials for the next run
        with open(".token.json", "w") as token:
            token.write(creds.to_json())

    # https://developers.google.com/drive/api/guides/search-files
    try:
        service = build("drive", "v3", credentials=creds)

        # q = f"mimeType = 'application/vnd.google-apps.folder' and '{directory_id}' in parents"
        q=f"\'{folder_id}\' in parents"
        # q="name contains 'Slack Reminder'" # search for things containing document
        print(type(q))
        print(q)
        # Call the Drive v3 API
        drive = service.files()
        results = drive.list(pageSize=10,
            corpora="allDrives",
            # supportsTeamDrivesRequired=True,
            q=q,
            # driveId={drive_id},
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
            # fields="nextPageToken, files(id, name)")
        ).execute()

        # Files appears to get directories.
        # mimeType used to distinquish

        items = results.get("files", [])
        # Don't need to requery to get metadata but can like this.
        # print(drive.get(supportsAllDrives=True,fileId="1QnWoIiGN-FnAba9msagh7tsZznn_gzqOb2JBjs2mWXE").execute())

        if not items:
            print("No files found.")
            return

        for item in items:
            if item['mimeType'] == GOOGLE_DOC_FILE_MINE:
                print("DOC")
            elif item['mimeType'] == GOOGLE_FOLDER_MIME:
                print("DIR")
            elif item['mimeType'] == GOOGLE_SHEET_FILE_MINE:
                print("SHEET")
            else:
                print(f'WTF: {item["mimeType"]}')
            print(f"{item['name']} ({item['id']})")


    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
