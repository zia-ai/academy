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
GOOGLE_SHEET_FILE_MINE = "application/vnd.google-apps.spreadsheet"
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
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                ".google-credentials.json", scopes
            )
            creds = flow.run_local_server(port=AUTH_SERVER_PORT)
        # Save the credentials for the next run
        with open(TOKEN_FILE, mode="w",encoding="utf8") as token:
            token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)

        read_directory(service, drive_id, folder_id, GOOGLE_SHEET_FILE_MINE)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

def read_directory(service, drive_id: str, folder_id=str,
                   looking_for_mime: str = None, counter: int = 0, indent: str = ""):
    """Recursively loop through a directory optionally looking for """

    # https://developers.google.com/drive/api/guides/search-files

    # Can search by mimetype - here we are filtering after
    # q = f"mimeType = 'application/vnd.google-apps.folder' and '{directory_id}' in parents"
    q=f"trashed=false and \'{folder_id}\' in parents"

    # do a general search
    # q="name contains 'Slack Reminder'" # search for things containing document

    # Call the Drive v3 API for this directory
    drive = service.files()
    results = drive.list(pageSize=1000,
        corpora="drive",
        q=q,
        driveId=drive_id,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
        # fields="nextPageToken, files(id, name)")
    ).execute()

    # Files appears to get directories.
    # mimeType used to distinquish

    items = results.get("files", [])
    # Don't need to requery to get metadata but can like this.

    folder = drive.get(supportsAllDrives=True,fileId=folder_id).execute()
    print(f'{indent}Searching: {folder["name"]}')

    if not items:
        print("No files found.")
        return

    for item in items:
        if item['mimeType'] == GOOGLE_FOLDER_MIME:
            read_directory(service, item["driveId"], item["id"],
                           looking_for_mime=looking_for_mime, counter=counter, indent=indent+"  ")
        elif looking_for_mime is None:
            do_whatever(item,indent)
        elif item['mimeType'] == looking_for_mime:
            do_whatever(item,indent)
        else:
            continue

    return counter

def do_whatever(item,indent):
    """Replace with the function you want"""
    indented_filename = indent+'  '+item['name']
    print(f"{indented_filename:<50} {item['id']:<25} {item['mimeType']}")

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
