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

# 3rd party imports
import click
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# custom imports
import google_sheets_read

# If modifying these scopes, delete the file token.json.
READ_ONLY_SCOPE = "https://www.googleapis.com/auth/drive.metadata.readonly"

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
def main(
        drive_id: str,
        folder_id: str
    ):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """

    # creds
    creds = google_sheets_read.check_creds([READ_ONLY_SCOPE],GOOGLE_PROJECT_CREDENTIALS_FILE,TOKEN_FILE)

    # print(json.dump(creds))

    # build service and read directory
    try:
        service = build("drive", "v3", credentials=creds)
        items = read_directory(service, drive_id, folder_id, GOOGLE_SHEET_FILE_MINE)
        print(f'Total found: {len(items)}')

        for item in items:
            do_whatever(item)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

    #

def read_directory(service, drive_id: str, folder_id: str, looking_for_mime: str = None) -> list:
    """Recursively loop through a directory optionally looking for """
    return_items = []

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
    print(f'Searching: {folder["name"]}')

    if not items:
        print("No files found.")
        return

    for item in items:
        if item['mimeType'] == GOOGLE_FOLDER_MIME:
            return_items.extend(read_directory(service, item["driveId"], item["id"],
                           looking_for_mime=looking_for_mime))
        elif looking_for_mime is None:
            return_items.append(item)
        elif item['mimeType'] == looking_for_mime:
            return_items.append(item)
        else:
            continue

    return return_items

def do_whatever(item):
    """Replace with the function you want"""
    print(f"{item['name']:<50} {item['id']:<25} {item['mimeType']}")

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
