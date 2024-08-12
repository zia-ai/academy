"""
python google_mail_read.py

https://developers.google.com/gmail/api/quickstart/python


"""
# ******************************************************************************************************************120

# standard imports
import base64

# 3rd party imports
import click
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# custom imports
import google_sheets_read

# Scope constants
# https://developers.google.com/sheets/api/scopes
READ_ONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"

GOOGLE_PROJECT_CREDENTIALS_FILE = ".google-credentials.json"
TOKEN_FILE = ".token.json"


@click.command()
@click.option('-u', '--user_email', type=str, required=True,
              help='User email to search for')
@click.option('-m', '--max_results', type=int, required=False, default=10,
              help='May number of results')
def main(user_email: str, max_results: int) -> None: # pylint: disable=unused-argument
    """
    Gmail example getting the labels from your mail file
    Doesn't require a mailbox, just works from the credentials
    """

    # Decide which scopes we want
    scopes = [READ_ONLY_SCOPE]

    # authenticate
    # https://console.cloud.google.com/apis/credentials
    creds = google_sheets_read.check_creds(scopes=scopes,
                        credential_location=GOOGLE_PROJECT_CREDENTIALS_FILE,
                        token_location=TOKEN_FILE)

    # Build service
    # https://developers.google.com/gmail/api/quickstart/python
    service = build("gmail", "v1", credentials=creds)


    # show all labels
    try:
        results = service.users().labels().list(userId="me").execute() # pylint: disable=no-member
        labels = results.get("labels", [])

        if not labels:
            print("No labels found.")
        else:
            print("Labels:")
            for label in labels:
                print(label["name"])

    except HttpError as error:
        print(f"A HttpError error occurred: {error}")

    # get list of messages
    # https://developers.google.com/gmail/api/reference/rest/v1/users.messages/list
    messages = []
    try:
        results = service.users().messages().list(  # pylint: disable=no-member
            userId="me",
            q=user_email,
            maxResults=max_results
        ).execute()
        messages = results.get("messages",[])
        print(messages)
    except HttpError as error:
        print(f"A HttpError error occurred: {error}")

    # for each message get the actual message
    # https://developers.google.com/gmail/api/reference/rest/v1/users.messages/get
    for m in messages:
        try:
            results = service.users().messages().get(  # pylint: disable=no-member
                userId="me",
                id = m["id"]
            ).execute()
            payload = results.get("payload",[])
            parts = payload.get("parts",[])
            for part in parts:
                if part["mimeType"] == "text/plain":
                    print("***************************************")
                    print(base64.urlsafe_b64decode(part["body"]["data"]))
        except HttpError as error:
            print(f"A HttpError error occurred: {error}")



if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
