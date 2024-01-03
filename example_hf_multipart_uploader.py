"""
 Code Language:   python
 Script:          upload_files_to_hf.py
 Imports:         re, json, click, requests, requests_toolbelt
 Functions:       main(), upload_multipart(), check_status(), replace(),
                  delete_file(), get_conversion_set_id(), get_conversion_source_id()
 Description:     Upload files to HumanFirst
"""
# **********************************************************************************************************************

# standard imports
import re
import json
import os

# third party imports
import requests
import requests_toolbelt
import click
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-h', '--filepath', type=str, required=True, help='file to upload to HumanFirst')
@click.option('-b', '--upload_name', type=str, default="", help='name assigned to the uploaded file to HumanFirst')
@click.option('-s', '--convoset_name', type=str, required=True, help='Conversation set name / data folder name')
@click.option('-f', '--force_upload', is_flag=True, default=False, help='Replaces the file name of the ')
def main(username: str,
         password: str,
         namespace: str,
         filepath: str,
         convoset_name: str,
         upload_name: str,
         force_upload: bool) -> None:
    """Main function"""

    hf_api = humanfirst.apis.HFAPI(username=username, password=password)

    # if conversation set already exists, then it returns the source id of the existing conversation set,
    # otherwise creates a new one
    conversation_source_id = hf_api.create_conversation_set(namespace=namespace, convoset_name=convoset_name)

    if upload_name == "":
        upload_name = os.path.basename(filepath).replace(".json","")
    print(upload_name)
    hf_api.upload_multipart(namespace, conversation_source_id, filepath, upload_name, force_upload)


def check_status(response: requests.models.Response, headers: str, namespace: str,
                 conversation_source_id: str, filepath: str, upload_name: str, force: bool) -> None:
    """Checks the status of the file upload API call response

    Parameters
    ----------

    response: requests.models.response
        file upload API call response

    headers: dict
        headers containing content-type and authorization bearer token

    namespace: str
        HumanFirst namespace

    filepath: str
        file to upload to HumanFirst

    conversation_source_id: str
        conversation source id

    upload_name: str
        name assigned to the uploaded file to HumanFirst

    force: bool
        Prevents runtime user input

    Returns
    -------

    None
    """
    if response.status_code != 201:
        if response.text:
            if re.sub("\n$", "", response.text) == "file already exists":
                print("File already exists")
                if force is True:
                    replace(headers, namespace, conversation_source_id, filepath, upload_name, force)
                else:
                    user_choice = input("""1. Replace exisiting file\n
                                        2. Upload the file with new name\n3. Quit\nEnter your choice: """)
                    if user_choice == "1":
                        replace(headers, namespace, conversation_source_id, filepath, upload_name, force)
                    elif user_choice == "2":
                        upload_name = input("Enter new name for the file: ")
                        upload_multipart(headers, namespace, conversation_source_id, filepath, upload_name, force)
                    elif user_choice == "3":
                        quit()
                    else:
                        raise RuntimeError("Incorrect choice")
            else:
                raise RuntimeError(f"Got {response.status_code} response: {response.text}")
        else:
            raise RuntimeError(f"Got {response.status_code} response: {response.text}")
    else:
        print(f"{filepath} uploaded successfully to HumanFirst")


def replace(headers: str, namespace: str, conversation_source_id: str, filepath: str, upload_name: str, force: bool) -> None:
    """Replaces the exisitng file with the new file in HumanFirst"""

    delete_file(namespace, conversation_source_id, upload_name)
    upload_multipart(namespace, conversation_source_id, filepath, upload_name, force)


def get_conversion_set_id(headers: str, namespace: str, convoset: str) -> tuple:
    """Lookup a conversation set id and conversation source id in a namespace based on its name

    Parameters
    ----------

    headers: dict
        headers containing content-type and authorization bearer token

    namespace: str
        HumanFirst namespace

    convoset: str
        Conversation set name / data folder name

    Returns
    -------

    conversation_set_id, conversation_source_id : tuple
        conversation set id and conversation source id
    """

    payload = {}
    url = f"https://api.humanfirst.ai/v1alpha1/conversation_sets?namespace={namespace}"
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Got {response.status_code} Response")
        quit()
    conversation_sets = response.json()['conversationSets']
    # print(json.dumps(response.json(),indent=3))
    conversation_set_id = None
    conversation_source_id = None
    for conversation_set in conversation_sets:
        if conversation_set['name'] == convoset:
            conversation_set_id = conversation_set['id']
            conversation_source_id = conversation_set['sources'][0]['conversationSourceId']
            break
    return conversation_set_id, conversation_source_id


def get_conversion_source_id(headers: str, namespace: str, convoset: str) -> str:
    """Lookup a conversation source id in a namespace based on its name.
    This is not ideal for getting the conversation source id because
    it never updates conversation source name, even after updating it in the HumanFirst console.
    Hence finding the conversation source id using the data folder name becomes difficult.
    Conversation sets get the updated name of the data folder.
    Hence it is better to use conversation sets than conversation sources.

    Parameters
    ----------

    headers: dict
        headers containing content-type and authorization bearer token

    namespace: str
        HumanFirst namespace

    convoset: str
        Conversation set name / data folder name

    Returns
    -------

    conversation_source_id : str
        conversation source id
    """
    payload = {}
    url = f"https://api.humanfirst.ai/v1alpha1/conversation_sources?namespace={namespace}"
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Got {response.status_code} Response")
        quit()
    conversation_sources = response.json()["conversationSources"]
    conversation_source_id = None
    for src in conversation_sources:
        # the convoset must be the initial data folder name
        if src['name'] == f'{convoset} - User upload':
            conversation_source_id = src['id']
            break
    return conversation_source_id


def authorize(username: str, password: str) -> dict:
    """Get bearer token for a username and password

    Parameters
    ----------

    username: str
        HumanFirst username

    password: str
        HumanFirst password

    Returns
    -------

    headers: dict
        headers containing content-type and authorization bearer token
    """
    auth_url = 'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyA5xZ7WCkI6X1Q2yzWHUrc70OXH5iCp7-c'
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json'
    }
    auth_body = {
        "email": username,
        "password": password,
        "returnSecureToken": True
    }
    auth_response = requests.request(
        "POST", auth_url, headers=headers, data=json.dumps(auth_body))
    if auth_response.status_code != 200:
        raise Exception(f'Not authorised, google returned {auth_response.status_code} {auth_response.json()}')
    idToken = auth_response.json()['idToken']
    headers['Authorization'] = f'Bearer {idToken}'
    # print('Retrieved idToken and added to headers')
    return headers


if __name__ == "__main__":
    main()
