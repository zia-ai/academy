#!/usr/bin/env python
# -*- coding: utf-8 -*-
# *******************************************************************************************************************120
#  Code Language:   python
#  Script:          upload_files_to_hf.py
#  Imports:         re, json, click, requests, requests_toolbelt
#  Functions:       main(), upload_multipart(), check_status(), replace(),
#                   delete_file(), get_conversion_set_id(), get_conversion_source_id()
#  Description:     Upload files to HumanFirst
#
# **********************************************************************************************************************

# standard imports
import re
import json

# third party imports
import requests
import requests_toolbelt
import click


@click.command()
@click.option('-u', '--username', type=str, required=True, help='HumanFirst username')
@click.option('-p', '--password', type=str, required=True, help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-h', '--filepath', type=str, required=True, help='file to upload to HumanFirst')
@click.option('-b', '--upload_name', type=str, required=True, help='name assigned to the uploaded file to HumanFirst')
@click.option('-s', '--convoset', type=str, required=True, help='Conversation set name / data folder name')
@click.option('-f', '--force', is_flag=True, default=False, help='Prevents runtime user input')
def main(username: str, password: str, namespace: str, filepath: str, convoset: str, upload_name: str, force: bool) -> None:
    """Main function - Pre-step: initially create the data folder and manually upload the file.
                       Only then a conversation source id gets generated.

    Parameters
    ----------

    username: str
        HumanFirst username

    password: str
        HumanFirst password

    namespace: str
        HumanFirst namespace

    filepath: str
        file to upload to HumanFirst

    convoset: str
        Conversation set name / data folder name

    upload_name: str
        name assigned to the uploaded file to HumanFirst

    force: bool
        Prevents runtime user input

    Returns
    -------

    None
    """
    headers = authorize(username, password)
    conversation_set_id, conversation_source_id = get_conversion_set_id(headers, namespace, convoset)
    upload_multipart(headers, namespace, conversation_source_id, filepath, upload_name, force)


def upload_multipart(headers: dict, namespace: str, conversation_source_id: str, filepath: str, upload_name: str, force: bool) -> None:
    """Uploads multipart/form-data to HumanFirst

    Parameters
    ----------

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
    url = f"https://api.humanfirst.ai/v1alpha1/files/{namespace}/{conversation_source_id}"
    upload_file = open(filepath, 'rb')
    payload = requests_toolbelt.multipart.encoder.MultipartEncoder(
        fields={
            'format': 'IMPORT_FORMAT_HUMANFIRST_JSON',
            'file': (upload_name, upload_file, 'application/json')}
    )
    # This is the magic bit - you must set the content type to include the boundary information
    # multipart encoder makes working these out easier
    headers["Content-Type"] = payload.content_type
    response = requests.request("POST", url, headers=headers, data=payload)
    check_status(response, headers, namespace, conversation_source_id, filepath, upload_name, force)
    upload_file.close()


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
                    user_choice = input("1. Replace exisiting file\n2. Upload the file with new name\n3. Quit\nEnter your choice: ")
                    if user_choice == "1":
                        replace(headers, namespace, conversation_source_id, filepath, upload_name, force)
                    elif user_choice == "2":
                        upload_name = input("Enter new name for the file: ")
                        response = upload_multipart(headers, namespace, conversation_source_id, filepath, upload_name, force)
                    elif user_choice == "3":
                        quit()
                    else:
                        raise Exception("Incorrect choice")
            else:
                raise Exception(f"Got {response.status_code} response: {response.text}")
        else:
            raise Exception(f"Got {response.status_code} response: {response.text}")
    else:
        print(f"{filepath} uploaded successfully to HumanFirst")


def replace(headers: str, namespace: str, conversation_source_id: str, filepath: str, upload_name: str, force: bool) -> None:
    """Replaces the exisitng file with the new file in HumanFirst

    Parameters
    ----------

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
    delete_file(headers, namespace, conversation_source_id, upload_name)
    upload_multipart(headers, namespace, conversation_source_id, filepath, upload_name, force)


def delete_file(headers: str, namespace: str, conversation_source_id: str, filename: str) -> None:
    """Deletes data file in HumanFirst

    Parameters
    ----------

    headers: dict
        headers containing content-type and authorization bearer tokene

    namespace: str
        HumanFirst namespace

    conversation_source_id: str
        conversation source id

    filename: str
        name of the file to be deleted in HumanFirst

    Returns
    -------

    None
    """
    url = f"https://api.humanfirst.ai/v1alpha1/files/{namespace}/{conversation_source_id}/{filename}"
    payload = {}
    response = requests.request("DELETE", url, headers=headers, data=payload)
    if response.status_code == 200:
        print(f"Deleted {filename} successfully")
    else:
        raise Exception(f"Deleting existing file unsuccessful. \n Got {response.status_code} response.")


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
