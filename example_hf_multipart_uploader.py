#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python example_hf_multipart_uploader.py
#
# *****************************************************************************
    
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
@click.option('-f', '--filename', type=str, required=True, help='file to upload to HumanFirst')
@click.option('-s', '--convoset', type=str, required=True, help='Conversation set name to upload to')

def main(username: str, password: str, namespace: str, filename: str, convoset:str):
    headers = authorize(username,password)
    conversation_set_id = get_conversion_set_id(headers, namespace, convoset)
    conversation_source_id = get_conversion_source_id(headers, namespace, convoset)
    answer = multi_part(headers, namespace, conversation_source_id, filename)   
    
def multi_part(headers:str, namespace: str, conversation_source_id: str, filename: str):

    url = f"https://api.humanfirst.ai/v1alpha1/files/{namespace}/{conversation_source_id}"
    
    upload_file = open(f'{filename}', 'rb')
    print(len(upload_file.read()))
    payload = requests_toolbelt.multipart.encoder.MultipartEncoder(
        fields={
            'format': 'IMPORT_FORMAT_HUMANFIRST_JSON',
            'file': (filename, upload_file, 'application/json')}
    )
    # This is the magic bit - you must set the content type to include the boundary information
    # multipart encoder makes working these out easier    
    print(payload.content_type)   
    headers["Content-Type"] = payload.content_type
    response = requests.request("POST", url, headers=headers, data=payload)   
    print(response.text.encode('utf8'))
    
def get_conversion_set_id(headers: str, namespace: str, set: str):
    '''Lookup a conversation set id in a namespace based on its name'''
    
    payload={}
    url = f"https://api.humanfirst.ai/v1alpha1/conversation_sets?namespace={namespace}"
    response = requests.request("GET", url, headers=headers,data=payload)
    if response.status_code != 200:
        print(f"Got {response.status_code} Response")
        quit()
    conversation_sets = response.json()['conversationSets']
    conversation_set_id = None
    for s in conversation_sets:
        if s['name'] == set:
            conversation_set_id = s['id']
            break
    return conversation_set_id

def get_conversion_source_id(headers: str, namespace: str, convoset: str):
    payload={}
    url = f"https://api.humanfirst.ai/v1alpha1/conversation_sources?namespace={namespace}"
    response = requests.request("GET", url, headers=headers,data=payload)
    if response.status_code != 200:
        print(f"Got {response.status_code} Response")
        quit()
    conversation_sources = response.json()["conversationSources"]
    conversation_source_id = ''
    for src in conversation_sources:
        if src['name'] == f'{convoset} - User upload':
            conversation_source_id = src['id']
            break
    return conversation_source_id

def authorize(username: str, password: str) -> dict:
    '''Get bearer token for a username and password'''
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