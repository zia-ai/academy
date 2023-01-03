#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# MAKE SURE NLU IS TRAINED IN TARGET WORKSPACE
#
# export HF_PASSWORD=<password>
#
# python predict_utterance.py
# -i "Yo! It's going to need to be a new shipping address as my circumstances have changed"
# -u <username>
# -p $HF_PASSWORD
# -b playbook-id
#
# To use with bearer token on on-prem
# HF_BEARER=`hf auth print-access-token`
# or
# HF_BEARER=`zia auth print-access-token`
#
# python predict_utterance.py --bearer $HF_BEARER -i "Utterance"
#
# *****************************************************************************

# standard imports
import requests
import json
import base64

# third party imports
import click


@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-v', '--verbose', is_flag=True, default=False, help='Increase logging level')
@click.option('-o', '--outputdir', type=str, default='./data/', help='Where to output playbook')

def main(username: str, password: int, namespace: bool, playbook: str, bearertoken: str, verbose: bool, outputdir: str):

    # check which authorization method using
    if bearertoken == '':
        for arg in ['username', 'password']:
            if arg == '':
                raise Exception(
                    f'If bearer token not provided, must provide username and password')
        headers = authorize(username, password)
    else:
        headers = get_headers(bearertoken)

    if not outputdir.endswith('/'):
        outputdir = outputdir + '/'
               
    workspace_info = get_playbook_info(headers, namespace, playbook)   
        
    with open(f'{outputdir}{namespace}-{playbook}-info.json','w') as file_out:
        file_out.write(json.dumps(workspace_info,indent=2))      

    workspace = get_playbook(headers, namespace, playbook)

    with open(f'{outputdir}{namespace}-{playbook}.json','w') as file_out:
        file_out.write(json.dumps(workspace,indent=2))      
    
        
    with open(f'{outputdir}{namespace}-{playbook}.json','w') as file_out:
        file_out.write(json.dumps(workspace,indent=2))        

def get_playbook_info(headers: str, namespace: str, playbook: str) -> dict:
    '''Returns metadata of playbook'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/playbooks/{namespace}/{playbook}'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response")
        print(response.status_code)
        print(response.text)
        quit()
    return response.json()


def get_playbook(headers: str, namespace: str, playbook: str) -> dict:
    '''Returns the actual training information including where present in the workspace
    * intents
    * examples
    * entities
    * tags
    '''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "format": 7,
        "format_options": {
            "hierarchical_intent_name_disabled": True,
            "hierarchical_delimiter": "-",
            "zip_encoding": False,
            "hierarchical_follow_up": True,
            "include_negative_phrases": False
        }
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents/export'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response")
        print(response.status_code)
        print(response.text)
        quit()
    response = response.json()['data']
    response = base64.b64decode(response)
    response = response.decode('utf-8')
    response_dict = json.loads(response)
    return response_dict


def get_headers(bearer_token: str) -> dict:
    bearer_string = f'Bearer {bearer_token}'
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json',
        'Authorization': bearer_string
    }
    return headers


def authorize(username: str, password: str) -> dict:
    '''Get bearer token for a username and password'''

    # print(f'Hello {username} getting auth token details')

    auth_url = 'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyA5xZ7WCkI6X1Q2yzWHUrc70OXH5iCp7-c'

    headers = get_headers('')

    auth_body = {
        "email": username,
        "password": password,
        "returnSecureToken": True
    }
    auth_response = requests.request(
        "POST", auth_url, headers=headers, data=json.dumps(auth_body))
    if auth_response.status_code != 200:
        raise Exception(
            f'Not authorised, google returned {auth_response.status_code} {auth_response.json()}')
    idToken = auth_response.json()['idToken']
    headers = get_headers(idToken)
    # print('Retrieved idToken and added to headers')
    return headers


if __name__ == '__main__':
    main()
