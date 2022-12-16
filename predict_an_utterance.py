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
@click.option('-i','--input',type=str,required=True,help='Input utterance')
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-v', '--verbose',is_flag=True,default=False,help='Increase logging level')
def main(input: str, username: str, password: int, namespace: bool, playbook: str, bearertoken: str, verbose: bool):

    # check which authorization method using
    if bearertoken == '':
        for arg in ['username','password']:
            if arg == '':
                raise Exception(f'If bearer token not provided, must provide username and password')
        headers = authorize(username,password)
    else:
        headers = get_headers(bearertoken)
        
    # determine which intents to analyse        
    response_dict = predict(headers, input, namespace, playbook)
    print(f'Returned {len(response_dict["matches"])} matches')
    
    # dump entities
    if 'entityMatches' in response_dict.keys():
        print(json.dumps(response_dict['entityMatches'],indent=2))
    
    i =0
    for intent in response_dict['matches']:
        intent_full = get_intent(headers, input, namespace, playbook, intent['id'])
        if i >= 3:
            break
        metadata = {}
        try:
            metadata = intent_full['metadata']
        except KeyError:
            pass
        print(f'{intent["score"]:.2f} {"-".join(intent["hierarchyNames"])} {metadata}')       
        i = i+1
         
    if verbose:
        print(json.dumps(response_dict,indent=2))

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
        raise Exception(f'Not authorised, google returned {auth_response.status_code} {auth_response.json()}')
    idToken = auth_response.json()['idToken']
    headers = get_headers(idToken)
    # print('Retrieved idToken and added to headers')
    return headers

def get_intent(headers: str, sentence: str, namespace: str, playbook: str, intent_id: str) -> dict:
    '''Get the metdata for the intent needed'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents/{intent_id}'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response")
        print(response.status_code)
        print(response.text)
        quit()
    return response.json()

def get_intent_metadata_from_workspace(headers: str, sentence: str, namespace: str, playbook: str, intent_id: str) -> dict:
    '''Get the metdata for the intent needed'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "format": 7, # Humanfirst JSON
        "format_options": {
            "hierarchical_intent_name_disabled": False,
            "hierarchical_delimiter": "-",
            "zip_encoding": False,
            "hierarchical_follow_up": False,
            "include_negative_phrases": False
        },
        "intent_ids": [ # this doesn't appear to work for us - doesn't return the intent-id passed - having to filter instead
        ]
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
    metadata = {}
    for intent in response_dict['intents']:
        if intent['id'] == intent_id:
            assert(isinstance(intent,dict))
            if "metadata" in intent.keys():
                metadata = intent["metadata"]
            break
    return metadata

def predict(headers: str, sentence: str, namespace: str, playbook: str) -> dict:
    '''Get response_dict of matches and hier matches for an input'''
    payload = {
        "namespace": "string",
        "playbook_id": "string",
        "input_utterance": sentence
    }

    url = f'https://api.humanfirst.ai/v1alpha1/nlu/predict/{namespace}/{playbook}'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response")
        print(response.status_code)
        quit()
    response_dict = response.json()
    return response_dict

def batchPredict(headers: str, sentences: list, namespace: str, playbook: str) -> dict:
    '''Get response_dict of matches and hier matches for a batch of sentences'''
    print(f'Analysing {len(sentences)} sentences')
    payload = {
        "namespace": "string",
        "playbook_id": "string",
        "input_utterances": sentences
    }

    url = f'https://api.humanfirst.ai/v1alpha1/nlu/predict/{namespace}/{playbook}/batch'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response")
        print(response.status_code)
        quit()
    response_dict = response.json()['predictions']
    return response_dict 

if __name__ == '__main__':
    main()