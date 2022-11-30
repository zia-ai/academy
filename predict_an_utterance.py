#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# MAKE SURE NLU IS TRAINED IN TARGET WORKSPACE
#
# python predict_utterance.py 
# -i "Yo! It's going to need to be a new shipping address as my circumstances have changed"
# -u username
# -p password
# -b playbook-id
#
# Testing making a change
#
# *****************************************************************************

# standard imports
import requests
import json

# third party imports
import click

@click.command()
@click.option('-i','--input',type=str,required=True,help='Input utterance')
@click.option('-u', '--username', type=str, required=True, help='HumanFirst username')
@click.option('-p', '--password', type=str, required=True, help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
def main(input: str, username: str, password: int, namespace: bool, playbook: str):
    
    # determine which intents to analyse
    headers = authorize(username,password)
    response_dict = predict(headers, input, namespace, playbook)
    top3 = get_top_n_intents(3, response_dict)
    print("")
    if 'entityMatches' in response_dict.keys():
        print(json.dumps(response_dict['entityMatches'],indent=2))
    for intent in top3:
        print(f'{extract_score(response_dict, intent)} {intent}')
    print("")
    # print(json.dumps(response_dict["matches"],indent=2))
    # print(json.dumps(response_dict["hierMatches"],indent=2))



def get_top_n_intents(comparison: int, response_dict: dict) -> list:
    '''Find the top n intents from the prediciton to do word weight analysis on'''
    intents = []
    for i in range(comparison):
        if i < len(response_dict['matches']):
            intents.append(response_dict['matches'][i]['name'])
        else:
            break
    return intents
  

def extract_score(response_dict: dict, target_intent: str) -> int:
    '''Extract the score for a certain intent from the lowest match level from predict json'''
    if 'matches' in response_dict.keys():
        for match in response_dict['matches']:
            if match['name'] == target_intent:
                score = round(match['score'],2)
                return score
    return 0

def authorize(username: str, password: str) -> dict:
    '''Get bearer token for a username and password'''
     
    # print(f'Hello {username} getting auth token details')

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