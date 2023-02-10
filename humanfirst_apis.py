#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# example api imports
#
# *****************************************************************************

# standard imports
import requests
import json
import base64
import humanfirst
import datetime


def validate_response(response, url: str, field: str = None):
    if isinstance(response,str):
        raise Exception(response)
    if response.status_code != 200:
        raise Exception(
            f'Did not receive 200 from url: {url} {response.status_code} {response.text}')
    candidate = response.json()
    if candidate:
        if field and field in candidate.keys():
            return candidate[field]
        else:
            return candidate
    else:
        return None

def query_conversation_set(
        headers: str,
        namespace: str,
        workspace: str,
        search_text: str = "",
        start_isodate: str = "",
        end_isodate: str = "",
        page_size: int = 10,
        convsetsource: str = "",
        nextPageToken: str = "") -> dict:
    '''Do a search and return the big data with predicates'''
    predicates = []
    if search_text and search_text != '':
        predicates.append({"inputMatch": {"text": search_text}})
    if start_isodate and end_isodate and start_isodate != '' and end_isodate != '':
        predicates.append(
            {
                "timeRange": {
                    "start": start_isodate,
                    "end": end_isodate
                }
            }
        )
    if convsetsource and convsetsource != "":
        predicates.append(
            {"conversationSet": {"conversationSetIds": [convsetsource]}})
    # if nextPageToken and nextPageToken != "":
    #     predicates.append({"PageTokenData":{"PageToken":nextPageToken}})

    if len(predicates) == 0:
        raise Exception(
            f"Must have either text or start and end date predicates.  search_text: {search_text} start_isodate: {start_isodate} end_isodate: {end_isodate}")

    payload = {
        "predicates": predicates,
        "pageSize": page_size
    }
    if nextPageToken and nextPageToken != "":
        payload["page_token"] = nextPageToken

    url = f'https://api.humanfirst.ai/v1alpha1/conversations/{namespace}/{workspace}/query'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    return validate_response(response, url)


def get_tags(headers: str, namespace: str, playbook: str) -> dict:
    '''Returns tags'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/tags'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload))
    return validate_response(response, url, "tags")


def delete_tag(headers: str, namespace: str, playbook: str, tag_id: str) -> dict:
    '''Returns tags'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "tag_id": tag_id
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/tags/{tag_id}'
    response = requests.request(
        "DELETE", url, headers=headers, data=json.dumps(payload))
    return validate_response(response, url)


def create_tag(headers: str, namespace: str, playbook: str, tag_id: str, name: str, description: str, color: str) -> dict:
    '''Returns tags'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "tag_id": tag_id
    }

    now = datetime.datetime.now()
    now = now.isoformat()
    tag = {
        "id": f'tag-{humanfirst.hash_string(name)}',
        "name": name,
        "description": description,
        "color": color,  # '#' + ''.join([random.choice('0123456789ABCDEF')
        "created_at": now,
        "updated_at": now
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/tags/{tag_id}'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    return validate_response(response, url)


def get_playbook_info(headers: str, namespace: str, playbook: str) -> dict:
    '''Returns metadata of playbook'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/playbooks/{namespace}/{playbook}'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload))
    return validate_response(response, url)


def get_playbook(headers: str,
                 namespace: str,
                 playbook: str,
                 hierarchical_delimiter="-",
                 hierarchical_intent_name_disabled: bool = True,
                 zip_encoding: bool = False,
                 hierarchical_follow_up: bool = True,
                 include_negative_phrases: bool = False
                 ) -> dict:
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
            "hierarchical_intent_name_disabled": hierarchical_intent_name_disabled,
            "hierarchical_delimiter": hierarchical_delimiter,
            "zip_encoding": zip_encoding,
            "hierarchical_follow_up": hierarchical_follow_up,
            "include_negative_phrases": include_negative_phrases
        }
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents/export'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    response = validate_response(response, url, "data")
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


def process_auth(bearertoken: str = '', username: str = '', password: str = '') -> dict:
    '''Validate which authorisation method using and return the headers'''

    if bearertoken == '':
        for arg in ['username', 'password']:
            if arg == '':
                raise Exception(
                    f'If bearer token not provided, must provide username and password')
        return authorize(username, password)
    else:
        return get_headers(bearertoken)
