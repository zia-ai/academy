#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ******************************************************************************************************************120
#
# Examples of how to the call the HumanFirst APIs
#
# *********************************************************************************************************************

# standard imports
import json
import base64
import datetime

# third party imports
import requests

# constants
TIMEOUT = 5

# ******************************************************************************************************************120
#
# Exceptions
#
# *********************************************************************************************************************
class HFAPIResponseValidationException(Exception):
    """When response validation fails"""

    def __init__(self, url: str, response, payload: dict = None):
        if payload is None:
            payload = {}
        self.url = url
        self.response = response
        self.payload = payload
        self.message = f'Did not receive 200 from url: {url} {self.response.status_code} {self.response.text}'
        super().__init__(self.message)

class HFAPIParameterException(Exception):
    """When parameter validation fails"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

class HFAPIAuthException(Exception):
    """When authorization validation fails"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


# ******************************************************************************************************************120
# Internal Functions
# *********************************************************************************************************************

def _validate_response(response, url: str, field: str = None, payload: dict = None):
    """Validate the response from the API and provide consistent aerror handling"""
    if payload is None:
        payload = {}
    if isinstance(response, str) or response.status_code != 200:
        print(payload)
        raise HFAPIResponseValidationException(
            url=url, payload=payload, response=response)

    candidate = response.json()
    if candidate:
        if field and field in candidate.keys():
            return candidate[field]
        else:
            return candidate
    else:
        return None

# ******************************************************************************************************************120
# Tags
# ********************************************************************************************************************

def get_tags(headers: str, namespace: str, playbook: str) -> dict:
    '''Returns tags'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/tags'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "tags")

def delete_tag(headers: str, namespace: str, playbook: str, tag_id: str) -> dict:
    '''Returns tags'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "tag_id": tag_id
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/tags/{tag_id}'
    response = requests.request(
        "DELETE", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)

def create_tag(headers: str, namespace: str, playbook: str, tag_id: str,
               name: str, description: str, color: str) -> dict:
    '''Create a tag - untested - not sure possible'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "tag_id": tag_id
    }

    now = datetime.datetime.now()
    now = now.isoformat()
    payload = {
        "id": tag_id,
        "name": name,
        "description": description,
        "color": color,  # '#' + ''.join([random.choice('0123456789ABCDEF')
        "created_at": now,
        "updated_at": now
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/tags/{tag_id}'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)

# ******************************************************************************************************************120
# Playbooks/Workspaces
# ********************************************************************************************************************


def list_playbooks(headers: str, namespace: str = "") -> dict:
    '''Returns list of all playbooks for an organisation
    Note namepsace parameter doesn't appear to provide filtering'''
    payload = {
        "namespace": namespace
    }

    url = 'https://api.humanfirst.ai/v1alpha1/playbooks'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "playbooks")

def get_playbook_info(headers: str, namespace: str, playbook: str) -> dict:
    '''Returns metadata of playbook'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/playbooks/{namespace}/{playbook}'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)

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
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    response = _validate_response(response, url, "data")
    response = base64.b64decode(response)
    response = response.decode('utf-8')
    response_dict = json.loads(response)
    return response_dict

# ******************************************************************************************************************120
# Intent retrieval
# ********************************************************************************************************************

def get_intents(headers: str, namespace: str, playbook: str) -> dict:
    '''Get all the intents in a workspace'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "intents")


def get_intent(headers: str, namespace: str, playbook: str, intent_id: str) -> dict:
    '''Get the metdata for the intent needed'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents/{intent_id}'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)


def get_revisions(headers: str, namespace: str, playbook: str,) -> dict:
    '''Get revisions for the namespace and playbook'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/revisions'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "revisions")

def update_intent(headers: str, namespace: str, playbook: str, intent: dict) -> dict:
    '''Update an intent'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "intent": intent,
        "update_mask": "name,id,tags" # doesn't seem to work - confirmed bug to be fixed in next release ~ 2023-09
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)


# ******************************************************************************************************************120
# Call NLU engines
# ********************************************************************************************************************

def get_models(headers: str, namespace: str) -> dict:
    '''Get available models for a namespace
    NOTE: THIS IS NOT nlu-id!'''
    payload = {}

    url = 'https://api.humanfirst.ai/v1alpha1/models'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    models = _validate_response(response, url, "models")
    namespace_models = []
    for model in models:
        print(model)
        if model["namespace"] == namespace:
            namespace_models.append(model)
    return namespace_models


def get_nlu_engines(headers: str, namespace: str, playbook: str) -> dict:
    '''Get nlu engines for the for the namespace and playbook'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/playbooks/{namespace}/{playbook}/nlu_engines'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "nluEngines")


def get_nlu_engine(headers: str, namespace: str, playbook: str, nlu_id: str) -> dict:
    '''Get nlu engine for the for the namespace and playbook'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "nlu_id": nlu_id
    }

    url = f'https://api.humanfirst.ai/v1alpha1/playbooks/{namespace}/{playbook}/nlu_engines/{nlu_id}'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)

def predict(headers: str, sentence: str, namespace: str, playbook: str,
            model_id: str = None, revision_id: str = None) -> dict:
    '''Get response_dict of matches and hier matches for an input
    optionally specify which model and revision ID you want the prediction from
    model_id probably better know as nlu-id
    revision_id probably better known as run_id 
    but it needs to be the run_id of the model job not revisions which is showing export job
    TODO: update when updated'''

    payload = {
        "namespace": "string",
        "playbook_id": "string",
        "input_utterance": sentence
    }

    if model_id or revision_id:
        if not model_id or not revision_id:
            raise HFAPIAuthException(
                "If either specified both model_id and revision_id are required")

    if model_id:
        payload["model_id"] = model_id
    if revision_id:
        payload["revision_id"] = model_id

    url = f'https://api.humanfirst.ai/v1alpha1/nlu/predict/{namespace}/{playbook}'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)


def batchPredict(headers: str, sentences: list, namespace: str, playbook: str) -> dict:  # pylint: disable=invalid-name
    '''Get response_dict of matches and hier matches for a batch of sentences
    TODO: model version changes'''
    print(f'Analysing {len(sentences)} sentences')
    payload = {
        "namespace": "string",
        "playbook_id": "string",
        "input_utterances": sentences
    }

    url = f'https://api.humanfirst.ai/v1alpha1/nlu/predict/{namespace}/{playbook}/batch'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "predictions")

# ******************************************************************************************************************120
# Authorisation
# ********************************************************************************************************************

def get_headers(bearer_token: str) -> dict:
    """Produce the necessary header"""
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

    key = 'AIzaSyA5xZ7WCkI6X1Q2yzWHUrc70OXH5iCp7-c'
    base_url = 'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key='
    auth_url = f'{base_url}{key}'

    headers = get_headers('')

    auth_body = {
        "email": username,
        "password": password,
        "returnSecureToken": True
    }
    auth_response = requests.request(
        "POST", auth_url, headers=headers, data=json.dumps(auth_body), timeout=TIMEOUT)
    if auth_response.status_code != 200:
        raise HFAPIAuthException(
            f'Not authorised, google returned {auth_response.status_code} {auth_response.json()}')
    id_token = auth_response.json()['idToken']
    headers = get_headers(id_token)
    # print('Retrieved id_token and added to headers')
    return headers

def process_auth(bearertoken: str = '', username: str = '', password: str = '') -> dict:
    '''Validate which authorisation method using and return the headers'''

    if bearertoken == '':
        for arg in ['username', 'password']:
            if arg == '':
                raise HFAPIAuthException(
                    'If bearer token not provided, must provide username and password')
        return authorize(username, password)
    else:
        return get_headers(bearertoken)

# ******************************************************************************************************************120
# Conversation sets and Querying Processed Conversation set data
# *********************************************************************************************************************

def get_conversion_set_list(headers: str, namespace: str) -> tuple:
    """Conversation set list"""

    payload = {}
    url = f"https://api.humanfirst.ai/v1alpha1/conversation_sets?namespace={namespace}"
    response = requests.request(
        "GET", url, headers=headers, data=payload, timeout=TIMEOUT)
    if response.status_code != 200:
        print(f"Got {response.status_code} Response\n URL - {url}")
        quit()
    conversation_sets = response.json()['conversationSets']
    conversation_set_list = []
    for conversation_set in conversation_sets:
        conversation_set_id = conversation_set['id']

        url = f"https://api.humanfirst.ai/v1alpha1/conversation_sets/{namespace}/{conversation_set_id}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, timeout=TIMEOUT)
        if response.status_code != 200:
            print(f"Got {response.status_code} Responsen\n URL - {url}")
            quit()
        conversation_set = response.json()

        if "state" in conversation_set.keys():
            conversation_set["no_data_file_is_uploaded_since_creation"] = False
            if (("jobsStatus" in conversation_set["state"].keys()) and
                    ("jobs" in conversation_set["state"]["jobsStatus"].keys())):
                jobs_dict = {}
                jobs = conversation_set["state"]["jobsStatus"]["jobs"]
                range_end = range(len(jobs))
                for i in range_end:
                    if jobs[i]["name"] in ["merged", "filtered", "indexed", "embedded"]:
                        jobs_dict[jobs[i]["name"]] = jobs[i]
                        del jobs_dict[jobs[i]["name"]]["name"]
                conversation_set["is_datafolder_empty"] = False
                conversation_set["state"]["jobsStatus"]["jobs"] = jobs_dict
            else:
                conversation_set["is_datafolder_empty"] = True
        else:
            conversation_set["is_datafolder_empty"] = True
            conversation_set["no_data_file_is_uploaded_since_creation"] = True
        conversation_set_list.append(conversation_set)

    return conversation_set_list

def query_conversation_set(
        headers: str,
        namespace: str,
        workspace: str,
        search_text: str = "",
        start_isodate: str = "",
        end_isodate: str = "",
        page_size: int = 10,
        convsetsource: str = "",
        next_page_token: str = "") -> dict:
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
    # if next_page_token and next_page_token != "":
    #     predicates.append({"PageTokenData":{"PageToken":next_page_token}})

    if len(predicates) == 0:
        raise HFAPIParameterException(
            "Must have either text or start and end date predicates." +
            f"search_text: {search_text} start_isodate: {start_isodate} end_isodate: {end_isodate}")

    payload = {
        "predicates": predicates,
        "pageSize": page_size
    }
    if next_page_token and next_page_token != "":
        payload["page_token"] = next_page_token

    url = f'https://api.humanfirst.ai/v1alpha1/conversations/{namespace}/{workspace}/query'
    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)


# ******************************************************************************************************************120
# Integrations
# *********************************************************************************************************************

def get_integrations(headers: str, namespace: str):
    '''Returns all the integrations configured for a namespace'''
    payload = {
        "namespace": namespace
    }

    url = f'https://api.humanfirst.ai/v1alpha1/integrations/{namespace}'

    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "integrations")

def get_integration_workspaces(headers: str, namespace: str, integration_id: str):
    '''Get the integration workspaces for an integration
    i.e call the integration in HF to detect in the integrated NLU
    what target/source workspaces there are.
    i.e in DF case find out what agents there are to import data from'''
    payload = {
        "namespace": namespace,
        "integration_id":integration_id
    }

    url = f'https://api.humanfirst.ai/v1alpha1/integration_workspaces/{namespace}/{integration_id}/workspaces'

    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "workspaces")

def trigger_import_from_df_cx_integration(
        headers: str,
        namespace: str,
        playbook: str,
        integration_id: str,
        integration_workspace_id: str,
        project: str,
        region: str,
        integration_language: str,
        bidirectional_merge: bool = False,
        hierarchical_intent_name_disabled: bool = True,
        hierarchical_delimiter: str = '--',
        zip_encoding: bool = False,
        gzip_encoding: bool = False,
        hierarchical_follow_up: bool = True,
        include_negative_phrases: bool = False,
        skip_empty_intents: bool = True,
        clear_intents: bool = False,
        clear_entities: bool = False,
        clear_tags: bool = False,
        merge_intents: bool = False,
        merge_entities: bool = False,
        merge_tags: bool = False,
        extra_intent_tags: list = None,
        extra_phrase_tags: list = None
    ):
    '''Triggers import of the wrokspace from the selected integration'''
    if extra_intent_tags is None:
        extra_intent_tags = []
    if extra_phrase_tags is None:
        extra_phrase_tags = []

    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "integration_id": integration_id,
        "integration_workspace_id": integration_workspace_id,
        "integration_location": {
            "project": project,
            "region": region
        },
        "bidirectional_merge": bidirectional_merge,
        "intent_options": {
            "hierarchical_intent_name_disabled": hierarchical_intent_name_disabled,
            "hierarchical_delimiter": hierarchical_delimiter,
            "zip_encoding": zip_encoding,
            "gzip_encoding": gzip_encoding,
            "hierarchical_follow_up": hierarchical_follow_up,
            "include_negative_phrases": include_negative_phrases,
            "skip_empty_intents": skip_empty_intents
        },
        "import_options": {
            "clear_intents": clear_intents,
            "clear_entities": clear_entities,
            "clear_tags": clear_tags,
            "merge_intents": merge_intents,
            "merge_entities": merge_entities,
            "merge_tags": merge_tags,
            "extra_intent_tags": extra_intent_tags,
            "extra_phrase_tags": extra_phrase_tags
        },
        "integration_language": integration_language
    }

    url = f'https://api.humanfirst.ai/v1alpha1/integration_workspaces/{namespace}/{integration_id}/workspaces/{integration_workspace_id}/import' # pylint: disable=line-too-long

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload),timeout=TIMEOUT)
    return _validate_response(response, url)


# ******************************************************************************************************************120
# Evaluations
# *********************************************************************************************************************

def get_evaluation_presets(headers: str, namespace: str, playbook: str):
    '''Get the presets to find the evaluation_preset_id to run an evaluation'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/playbooks/{namespace}/{playbook}/presets'

    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url, "presets")

def trigger_preset_evaluation(headers: str,
                       namespace: str,
                       playbook: str,
                       evaluation_preset_id: str,
                       name: str = ''):
    '''Start an evaluation based on a preset'''
    if name == '':
        name = f'API triggered: {datetime.datetime.now()}'
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "params": {
            "evaluation_preset_id": evaluation_preset_id
        },
        "name": name
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/evaluations'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)

def get_evaluation_zip(headers: str, namespace: str, playbook: str, evaluation_id: str) -> dict:
    '''Get the metdata for the intent needed'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/evaluations/{evaluation_id}/report.zip'
    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)

    return response

# ******************************************************************************************************************120
# Subscriptions
# *********************************************************************************************************************

def get_plan(headers: str):
    '''Get the plan for a subscription'''
    payload = {}

    url = 'https://api.humanfirst.ai/v1alpha1/subscriptions/plan'

    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)

def get_usage(headers: str):
    '''Get the usage for a subscription'''
    payload = {}

    url = 'https://api.humanfirst.ai/v1alpha1/subscriptions/usage'

    response = requests.request(
        "GET", url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    return _validate_response(response, url)
