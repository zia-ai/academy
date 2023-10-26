#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python demo_creation.py
# -u <username>
# -p $HF_PASSWORD
# -n <namepspace>
# -b <playbook-id>
#
# demonstration of creating a workspace and intents incrementally using APIs
#
# *****************************************************************************

# standard imports
import json
import os
import time

# third party imports
import click
import pandas
import nltk

# Custom imports
import humanfirst_apis


@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-w', '--wait_to_train', type=int, default=10, help='How long for NLU to train')
@click.option('-m', '--min_match_score', type=float, default=0.4, help='Minimum threshold to consider a match')
@click.option('-v', '--verbose', is_flag=True, default=False, help='Increase logging level')
@click.option('-d', '--dummy', is_flag=True, default=False, help='Dummy run - don\'t create humanfirst objects')
@click.option('-b', '--playbook', type=str, default='', help='If present will skip creation and try and load into this')
@click.option('-o', '--output_directory', type=str, default="./data", help='Where to write files')
def main(username: str,
         password: int,
         namespace: bool,
         bearertoken: str,
         wait_to_train: int,
         min_match_score: float,
         verbose: bool = False,
         dummy: bool = False,
         playbook: str = '',
         output_directory: str = ''):
    """Main"""
    if verbose:
        print('Verbose mode on')
    if dummy:
        print('This is a dummy run no objects will be created')

    # here are some examples that the user has inputted
    user_bootstrap_examples = get_user_examples()

    # create a workspace structure for the intents and examples
    workspace = get_workspace_skeleton(name="demo_creation_2",description="Should be showing new name")

    # if you have any tags, they would go here so that intents and examples can reference them

    # otherwise start by creating the intents
    workspace["intents"] = create_hf_intents(user_bootstrap_examples)

    # then create the examples referencing the intents
    workspace["examples"] = create_hf_examples(user_bootstrap_examples)

    # at this point this should validate using vscode or another json schema validating IDE/programme/website
    # you can also test pushing it directly out into a json and uploading to HF
    # must be UTF-8
    workspace_file_uri = os.path.join(output_directory,"bootstrap_training.json")
    with open(workspace_file_uri,mode="w",encoding="utf8") as bootstrap_file:
        json.dump(workspace,fp=bootstrap_file,indent=2)
    print(f'Created example workspace to upload at: {workspace_file_uri}')

    if not dummy:
        # authorisation
        print('Authorising')
        headers = humanfirst_apis.process_auth(bearertoken=bearertoken, username=username, password=password)

        if playbook == '':
            # create the workspace/playbook
            # calling this the returned playbook to differentiate it from workspace.
            # this is the object with the ids created.
            playbook = humanfirst_apis.post_playbook(headers, namespace, "name not yet working")
            playbook_id = playbook["metastorePlaybook"]["id"]
            print(f'Created playbook: {playbook_id}')
        else:
            playbook_id = playbook
            print(f'Using passed playbook: {playbook_id}')

        # update the workspace with the training
        print('Importing workspace into playbook:')
        print(humanfirst_apis.import_intents(headers,namespace,playbook_id,workspace_as_dict=workspace))

        # get the NLU enginess for the workspace
        nlu_engines = humanfirst_apis.get_nlu_engines(headers,namespace,playbook_id)

        # in this case ther is only going to be one (as we haven't created any others)
        # and that is going to be humanfirst engine, so assume it's in the first position
        # This is a tautism, you could get from the nlu_engines call the same info.
        nlu_engine = humanfirst_apis.get_nlu_engine(headers, namespace, playbook_id, nlu_engines[0]["id"])
        print('NLU engine to train:')
        print(nlu_engine)

        # Trigger this - doesn't have a very meaningful response, None, or {} here, but with a code 200
        humanfirst_apis.trigger_train_nlu(headers,namespace,playbook_id,nlu_engine["id"])
        print("Triggered training on NLU engine")

    # Get the docs to classify
    docs = get_docs()

    # sentence split them
    docs = sentencize_docs(docs)
    print("Generated some test data")

    # wait for model to train - later on example checking NLU ready
    print("Starting wait for model to train")
    if not dummy:
        time.sleep(wait_to_train)
    print("Wait complete")

    if not dummy:
        # for each doc do a batch predict on (or for all docs)
        for doc in docs:

            # print some metaddata of the doc
            print(f'\nAnalysis of {doc["type"]} "{doc["filename"]}"')
            print(f'Interviewer: {doc["author"]} interviewing {doc["interviewee"]}')

            # get the predictions from huamnfirst
            predictions = humanfirst_apis.batchPredict(headers,doc["text"],namespace,playbook_id)

            # loop through them printing out the text sentence with the score
            for i,match in enumerate(predictions):
                match_name = ''
                match_confidence = match["matches"][0]["score"]
                if match_confidence >= min_match_score:
                    match_name = match["matches"][0]["name"]
                print(f'{doc["text"][i]:80} {match_name:>20}:{match_confidence:.2f},')

    # Show changing one thing in workspace
    if not dummy:
        print(json.dumps(workspace,indent=2))
        workspace["examples"][0]["text"] = "I changed only the first example"
        print(humanfirst_apis.import_intents(headers,namespace,playbook_id,workspace_as_dict=workspace,clear_intents=True))

def list_workspaces(dummy: bool, headers, namespace) -> pandas.DataFrame:
    """Returns a list of workspace ids and names checking connection is correct"""
    if not dummy:
        df_all_workspaces = pandas.json_normalize(
            humanfirst_apis.list_playbooks(headers, namespace))
        return df_all_workspaces[df_all_workspaces["namespace"] == "humanfirst-academy"]["name"]

def get_user_examples() -> list:
    """some example training phrases being used to boot strap the model

    Note: need 2+ intents, minimum five examples
    """
    user_bootstrap_examples = [
        {
            "label": "financial_concerns",
            "examples": [
                "I'm concerned about the financial situation",
                "I'm worried about the balance sheet, and the company finances in general",
                "I don't understand where the money has gone",
                "The fiscal situation is difficult, we're in an unstable situation",
                "We're going cash flow negative",
                "I don't think we have enough capital funding"
            ]
        },
        {
            "label": "travel_plans",
            "examples": [
                "I'm booking a holiday",
                "I'm going to flee the country",
                "I need to get away",
                "I've got a ticket on the next flight to the Bahamas",
                "Did you know that Algeria has no extradiction treaty with the UK",
                "I'm going somewhere warm and sunny"
            ]
        },
        {
            "label": "impropriety",
            "examples": [
                "My boss can't keep his hands off me",
                "There was this issue at the staff party",
                "Did you hear about Dave and Janet?",
                "All the donations for Alfred's leaving do when missing",
                "I'm sure someone is pilfering the petty cash",
                "Craig is very handsy"
            ]
        },
        {
            "label": "positivity",
            "examples": [
                "It's brilliant",
                "Things are going so well",
                "Can you believe how great our results are",
                "I love it here",
                "Fantastic",
                "It is great"
            ]
        }
    ]
    return user_bootstrap_examples


def sentencize_docs(docs: list) -> list:
    """sentencize the docs ready for labelling"""
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt')
    punkt_tokenizer = nltk.tokenize.PunktSentenceTokenizer()
    # split the sentences
    for doc in docs:
        doc["text"] = punkt_tokenizer.tokenize(doc["text"])
        # clean up white space how you want - you just need to be consistent with what you store locally
        # with what you ask humanfirst to annotate as separate sentences.
        for i, text in enumerate(doc["text"]):
            assert isinstance(text, str)
            doc["text"][i] = text.strip()
    return docs


def get_workspace_skeleton(name:str, description: str, color: str = "#ff33da") -> dict:
    """create skeletons for the objects in humanfirst.py as full JSON for readibility/replication outside of python"""
    workspace_skeleton = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "name": name,
        "description": description,
        "color": color,
        "examples": [],
        "tags": [],
        "intents": [],
        "entities": []
    }
    return workspace_skeleton

def create_hf_intents(user_bootstrap_examples: list) -> list:
    """Create HF intents"""
    intent_skeleton = {
        "id": "",  # id is what actually links examples to intents
        "name": "",  # duplicates are allowed
        "metadata": {},  # single level key value pairs all data as strings
        "tags": [],  # a list of HF tag objects
        "parent_intent_id": None  # optional link to parent id
    }
    intents = []
    for i, input_example in enumerate(user_bootstrap_examples):
        hf_intent = intent_skeleton.copy()
        # build any id you want must not overlap with others
        hf_intent["id"] = f'intent-id-{i}-{input_example["label"]}'
        hf_intent["name"] = input_example["label"]
        # Parents you must create first, then the children if you want a hierarchy
        # add tags and metadata at the intent level as you wish.

        # remove the parent_intent_id for json validation if not required
        del hf_intent["parent_intent_id"]
        intents.append(hf_intent)
    return intents


def create_hf_examples(user_bootstrap_examples: list) -> list:
    """create hf examples"""
    example_skeleton = {
        "id": "",
        "text": "",  # the text of the example
        "context": {},  # HFContext, optional  A HFContext object defining what document type the example came from
        "intents": [],  # intent to add a label goes here.
        "tags": [],  # utterance level tags
        "metadata": {}
    }
    examples = []
    for i, input_example in enumerate(user_bootstrap_examples):
        for j, text in enumerate(input_example["examples"]):
            hf_example = example_skeleton.copy()
            # build any id you want must not overlap with others
            hf_example["id"] = f'example-id-{i}-{input_example["label"]}-{j}'
            hf_example["text"] = text  # The text
            # no context here as these are single examples, not necessarily connected to a doc.
            hf_example["context"] = {}
            # we want to link it to the id of the intent to train
            # it is a list but generally only 1 intent should be present depending on your nlu.
            # we provide python sdk which indexes the ids in objects for you.
            # here we are linking just by using the same format of id as we used in create_hf_intents
            hf_example["intents"] = [
                {
                    'intent_id': f'intent-id-{i}-{input_example["label"]}'
                }
            ]
            # add tags and metadata as wish
            examples.append(hf_example)
    return examples


def get_tag_skeleton() -> dict:
    """Return a tag skeleton"""
    tag_skeleton = {
        "id": "",  # unique id for tag
        "name": "",  # name of tag that will be displayed in HF studio
        # str, optional  a hex code starting with # for a color to display the tag in eg #ff33da (a bright pink)
        "color": ""
    }
    return tag_skeleton.copy()


def get_context_skeleton() -> dict:
    """Return a skeleton for a context object linking examples together into a document"""
    context_skeleton = {
        "context_id": "",  # unique id for context object - i.e the document id or similar to link
        # always conversation for now if present.  New document types coming
        "type": "conversation",
        "role": ""  # client or expert
    }
    return context_skeleton.copy()


def get_docs() -> list:
    """some example docs to annotate"""
    docs = [
        {"filename": "CMO-2023-09-18 18:04:00",
            "author": "Gary McGibbons (Intern)",
            "interviewee": "Sarah Tribbins (CMO)",
            "type": "transcript",
            "text": """
                    This was a very boring year, nothing happened.
                    I become some bored and concerned that I started sleeping in meetings.
                    I can't imagine anything interesting happened at all.
                    Apart from the CEO's affair with his secretary.
                    And us loosing $50k at Aintree on the company away day.
                    Other than that very dull.
                    """},
        {"filename": "CFO-2023-10-19 10:45:04",
            "author": "Gary McGibbons (Intern)",
            "interviewee": "Harvey Happenstance (CFO)",
            "type": "transcript",
            "text": """
                    I'm really worried about the finances since the disaster at the company away today.
                    People's conduct was highly in appropriate.
                    I don't know what we're going to do to stabilise the balance sheet.
                    I'm on the express train to Bristol in the morning.
                    I'm going to try and find a friendly banker.
                    """},
        {"filename": "CFO-2023-09-19 12:04:00",
            "author": "Gary McGibbons (Intern)",
            "interviewee": "Lisa Lords (CEO)",
            "type": "transcript",
            "text": """
                    Everything is glorious.
                    Literally nothing could be better.
                    I'm ecstatic about out new product line, it is so nearly ready for market.
                    I've seen the books and they are looking great, we'll be in the black soon.
                    I don't think there are any issues.
                    """}
    ]
    return docs

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
