#!/usr/bin/env python
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

# third party imports
import click

# Custom imports
import humanfirst_apis

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-v', '--verbose',is_flag=True,default=False,help='Increase logging level')
def main(username: str, password: int, namespace: bool, bearertoken: str, verbose: bool):
    if verbose:
        print('Verbose mode on')


    # do authorisation
    headers = humanfirst_apis.process_auth(bearertoken=bearertoken,username=username,password=password)
    
    # create skeletons for the objects in humanfirst.py as full JSON for readibility/replication outside of python
    workspace_skeleton = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": [],
        "tags": [],
        "intents": [],
        "entities": []
    }
    
    example_skeleton = {
        "id": "",
        "text": "", # the text of the example
        "context": {}, #  HFContext, optional  A HFContext object defining what document type the example came from
        "intents": [], # intent to add a label goes here.
        "tags": [], # utterance level tags
        "metadata": {}
    }
    
    intent_skeleton = {
        "id": "", # id is what actually links examples to intents
        "name": "", # duplicates are allowed
        "metadata": {}, # single level key value pairs all data as strings
        "tags": [], # a list of HF tag objects
        "parent_intent_id": None # optional link to parent id
    }
    
    tag_skeleton = {
        "id": "", # unique id for tag
        "name": "",# name of tag that will be displayed in HF studio
        "color":"" # str, optional  a hex code starting with # for a color to display the tag in eg #ff33da (a bright pink)
    }
    
    context_skeleton = {
        "context_id": "", # unique id for context object - i.e the document id or similar to link
        "type": "conversation", #always conversation for now if present.  New document types coming
        "role": "" # client or expert
    }
    
    # some example docs
    doc1 = {"filename": "CMO-2023-09-18 18:04:00",
            "author": "Gary McGibbons (Intern)",
            "interviewee": "Sarah Tribbins (CMO)",
            "type": "transcript",
            "text": """
                    This was a very boring year, nothing happened.  
                    I become some bored and concerned that I started sleeping in meetings.
                    I can't imagine anything interesting happened at all.
                    Apart from the CEO's affair with his secretary
                    And us loosing $50k at Aintree on the company away day.
                    Other than that very dull
                    """}
    
    doc2 = {"filename": "CFO-2023-10-19 10:45:04",
            "author": "Gary McGibbons (Intern)",
            "interviewee": "Harvey Happenstance (CFO)",
            "type": "transcript",
            "text": """
                    I'm really worried about the finances since the disaster at the company away today.
                    People's conduct was highly in appropriate.
                    I don't know what we're going to do to stabilise the balance sheet
                    I'm on the express train to Bristol in the morning
                    I'm going to try and find a friendly banker.
                    """}

    doc3 = {"filename": "CFO-2023-09-19 12:04:00",
            "author": "Gary McGibbons (Intern)",
            "interviewee": "Lisa Lords (CEO)",
            "type": "transcript",
            "text": """
                    Everything is glorious.
                    Literally nothing could be better.
                    I'm ecstatic about out new product line, it is so nearly ready for market
                    I've seen the books and they are looking great, we'll be in the black soon.
                    I don't think there are any issues.
                    """}
    
    docs = [doc1,doc2,doc3]
    
    # some example training phrases being used to boot strap the model
    example_training_intent_1 = {
        "label": "financial_concerns",
        "examples": [
            "I'm concerned about the financial situation",
            "I'm worried about the balance sheet",
            "I don't understand where the money has gone"
        ]
    }

    example_training_intent_2 = {
        "label": "travel_plans",
        "examples": [
            "I'm booking a holiday",
            "I'm going to flee the country",
            "I need to get away"
        ]
    }

    example_training_intent_3 = {
        "label": "impropriety",
        "examples": [
            "My boss can't keep his hands off me",
            "There was this issue at the staff party",
            "Did you hear about Dave and Janet?"
        ]
    }
    
    bootstrap_data_from_tool = [example_training_intent_1,example_training_intent_2,example_training_intent_3]
    
    # create the workspace - skipping past temp
    # print(humanfirst_apis.post_playbook(headers, namespace))
    
    # temp create this
    playbook = "playbook-52UTQ4UGJJD7BINYXF2PV2LW"
    
    # create our intent to update
    intent = intent_skeleton.copy()
    intent["name"] = example_training_intent_1["label"]
    intent["id"] = f'intent-id-{example_training_intent_1["label"]}'
    
    # push our first intent data into it.
    print(humanfirst_apis.update_intent(headers, namespace, playbook, intent))
    
    

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter