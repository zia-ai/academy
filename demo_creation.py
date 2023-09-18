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
import json

# third party imports
import click

# Custom imports
import humanfirst_apis

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-v', '--verbose',is_flag=True,default=False,help='Increase logging level')
def main(input: str, username: str, password: int, namespace: bool, playbook: str, bearertoken: str, verbose: bool):

    # do authorisation
    headers = humanfirst_apis.process_auth(bearertoken=bearertoken,username=username,password=password)
    
    # create a blank workspace - raw JSON - ignore python helpers as not in python
    workspace_skeleton = blank_
    {
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
        # importa
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
        "color" ""# str, optional  a hex code starting with # for a color to display the tag in eg #ff33da (a bright pink)
    }
    
    context_skeleton = {
        "context_id": "", # unique id for context object - i.e the document id or similar to link
        "type": "conversation", #always conversation for now if present.  New document types coming
        "role": "" # client or expert
    }
    
    doc1 = {
        "filename": "CFO-2023-09-18 18:04:00",
        "author": "Gary McGibbons (Intern)",
        "type": "transcript",
        "text": """
                This was a very boring year, nothing happened.  
                I become some bored and concerned that I started sleeping in meetings.
                I can't imagine anything interesting happened at all.
                Apart from the CEO's affair with his secretary
                And us loosing $50k at Aintree on the company away day.
                Other than that very dull
                """
    }
    print(doc1)
    quit()
    
    doc2 = """
    I'm really worried about the finances since the disaster at the company away today.
    People's conduct was highly in appropriate.
    I don't know what we're going to do to stabilise the balance sheet
    I'm expre
    """
    
    
    # entities not done currently
    
    # so l


if __name__ == '__main__':
    main()