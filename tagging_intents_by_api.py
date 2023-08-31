#!/usr/bin/env python  # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# tagging intents
#
# *****************************************************************************

# standard imports
import json

# third part imports
import pandas
import click

# custom imports
import humanfirst_apis


@click.command()
@click.option('-f', '--filename', type=str, default='./examples/example_tagging_list_ccai_bau.csv',
              help='Two column utf8 csv')
@click.option('-i', '--intent_col', type=str, default='intent_name',
              help='Name of intent_name column - default intent_name')
@click.option('-t', '--tag_col', type=str, default='tag_name',
              help='Name of intent_name column - default tag_name')
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True,
              help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True,
              help='HumanFirst playbook id')
def main(filename: str,
         intent_col: str, tag_col: str,
         username: str, password: str,
         namespace: str, playbook: str):
    '''Main function'''

    # columns using.
    print(f'Columns to be read are intent_col:{intent_col} tag_col:{tag_col}')

    # accept a csv of intent names and tags based on flat Ex03 csv
    df = pandas.read_csv(filename, usecols=[intent_col, tag_col])
    print(df)

    # work out distinct tags from sheet
    tag_names = list(df[tag_col].unique())
    print(f'distinct tag_names:{tag_names}')

    # auth
    headers = humanfirst_apis.process_auth(
        username=username, password=password)

    # check what tags exist
    all_tags = humanfirst_apis.get_tags(headers, namespace, playbook)
    if all_tags is None:
        all_tags = []
    print(all_tags)

    # check for those tags
    tag_index = {}
    for tag_name in tag_names:
        found = False
        for tag in all_tags:
            if tag_name == tag["name"]:
                found = True
                print(f"Tag {tag_name} found")
                tag_index[tag_name] = tag
                break
        if not found:
            print(f"Tag {tag_name} not found - please create manually")
            # not sure whether this api is exposed - note method not allowed, field mask etc?
            # humanfirst_apis.create_tag(headers,namespace,playbook,tag_id=f'tag-{tag_name}',
            # name=tag_name,description='',color=humanfirst.generate_random_color())

    print("Tag_index")
    print(json.dumps(tag_index, indent=2))
    print("/n")

    # then download each intent by name
    intent_names = list(df[intent_col].unique())

    # get all intents
    all_intents = humanfirst_apis.get_intents(headers, namespace, playbook)

    # make intent_index
    intent_index = {}
    for intent_name in intent_names:
        found = False
        for intent in all_intents:
            if intent_name == intent["name"]:
                found = True
                print(f"Intent {intent_name} found")
                intent_index[intent_name] = intent
                break
        if not found:
            print(f"Intent {intent_name} not found in workspace")

    # then update them with their tag
    for i, row in df.iterrows():

        # get intent
        print(f'{i} {row[intent_col]} begin:')
        intent = intent_index[row[intent_col]]
        assert isinstance(intent, dict)

        # check if tags field exists if not create it
        if not "tags" in intent.keys():
            print(
                f'- No current tags creating tags field for intent: {intent["name"]}')
            intent["tags"] = []

        # see if the tag we are trying to add is already there
        found = False
        for existing_tag in intent["tags"]:
            print(f'- Already has len(tags):{len(intent["tags"])}')
            if existing_tag["name"] == row[tag_col]:
                print(f'- Intent {row[intent_col]} already has {row[tag_col]}')
                found = True
                break

        # if it is not try and add it.
        if not found:
            print(
                f'- Intent: {row[intent_col]} does not have tag:{row[tag_col]}')
            additional_tag = {
                'id': tag_index[row[tag_col]]['id'],
                'name': tag_index[row[tag_col]]['name']
            }
            intent["tags"].append(additional_tag)
            print(intent)
            humanfirst_apis.update_intent(
                headers, namespace, playbook, intent)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
