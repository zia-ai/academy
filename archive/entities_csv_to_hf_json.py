#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python entities_csv_to_hf_json.py
#
# Converts Entities CSV to HF JSON
# CSV format
#     first column          - entity name
#     second column         - key value
#     rest of the columns   - synonyms
#
# This script has been abused between combinations of entities, key values
# and synonyms.  Needs to be rewritten cleanly before being moved out of
# archive
#
# *****************************************************************************

# standard imports
import json
import uuid

# third party imports
import pandas
import click



@click.command()
@click.option('-i', '--input_file', type=str, required=True, help='CSV containing enitites"')
@click.option('-o', '--output_file', type=str, default='./data/entities.json', help='JSON containing enitites in HF format')
@click.option('-h', '--header', is_flag=True, default=False, help='States if the CSV has a header or not')
def main(input_file: str, output_file: str, header: bool):
    process(input_file, output_file, header)


def process(input_file: str, output_file: str, header: bool) -> None:
    ''' Read a csv containing entities and synonyms'''

    df = pandas.read_csv(input_file, delimiter=',', encoding='utf-8', header=None)
    assert isinstance(df, pandas.DataFrame)

    if header:
        df.drop(labels=0, axis=0, inplace=True)

    df.reset_index(drop=True, inplace=True)

    # checking if the CSV is empty or not
    if df.shape[0] == 0 or pandas.isna(df).values.all():
        raise Exception("The CSV is empty")

    # assign headers to CSV
    col = []
    for i in list(df.columns):
        if i == 0:
            col.append("entity_name")
        elif i == 1:
            col.append("key_value")
        else:
            col.append(f"synonym{i-1}")
    df.columns = col

    # remove those rows with empty entity names
    df = df.loc[~df["entity_name"].isna()]

    # get all the entity names
    entity_names = df["entity_name"].unique()

    # strip all the leading and trailing white spaces from entity names and key values
    df[["entity_name", "key_value"]] = df[["entity_name", "key_value"]].apply(strip_leading_and_trailing_white_spaces, axis=1)

    workspace = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "entities": []
    }
    assert isinstance(workspace,dict)

    print(f"Total Number of entities: {len(entity_names)}")

    # make entit(ies)
    entities = {}
    for entity_name in df["entity_name"].unique():
        print(f'Create: {entity_name}')
        entities[entity_name] = make_entity(entity_name=entity_name)

    # make a key value for every line appending to entities
    df["key_value"] = df.apply(make_key_value,args = [entities], axis=1)

    entities = list(entities.values())
    workspace["entities"] = entities

    with open(output_file, mode="w", encoding="utf8") as fileobj:
        json.dump(workspace, fileobj, indent=3)

    print(f"JSON file is stored at {output_file}")

def make_entity(entity_name: str) -> dict:
    entity = {
            "id": entity_name,
            "name": entity_name,
            "values": []
    }
    return entity

def make_key_value(row: pandas.DataFrame, entities: dict) -> dict:
    """Create a raw HF Entity in json"""
    key_value = {
        "id": f'keyvalue-{uuid.uuid4()}',
        "key_value": row["key_value"],
        "synonyms": []
    }
    potential_synonyms = list(set(row[2:]))
    synonyms = []
    for s in potential_synonyms:
        if not pandas.isna(s):
            synonyms.append({"value":s})
    key_value["synonyms"]= synonyms
    entities[row["entity_name"]]["values"].append(key_value.copy())

    return key_value

def strip_leading_and_trailing_white_spaces(row: pandas.Series) -> pandas.Series:
    '''Removes white spaces from start and end of the strings from entity_name and key_values'''
    row.entity_name = row.entity_name.strip()
    if not pandas.isna(row.key_value):
        row.key_value = row.key_value.strip()
    return row


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
