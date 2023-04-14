#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python entities_csv_to_hf_json.py
#
# Converts Entities CSV to HF JSON
# CSV first column          - entity name
#     second column         - key value
#     rest of the columns   - synonyms
#
# *****************************************************************************

# standard imports
import json

# third party imports
import pandas
import click
import uuid

@click.command()
@click.option('-i','--input',type=str,required=True,help='CSV containing enitites"')
@click.option('-o','--output',type=str,default='./data/entities.json',help='JSON containing enitites in HF format')
@click.option('-h','--header',is_flag=True,default=False,help='States if the CSV has a header or not')
def main(input: str, output: str, header: bool):
    process(input, output, header)

def process(input: str, output: str, header: bool) -> None:
    ''' Read a csv containing entities and synonyms'''
    
    df = pandas.read_csv(input,delimiter=',',encoding='utf-8',header=None)
    assert isinstance(df,pandas.DataFrame)

    if header:
        df.drop(labels=0,axis=0,inplace=True)
    
    df.reset_index(drop=True,inplace=True)

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
    df[["entity_name","key_value"]] = df[["entity_name","key_value"]].apply(strip_leading_and_trailing_white_spaces,axis=1)
    df = df.set_index (["entity_name","key_value"])

    df.sort_index(inplace=True)

    entities = {"$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
              "entities": []}
    
    print(f"Total Number of entities: {len(entity_names)}")

    for entity_name in entity_names:
        key_values = []
        # for every key_value for a entity
        for value in df.loc[entity_name].iterrows():
            # value should not be empty cell
            if not pandas.isna(value[0]):
                key_value = {
                    "id": f"entval-{uuid.uuid4()}",
                    "key_value": value[0],
                    "synonyms": []
                }
                synonyms = set({value[0],value[0].lower()})

                # this ensures if the csv has more than one same value under a single entity, 
                # then only one value is chosen and all the synonyms under all the values added to the single chosen value.
                # for example 
                #   entity1 value1 synonym1 synonym2
                #           value1 synonym3 synonym4
                #           value1 synonym4
                # This would become
                # entity1 value1 - synonym1 synonym2 synonym3 synonym4
                # only unique synonyms are taken into consideration after stripping down the leading and trailing white spaces
                for row in df.loc[entity_name,value[0]].iterrows(): 
                    for synonym in row[1]:
                        if not pandas.isna(synonym):
                            synonyms.add(synonym.strip())
                
                # add all the synonyms to values
                for synonym in synonyms:
                    key_value["synonyms"].append({
                        "value": synonym
                    })
                key_values.append(key_value)

        entities["entities"].append({
            "id":f"entname-{uuid.uuid4()}",
            "name": entity_name,
            "values": key_values
        })

        print(f"{entity_name} has {len(key_values)} values")

    with open(output, mode="w", encoding="utf8") as fileobj:
        json.dump(entities, fileobj ,indent=3)

    print(f"JSON file is stored at {output}")

def strip_leading_and_trailing_white_spaces(row:pandas.Series) -> pandas.Series:
    '''Removes white spaces from start and end of the strings from entity_name and key_values'''

    row.entity_name = row.entity_name.strip()
    if not pandas.isna(row.key_value):
        row.key_value = row.key_value.strip()
    return row

if __name__ == '__main__':
    main()