#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python simple_json_unlabelled.py -f <your input file relative path>
#
# *****************************************************************************

# standard imports
import os
import json
from datetime import datetime
from dateutil import parser
from typing import Union


# third part imports
import pandas
import numpy
import click

# custom imports
import humanfirst
import humanfirst_apis

@click.command()
@click.option('-d','--directory',type=str,required=True,help='Directory containing your unzipped agent zip')
# @click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
# @click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
# @click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
# @click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
# @click.option('-v', '--verbose', is_flag=True, default=False, help='Increase logging level')
@click.option('-f', '--filename', type=str, required=True, help='Workspace file to update')
def main(directory: str, filename: str): # username: str, password: str, namespace: str, playbook: str, verbose: bool):
    
    config = validate_agent_directory(directory)
    intents = load_intents(config["intent_dir"])
    df = pandas.json_normalize(intents)
    df = process_priorities(df)
    df = process_intent_types(df)
    
    labelled_workspace = get_workspace(config,filename)
    intent_name_index = labelled_workspace.get_intent_index(delimiter="-")
    df["intent_id"] = df["name"].apply(map_values,args=[intent_name_index])  
    df.apply(add_priority_tags,args=[labelled_workspace],axis=1)
    df.apply(add_intent_type_tags,args=[labelled_workspace],axis=1)
    write_workspace(config, filename, labelled_workspace)
    
def add_priority_tags(row:pandas.Series, labelled_workspace: humanfirst.HFWorkspace):
    labelled_workspace.tag_intent(row["intent_id"],labelled_workspace.tag(row["priority"]))

def add_intent_type_tags(row:pandas.Series, labelled_workspace: humanfirst.HFWorkspace):
    labelled_workspace.tag_intent(row["intent_id"],labelled_workspace.tag(row["intent_type"]))
    

def get_workspace(config: dict, filename: str) -> humanfirst.HFWorkspace:
    file_uri = f'{config["directory_dir"]}{filename}'
    file = open(file_uri, mode="r",encoding="utf8")
    labelled_workspace = humanfirst.HFWorkspace.from_json(file)
    file.close()
    return labelled_workspace

def write_workspace(config: dict, filename: str, labelled_workspace: humanfirst.HFWorkspace) -> humanfirst.HFWorkspace:
    file_uri = f'{config["directory_dir"]}{filename}'
    file_uri = file_uri.replace("in.json","out.json")
    file = open(file_uri, mode="w",encoding="utf8")
    labelled_workspace.write_json(file)
    file.close()
    
def process_intent_types(df: pandas.DataFrame) -> pandas.DataFrame:
    df["intent_type"] = df.apply(classify_intent_type,axis=1)
    return df    

def process_priorities(df: pandas.DataFrame) -> pandas.DataFrame:
    df.rename(columns={"priority":"priority_raw"},inplace=True)
    mapper = get_priority_mapper()
    df["priority"] = df["priority_raw"].apply(map_values,args=[mapper])
    return df
   
def get_priority_mapper():
    return {
        -1:"ignore",
        250000:"low",
        500000:"normal",
        750000:"high",
        1000000:"highest"
    }
    
    
def map_values(key: any, mapper: dict):
    return mapper[key]
    
def classify_intent_type(row: pandas.Series) -> str:
    intent_type = "unknown"
    if pandas.isna(row["parentId"]) and pandas.isna(row["parentId"]):
        if len(row["contexts"]) == 0:
            intent_type = "top_level"
        else:
            intent_type = "context"
    else:
        intent_type = "follow_on"            
    return intent_type
    
        
def load_intents(intent_dir: str):
    """Read all intent definitions (not usersays_en)"""
    intent_filenames = os.listdir(intent_dir)
    intents = []
    for intent_filename in intent_filenames:
        if intent_filename.endswith("_usersays_en.json"):
            continue
        elif intent_filename.endswith(".json"):
            intent_uri = f'{intent_dir}{intent_filename}'
            file = open(intent_uri,mode="r",encoding="utf8")
            intents.append(json.load(file))
            file.close()
        else:
            raise Exception(f"Did not recognise file in directory: {intent_dir} of name: {intent_filename}")
    return intents
        
def validate_agent_directory(directory: str):
    """Directory must contain agent.json, packaged.json and intents and entities directories to be valid"""
    if not directory.endswith("/"):
        directory = directory + "/"
    config = {
        "directory_dir": directory,
        "agent_file": f'{directory}agent.json',
        "package_file": f'{directory}package.json',
        "intent_dir": f'{directory}intents/',
        "entities_dir": f'{directory}entities/'
    }

    for key in config.keys():
        is_file_object(config[key], key.split('_')[1])
        
    return config
    
def is_file_object(uri:str, type: str):
    if type == "file":
        if not os.path.isfile(uri):
            print(f'Not a file: {uri}')
            quit()
    elif type == "dir":
        if not os.path.isdir(uri):
            print(f'Not a dir: {uri}')
            quit()
    else: 
        raise Exception(f"unrecognised type: {type}")
    print(f'Verified {type:4} {uri}')
    
            
            
    
def old():

    # our example input file looks like this
    dtypes={
        'external_id': str,
        'timestamp': str,
        'utterance': str,
        'speaker': str,
        'nlu_detected_intent': str,
        'nlu_confidence': str,
        'overall_call_star_rating': int
    }

    # read the input csv
    df = pandas.read_csv("blah",encoding='utf8',dtype=dtypes)

    # map external NLU to HF roles
    role_mapper = {
        'AGENT':'expert',
        'CUSTOMER':'client' # by default only the client data is analysed 
        # if you have limited diarization quality upload all as
    }
    df['role'] = df['speaker'].apply(translate_roles,args=[role_mapper])
    
    # index the speakers
    df['idx'] = df.groupby(["external_id"]).cumcount()
    df['idx_max'] = df.groupby(["external_id"])['idx'].transform(numpy.max)
    
    # This info lets you filter for the first or last thing the client says - this is very useful in boot strapping bot design
    df['idx_client'] = df.groupby(['external_id', 'role']).cumcount().where(df.role == 'client', 0)
    df['first_customer_utt'] = df['idx_client'] == 0
    df['second_customer_utt'] = df['idx_client'] == 1
    df['idx_client_max'] = df.groupby(["external_id"])['idx_client'].transform(numpy.max)
    df['final_customer_utt'] = df['idx_client'] == df['idx_client_max']

    # convert date_strings to get a created_at timestamp for each utterance (mandatory)
    # if you don't have one will need to generate one
    # must be 1ms difference minimum between utterances to sort propery
    df['created_at'] = df['timestamp'].apply(convert_timestamps)

    # create metadata object per utterance
    metadata_keys_to_extract = ['nlu_detected_intent', 'nlu_confidence'] # example utterance level info
    metadata_keys_to_extract.extend(['overall_call_star_rating']) # example conversation level data from original sheet
    metadata_keys_to_extract.extend(['idx','first_customer_utt','second_customer_utt','final_customer_utt']) # generated custom indexing fields
    dict_of_file_level_values = { 'loaded_date': datetime.now().isoformat(), 'script_name': 'simple_example_script'} # if there is anythign you think relevant
    df['metadata'] = df.apply(create_metadata,args=[metadata_keys_to_extract, dict_of_file_level_values],axis=1)

    # add any tags
    # generally we don't want to use tags on unlabelled data

    # index by conversation and utterance
    df = df.set_index(['external_id','idx'],drop=True)

    # build examples
    df = df.apply(build_examples,axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    filename_out = "blah.json".replace('.csv','.json')
    file_out = open(filename_out,mode='w',encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()

def build_examples(row: pandas.Series):
    '''Build the examples'''

    # build examples
    example = humanfirst.HFExample(
    text=row['utterance'],
        id=f'example-{row.name[0]}-{row.name[1]}',
        created_at=row['created_at'],
        intents=[], # no intents as unlabelled
        tags=[], # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        # this links the individual utterances into their conversation
        context=humanfirst.HFContext(
            str(row.name[0]), # any ID can be used recommend a hash of the text which is repeatable or the external conversation id if there is one.
            'conversation', # the type of document
            row['role'] # the speakers role in the conversations
        )
    )
    row['example'] = example
    return row
    
def create_metadata(row: Union[pandas.Series, dict], metadata_keys_to_extract: list, dict_of_file_level_values: dict= None) -> pandas.Series:
    '''Build the HF metadata object for the pandas line using the column names passed'''
    
    metadata = {} # metadata is a simple dict object 
    if dict_of_file_level_values and len(dict_of_file_level_values.keys()) > 0:
        metadata = metadata
    
    for key in metadata_keys_to_extract:
        if isinstance(row[key],list):
            metadata[key] = ','.join(row[key])
        else:
            metadata[key] = str(row[key])

    # all key value pairs must be strings
    for key in metadata.keys():
        try:
            assert(isinstance(metadata[key],str))
        except Exception:
            print(f'Key: {key} value {metadata[key]} is not a string')

    return metadata

def convert_timestamps(datestring:str) -> datetime:
    return parser.parse(datestring)

def translate_roles(role:str, mapper:dict) -> str:
    '''Translates abcd to hf role mapping'''
    try:
        return mapper[role]
    except KeyError:
        raise Exception(f'Couldn\'t locate role: "{role}" in role mapping')

if __name__ == '__main__':
    main()