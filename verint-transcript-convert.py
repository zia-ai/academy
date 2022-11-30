#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# single file mode
# python verint-transcript-convert.py -d './data' -f 2022111505165619506742122126350401.json
#
# directory mode
# python verint-transcript-convert.py -d './data'
#
# Optional
# --threshold to split utterances based on silence - default 150 (ms)
# --skip      through warning and skip bad files
# --maxms     discard utterances starting after this threshold (default infinity == 0)
# 
# *****************************************************************************

# standard imports
import os
from io import TextIOWrapper
import json
from dateutil import parser
import copy

# third party imports
import pandas
import click
import datetime


# custom imports
import common


@click.command()
@click.option('-d', '--directory', type=str, required=True, help='Directory for input and output - will be searched for subfolders of json')
@click.option('-f', '--file',type=str,required=False,default='',help='Run just on a single file')
@click.option('-t', '--threshold',type=int,default=150,help='Word gap threshold ms')
@click.option('-k', '--skip',is_flag=True,default=False,help='Log and continue if a single file failes')
@click.option('-m', '--maxms',type=int,default=0,help='Maximum number of miliseconds of call to process')
def main(directory: str, file: str, threshold: int, skip: bool, maxms: int):


    # single file mode, products
    if file != '':
        print(f'Single file mode')
        directory, file_uri = validate_args(directory, file)
        json_obj = read_json(file_uri)
        df = process(json_obj,threshold,maxms)
        write_output(df['example'].to_list(),f'{directory}{file.replace(".json","-hf.json")}')
    # multi file mode, assumes directory, has sub directories (by day for example), each with many json files
    # produces a single json per sub directory
    else:
        dir_name_list = get_directory_list(directory)
        for dir in dir_name_list:    
            print(f'Processing directory {dir}')
            file_name_list = get_file_list(dir)
            df = pandas.DataFrame()
            for file in file_name_list:
                try:
                    directory, file_uri = validate_args(dir, file)
                    json_obj = read_json(file_uri)
                    df_temp = process(json_obj,threshold,maxms)
                except Exception as e:
                    print(e)
                    print(f'WARNING Failed on file: {file}')
                    if skip:
                        print(f'Skip mode on - continuing')
                    else:
                        quit()
                if df.empty:
                    df = df_temp
                else:
                    # batch all the files within a subfolder for easier uploading
                    df = pandas.concat([df,df_temp],axis=0)
            file_out = f'{dir}-hf.json'
            file_out = file_out.replace(' ','-')
            write_output(df['example'].to_list(),file_out)

def write_output(examples: list, output_file_uri: str):
        '''Build a workspace from the examples and write it to file'''
        # write output 
        unlabelled_workspace = common.HFWorkspace()
        for example in examples:
            unlabelled_workspace.add_example(example)
        
        file_out = open(output_file_uri, 'w', encoding='utf8')
        # use the hf commandline tool to automate uploading to HF as will create many files
        unlabelled_workspace.write_json(file_out)
        file_out.close()
        print(f'Wrote output to: {output_file_uri}')


def process(json_obj:dict, threshold: int, maxms: int):
    '''Takes a full json_object from a verint file, and then creates a DF of metadata
    and examples ready to put into a HF workspace
    you can import this file and call this directly
    assumes there is one call per file
    
    - json_obj:   verint json object as a dict
    - threshold:  gap in ms between words that triggers new passage within a speakers passage
    - maxms:      ignore utterances starting after this (0 treated as infinity)

    '''
    top_level_metadata_fieldss =  ['language','mediaId','contentType'] # add whatever fields you want from top level here
    metadata_subobj_fields = ['AUDIO_START_TIME','Agent_name'] # add whatever fields you want here

    # build metadata 
    metadata = {}
    metadata = build_metadata(metadata,top_level_metadata_fieldss,json_obj)
    metadata = build_metadata(metadata,metadata_subobj_fields,json_obj['metadata'])
    
    # better word level - assembling own passages with a configurable silence threshold to split speaker segments.
    if len(json_obj["transcript_detailed"]["words"]) <= 1:
        return pandas.DataFrame()
    prev = {
        "p":0,
        "s":0,
        "e":0,
        "w":"",
        "sp":""
    }
    words = json_obj["transcript_detailed"]["words"]
    utterances = []
    u = {
        "g":0,
        "s":0,
        "e":0,
        "utterance": "",
        "sp":""
    }
    gap = 0
    for i, word_obj in enumerate(words):
        begin = False
        gap = words[i]["s"] - prev["e"]
        if prev["sp"] != words[i]["sp"]:
            gap = 0
            begin = True
        if gap > threshold:
            begin = True
        if begin:
            if u["s"] > 0:
                utterances.append(u)
            u = {
                "g": gap,
                "s":words[i]["s"],
                "e":words[i]["e"],
                "utterance": words[i]["w"],
                "sp": words[i]["sp"]
            }
            begin = True
        else:
            u["utterance"] = u["utterance"] + " " + words[i]["w"]
            u["e"] = words[i]["e"]

        words[i]["b"] = begin
        words[i]["g"] = gap
        prev = word_obj

    df = pandas.json_normalize(utterances)
    df = df.rename(columns={'g':'gap','s':'start_ms','e':'end_ms','sp':'verint_role'})

    # verint to hf role mapping
    role_mapping = {
        'Customer': 'client',
        'Agent':    'expert',
    }
    df['role'] = df['verint_role'].apply(map_roles,args=[role_mapping])

    # created_at
    convo_start =  parser.parse(metadata['AUDIO_START_TIME'])
    df['created_at'] = df['start_ms'].apply(created_at,args=[convo_start])

    # index the speakers by speaker
    df['idx'] = df.index
    df['idx_max'] = df.index.max()
    df['idx_client'] = df.groupby(['role']).cumcount().where(df.role == 'client', 0)
    df['idx_client_max'] = df['idx_client'].max()
    df['idx_expert'] = df.groupby(['role']).cumcount().where(df.role == 'expert', 0)
    df['idx_expert_max'] = df['idx_expert'].max()

    df = df.apply(index_utt_flags,axis=1)
    
    # if only care about first section of call
    if maxms > 0:
        df = df[df["start_ms"]<=maxms]

    # build examples
    df = df.apply(build_example,args=[metadata],axis=1)

    return df

def get_directory_list(directory: str) -> list:
    '''Get a list of sub directories'''
    if not directory.endswith('/'):
        directory = directory + '/'
    if not os.path.isdir(directory):
        raise Exception (f'Can\'t read directory: {directory}')

    all_file_names = os.listdir(directory)
    dir_name_list = []
    for dir_name in all_file_names:
        rel_path = f'{directory}{dir_name}'
        if os.path.isdir(rel_path):
            dir_name_list.append(rel_path)
    return dir_name_list  

def get_file_list(directory: str) -> list:#
    '''Get a list of json file from directory that are probably verint files'''
    if not directory.endswith('/'):
        directory = directory + '/'
    if not os.path.isdir(directory):
        raise Exception (f'Can\'t read directory: {directory}')
    all_file_names = os.listdir(directory)
    file_name_list = []
    for file_name in all_file_names:
        if file_name.endswith('-hf.json'):
            continue
        if file_name.endswith('.json'):
            file_name_list.append(file_name)
    return file_name_list  

def index_utt_flags(row: pandas.Series) -> pandas.Series:
    '''Build flags for first, second, final client/expert
    This allows filtering to look at just the start or end of the conversation'''

    # create fallback/initial part metadata
    row["final_utt"] = False
    if row['idx'] == row['idx_max']:
        row["final_utt"] = True

    # client
    row["first_client_utt"] = False
    row["second_client_utt"] = False
    row["final_client_utt"] = False
    if row["role"] == "client":
        if row['idx_client'] == 0:
            row["first_client_utt"] = True
        if row['idx_client'] == 1:
            row["second_client_utt"] = True 
        if row['idx_client'] == row['idx_client_max']:
            row["final_client_utt"] = True

    # expert
    row["first_expert_utt"] = False
    row["second_expert_utt"] = False
    row["final_expert_utt"] = False
    if row["role"] == "expert":
        if row['idx_expert'] == 0:
            row["first_expert_utt"] = True
        if row['idx_expert'] == 1:
            row["second_expert_utt"] = True
        if row['idx_expert'] == row['idx_expert_max']:
            row["final_expert_utt"] = True
    
    return row    
    
def created_at(start_ms: int, convo_start: datetime) -> datetime:
    '''Calculate an utterance start time based on a delta from start of conversation'''
    return convo_start + datetime.timedelta(milliseconds=start_ms)

def build_example(row: pandas.Series, metadata: dict) -> pandas.Series:
    '''Creates a HumanFirst unlabelled utterance example linking it to it's conversation and adding metadata'''

    key_fields = ['gap','start_ms','end_ms','idx','final_utt',\
        'first_client_utt','second_client_utt','final_client_utt',\
        'first_expert_utt','second_expert_utt','final_expert_utt']
    row_metadata = copy.deepcopy(build_metadata(metadata,key_fields,row))

    example = common.HFExample(
        text=row['utterance'],
        id=f'{row_metadata["mediaId"]}-{row["start_ms"]}',
        created_at=row['created_at'],
        intents=[], # no intents as unlabelled
        tags=[], # no tags only metadata on unlabelled
        metadata=row_metadata,
        context=common.HFContext(str(row_metadata['mediaId']),'conversation',row['role'])
    )
    row['example'] = example
    return row

def map_roles(role_in: str, role_mapping: dict) -> str:
    '''Converts verint roles to hf roles'''
    try:
        return role_mapping[role_in]
    except KeyError:
        raise Exception(f'Couldn\'t locate role: "{role_in}" in role mapping {",".join(role_mapping.keys())}')

def build_metadata(metadata: dict, field_list: list, source_obj: dict) -> dict:
    '''Builds metadata from key lists out of objects'''
    for key in field_list:
        try:
            metadata[key] = str(source_obj[key])
        except Exception as e:
            print(e)
            print(f'Can\'t find {key} in {",".join(source_obj.keys())}')
            quit()
    return metadata

def validate_args(directory: str, file: str):
    ''' Validate that directory and file are valid'''
    if not directory.endswith('/'):
        directory = directory + '/'
    if not os.path.isdir(directory):
        raise Exception (f'Can\'t read directory: {directory}')
    file_uri = f'{directory}{file}'
    if not os.path.isfile(file_uri):
        raise Exception (f'Can\'t read directory/file {file_uri}')
    return directory, file_uri

def read_json(file_uri: str) -> dict:
    '''Read the json'''
    file = open(file_uri,encoding='utf8',mode='r')
    assert(isinstance(file,TextIOWrapper))
    contents = file.read()
    file.close()
    contents = json.loads(contents)
    assert(isinstance(contents,dict))
    return contents

if __name__ == '__main__':
    main()


