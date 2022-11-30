#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python simple_json_unlabelled.py -f <your input file relative path>
#
# *****************************************************************************

# standard imports
import pandas
import numpy
import click
import humanfirst
from datetime import datetime
from dateutil import parser
from typing import Union

@click.command()
@click.option('-f','--filename',type=str,required=True,help='Input File')
def main(filename: str):

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
    df = pandas.read_csv(filename,encoding='utf8',dtype=dtypes)

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
    filename_out = filename.replace('.csv','.json')
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