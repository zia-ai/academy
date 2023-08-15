#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python simple_json_unlabelled_c.py -f <your input file relative path>
#
# *****************************************************************************

# standard imports
import re
from datetime import datetime
from datetime import timedelta  
from dateutil import parser
from typing import Union

# third party
import pandas
import numpy
import click

# custom imports
import humanfirst

@click.command()
@click.option('-f','--filename',type=str,required=True,help='Input File')
@click.option('-v','--voice',is_flag=True,default=False,help='Whether voice file or not')
@click.option('-c','--cleanse',is_flag=True,default=False,help='Whether to clean start and end of utterance for <speak><speak/> and whitespace and quotes')
@click.option('-h','--head',type=int,default=0,help='Number to sample from head of file')
def main(filename: str, cleanse: bool, voice: bool, head: int):
   
    # this file looks like 
    dtypes = {
        'Scenario ID': str,
        'Lead Id': str,
        'Question': str,
        'Prompt': str,
        'Response': str,
        'Intent Detected': str,
        'Success': str, # Y/N
        'Length In Mil Sec': str,
        'Created At': str,
        'Call Log Id': str,
        'Scenario Name': str,
        'Channel': str
    }

    # read the input csv
    df = pandas.read_csv(filename,encoding='utf8',dtype=dtypes)
    
    if head > 0:
        df = df.head(head)
        
    
    # create an id field
    if voice:
        df['external_id'] = df['Lead Id'] + '-' + df['Call Log Id']
    else:
        df['external_id'] = df['Lead Id']
    df['Response'] = df['Response'].fillna('')
    
    # flush blank Response lines
    print(f'before blank removal: {df.shape}')
    df = df[df['Response']!='']
    print(f'after  blank removal: {df.shape}')
   
    # setup the expert data as separate dataframe
    df_prompt = df.copy(deep=True)
    df_prompt['created_at'] = df_prompt['Created At'].apply(convert_timestamps, args=[0])
    df_prompt['utterance'] = df_prompt['Prompt']
    df_prompt['role'] = 'expert'
    
    # setup the client data from original
    df['created_at'] = df['Created At'].apply(convert_timestamps, args=[1]) # treat it as one microsecond after the prompt
    df['utterance'] = df['Response']
    df['role'] = 'client'
    
    # join on the prompt data
    df = pandas.concat([df,df_prompt])
    print(f'after concat        : {df.shape}')
    df = df.sort_values(['external_id','created_at'],ignore_index=True)
    df = df.reset_index(drop=True)
    
    # clean up utterances
    if cleanse: 
        df['utterance'] = df['utterance'].apply(cleanse_prompts)
           
    # index the speakers
    df['idx'] = df.groupby(["external_id"]).cumcount()
    df['idx_max'] = df.groupby(["external_id"])['idx'].transform(numpy.max)
    
    # This info lets you filter for the first or last thing the client says - this is very useful in boot strapping bot design
    df['idx_client'] = df.groupby(['external_id', 'role']).cumcount().where(df.role == 'client', 0)
    df['first_customer_utt'] = df['idx_client'] == 0
    df['second_customer_utt'] = df['idx_client'] == 1
    df['idx_client_max'] = df.groupby(["external_id"])['idx_client'].transform(numpy.max)
    df['final_customer_utt'] = df['idx_client'] == df['idx_client_max']
      
    # create metadata object per utterance
    metadata_keys_to_extract = ['Scenario ID','Lead Id','Question','Prompt','Response','Intent Detected',
                                'Success','Length In Mil Sec','Created At','Call Log Id','Scenario Name','Channel']
    metadata_keys_to_extract.extend(['idx','first_customer_utt','second_customer_utt','final_customer_utt']) # generated custom indexing fields
    dict_of_file_level_values = { 'loaded_date': datetime.now().isoformat()} # if there is anythign you think relevant
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
    print(df[['role','utterance']])
    print(f'Wrote to {filename_out}')
    file_out.close()
    
def cleanse_prompts(utterance: str) -> str:
    clean_utterance = re.sub(r'^["\s]*(<speak>)*[\s\n]*',"",utterance)
    clean_utterance = re.sub(r'[\s\n\"]*(<\/speak>)*[\"\s\n]*$',"",clean_utterance)
    return clean_utterance

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

def convert_timestamps(datestring:str, add_microseconds: int) -> datetime:
    return parser.parse(datestring) + timedelta(microseconds=add_microseconds)

if __name__ == '__main__':
    main()