#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python filename_as_phrase_level_tags.py -f <input file> -o <output file>
#
# *****************************************************************************

# standard imports
import pandas
from datetime import datetime
from datetime import timedelta  
from typing import Union
from uuid import uuid4
from pathlib import Path  

# 3rd party imports
import click

# custom imports
import humanfirst
@click.command()
@click.option('-f','--filename',type=str,required=True,help='Input File')
@click.option('-o','--output_filename',type=str,default='',help='Output File')
def main(filename: str,output_filename: str) -> None:
    '''Main function'''

    # read the input csv
    df = pandas.read_csv(filename,encoding='utf8')

    df["id"] = df.apply(assign_uuid,axis=1)

    # set time
    df["created_at"] = datetime.now()
    df["created_at"] = df.apply(assign_sequential_time,axis=1)

    # create metadata object per utterance
    metadata_keys_to_extract = list(df.columns)
    metadata_keys_to_extract.remove("utterance")
    metadata_keys_to_extract.remove("id")
    metadata_keys_to_extract.remove("created_at")
    df["metadata"] = df.apply(create_metadata,args=[metadata_keys_to_extract],axis=1)

    # create tags
    tag_name = f"{Path(filename).name.split('.csv')[0]}"
    df["tags"] = df.apply(create_tags,args=[tag_name],axis=1)

    # index by conversation and utterance
    df = df.set_index(['id'],drop=True)
    print(df)

    # build examples
    df = df.apply(build_examples,axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    if output_filename == '':
        output_filename = filename.replace('.csv','.json')
    file_out = open(output_filename,mode='w',encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"File is saved at {output_filename}")

def assign_uuid(row: pandas.Series) -> str:
    '''Generates UUID'''

    return uuid4()

def assign_sequential_time(row: pandas.Series) -> str:
    '''Makes timestamp incremental'''

    return (row["created_at"] + timedelta(seconds=row.name)).isoformat()

def create_tags(row: pandas.Series,tag_name: str) -> dict:
    '''Create Tags'''

    tag = {
        "id": f"tag-{row['id']}",
        "name": tag_name
        }
    return tag

def build_examples(row: pandas.Series) -> pandas.Series:
    '''Build the examples'''

    # build examples
    example = humanfirst.HFExample(
        text=row['utterance'],
        id=f'example-{row.name}',
        created_at=row["created_at"],
        intents=[], # no intents as unlabelled
        tags=[row["tags"]], # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        # this links the individual utterances into their conversation
    )
    row['example'] = example
    return row
    
def create_metadata(row: Union[pandas.Series, dict], metadata_keys_to_extract: list) -> pandas.Series:
    '''Build the HF metadata object for the pandas line using the column names passed'''
    
    metadata = {} # metadata is a simple dict object 
    for key in metadata_keys_to_extract:
        metadata[key] = row[key]
    return metadata

if __name__ == '__main__':
    main()