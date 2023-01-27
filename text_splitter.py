#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python text_splitter.py 
# -filepath <unlabelled hf json> 
# --split <text_to_split> 
# --output <output_path> 
# --timestamp <created_at_time> 
# --key_id <unique_id>
# 
# text_to_split,created_at_time,key_id - in case of nested object start the   
#                                        naming from root with the delimeter as 
#                                        "-"
# *****************************************************************************

# standard imports
import random
import json
import click
from os import listdir
from os.path import isfile, join
import uuid
import re
from datetime import datetime,timedelta,date
from dateutil import parser

# third Party imports
import pandas
import nltk
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

# custom imports
import humanfirst

@click.command()
@click.option('-i','--key_id',type=str,required=True,help='Key ID field name')
@click.option('-t','--timestamp',type=str,required=True,help='Timestamp field name')
@click.option('-f','--filepath',type=str,required=True,help='Directory containing utterances as HF json')
@click.option('-o','--output',type=str,required=True,help='Filepath where the HF file should be produced')
@click.option('-s','--split', type=str,required=True,help='Text field name that requires splitting')

def main(filepath: str, split: str, output: str, key_id: str, timestamp: str) -> None:
    """Main Function"""

    with open(filepath, mode="r", encoding="utf-8") as utterance_file:
        utterances = json.load(utterance_file)
    
    df = pandas.json_normalize(utterances["examples"],sep="-")
    # print(df)

    df["created_at"] = df[timestamp].apply(convert_timestamps)

    # split the utterances into logical units
    pt = nltk.tokenize.PunktSentenceTokenizer()
    df["split_text"] = df[split].apply(split_utterance,args=[pt])
    df[f"metadata-before_splitting_{split}"] = df[split].copy()

    df = df.explode(["split_text"],ignore_index=True)
    # print(df["split_text"])

    df["utterance_id"] = df[key_id].copy()
    df["idx"] = df.groupby([key_id]).cumcount()
    df[["metadata-is_first_sentence","metadata-sentence_num"]] = df["idx"].apply(set_first_utterance_and_seq)
    print(df[["idx","metadata-is_first_sentence","metadata-sentence_num","split_text","text"]])

    # Extract metadata keys and store the corresponding items in metadata column in dataframe
    metadata_keys_to_extract = []
    for i in df.columns.tolist():
        j = re.findall("^metadata-",i)
        if j:
            metadata_keys_to_extract.append(i)

    df["metadata"] = df.apply(create_metadata, args= [metadata_keys_to_extract],axis=1)
  
    df = df.set_index(["utterance_id","idx"])
    print(f"Number of rows after splitting text is {df.shape[0]}")

    # build examples
    df = df.apply(build_examples,axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()
    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    file_out = open(output,mode='w',encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"{output} is successfully created")

def set_first_utterance_and_seq(index: int) -> list:
    """Decides if a sentence is a first sentence or not and sets the sentence number"""

    row = pandas.Series(data = [True if index == 0 else False, index+1],
                        index = ["metadata-is_first_sentence","metadata-sentence_num"])
    return row

def convert_timestamps(datestring:str) -> datetime:
    """Convert datestring to isoformat"""

    datestring = re.sub(" UTC$","",datestring)
    return parser.parse(datestring).isoformat()

def split_utterance(text: str, pt) -> list:
    """Split the utterances into logical units"""

    list_sentences = pt.tokenize(text)
    return list_sentences

def create_metadata(row: pandas.Series, metadata_keys_to_extract: list) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''
    
    metadata = {} # metadata is a simple dict object 
    for key in metadata_keys_to_extract:
            if isinstance(row[key],list):
                # ensures empty cells are not added to metadata
                # this prevents the conflict that arises due to the presence of properties with similar semantics
                if not pandas.isna(row[key]).any(): 
                    metadata[re.sub("^metadata-","",key)] = ','.join(row[key])
            else:
                # ensures empty cells are not added to metadata
                # this prevents the conflict that arises due to the presence of properties with similar semantics
                if not pandas.isna(row[key]):
                    metadata[re.sub("^metadata-","",key)] = str(row[key])

    # all key value pairs must be strings
    for key in metadata.keys():
        try:
            assert(isinstance(metadata[key],str))
        except Exception:
            print(f'Key: {key} value {metadata[key]} is not a string')

    return metadata

def build_examples(row: pandas.Series) -> pandas.Series:
    '''Build the examples'''

    # build examples
    example = humanfirst.HFExample(
        id=f"example-{row.name[0]}-{row.name[1]}",
        text=row['split_text'],
        created_at=row["created_at"],
        intents=[], # no intents as unlabelled
        tags=[], # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        # this links the individual utterances into their conversation
        context=humanfirst.HFContext(
            str(row.name[0]), # any ID can be used recommend a hash of the text which is repeatable or the external conversation id if there is one.
            'conversation', # the type of document
            "client" # the speakers role in the conversations
        )
    )
    row['example'] = example
    return row

if __name__ == '__main__':
    main()