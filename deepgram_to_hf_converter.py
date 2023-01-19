#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python deepgram_to_hf_converter.py -f <directory> -o <output_file_path>
#
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

# custom imports
import humanfirst

@click.command()
@click.option('-f','--filedir',type=str,required=True,help='Directory containing Deepgram transcribed files')
@click.option('-o','--output',type=str,required=True,help='Filepath where the HF file should be produced')
def main(filedir: str, output: str) -> None:
    """Main Function"""

    convo_list = []
    for f in listdir(filedir):
        if isfile(join(filedir, f)):
            filepath = join(filedir, f) 
            with open(filepath, mode="r", encoding="utf-8") as convo_file:
                convo = json.load(convo_file)
                convo_list.append(preprocess_convo(convo))

    df = pandas.json_normalize(convo_list,sep="-")
    print(df.columns)

    # explode the below columns
    col_list = ['results-utterances-utterance','results-utterances-start',
                'results-utterances-end', 'results-utterances-avg_confidence',
                'results-utterances-channel']
    df = df.explode(col_list,ignore_index=True)
    print(df[col_list])

    # rename columns for ease of access
    rename_col = {
        'results-utterances-utterance': 'utterance',
        'results-utterances-start': 'start',
        'results-utterances-end': 'end',
        'results-utterances-avg_confidence': 'avg_confidence',
        'metadata-created': 'convo_transcribed_at',
        'metadata-duration': 'convo_duration'
    }
    df = df.rename(columns=rename_col)

    # assign role
    df['role'] = df['results-utterances-channel'].apply(assign_role)

    # Extract metadata keys and store the corresponding items in metadata column in dataframe
    metadata_keys_to_extract = ["start","end","avg_confidence","convo_transcribed_at","convo_duration"]
    df["metadata"] = df.apply(create_metadata, args= [metadata_keys_to_extract],axis=1)

    # add start seconds to the created_at time 
    df["created_at"] = df[["created_at","start"]].apply(add_seconds,axis = 1)
    df = df.sort_values(["conversation_id","created_at"]).reset_index(drop=True)
    df["idx"] = df.groupby(["conversation_id"]).cumcount()
    df = df.set_index(["conversation_id","idx"])

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
    file_out = open(output,mode='w',encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"{output} is successfully created")

def preprocess_convo(convo: dict) -> dict:
    """Modify the struture of the data in order to get the entire utterance and its corresponding metadata"""

    count = 0

    # get random year,month,day
    rand_year,rand_month,rand_day = get_rand_date()
    convo["created_at"] = datetime(rand_year,rand_month,rand_day,0,0,0).isoformat()

    # get values of each property and put that in a list
    # example - "start": [1,2,3,4,....], "end": [7,8,9,.....], ...
    utterances = convo["results"]["utterances"]
    u_dict = {}
    for key in utterances[0].keys():
        u_dict[key] = []
    for utterance in utterances:
        for key in u_dict:
            u_dict[key].append(utterance[key])
        count = count + 1
    
    # merging the sentences that belong to a soingle utternace and storing their corresponding metadata
    i = 0
    convo_dict = {
        "utterance": [],
        "start": [],
        "end": [],
        "avg_confidence": [],
        "channel": []
    }
    start_utterance,end_utterance,transcript,prev_channel,confidence,no_of_parts = assign_initial_utterance_values(u_dict,i)
    i = i+1
    # Example - In a list of channel number 0,0,1,1,0,0,0,1 has 4 utterances
    # first 2 0's belong to a single utterance, next two 1's belong to a single utterance, 
    # next 3 0's belong to a utterance and final 1 belong to a utterance
    while(i < count):
        current_channel = u_dict["channel"][i]
        while (prev_channel == current_channel):
            end_utterance = u_dict["end"][i]
            transcript = transcript + " " + u_dict["transcript"][i]
            confidence = confidence + u_dict["confidence"][i]
            no_of_parts = no_of_parts + 1
            prev_channel = current_channel
            i = i+1
            if (i < count):
                current_channel = u_dict["channel"][i]
            else:
                break
        convo_dict = append_final_utterance_values( convo_dict,transcript, start_utterance, 
                                                    end_utterance,confidence, no_of_parts, prev_channel)
        if (i < count):
            start_utterance,end_utterance,transcript,prev_channel,confidence,no_of_parts = assign_initial_utterance_values(u_dict,i)
        else:
            break
        i = i+1
    else:
        convo_dict = append_final_utterance_values( convo_dict,transcript, start_utterance, 
                                                    end_utterance, confidence, no_of_parts, prev_channel)
    convo["results"]["utterances"] = convo_dict

    # creating a conversation id for a conversation
    convo["conversation_id"] = f"convo-{uuid.uuid4()}"
    return convo

def append_final_utterance_values(convo_dict: dict, transcript, start_utterance, end_utterance, confidence, no_of_parts, prev_channel) -> dict:
    """Helper function for preprocess_convo - appends the final utterance and its corresponding metadata"""

    convo_dict["utterance"].append(transcript)
    convo_dict["start"].append(start_utterance)
    convo_dict["end"].append(end_utterance)
    convo_dict["avg_confidence"].append(round((confidence / no_of_parts),4))
    convo_dict["channel"].append(prev_channel)
    return convo_dict

def assign_initial_utterance_values(u_dict: dict, i: int) -> dict:
    """Helper function for preprocess_convo - assign inital values for a utterance"""

    start_utterance = u_dict["start"][i]
    end_utterance = u_dict["end"][i]
    transcript = u_dict["transcript"][i]
    prev_channel = u_dict["channel"][i]
    confidence = u_dict["confidence"][i]
    no_of_parts = 1
    return start_utterance,end_utterance,transcript,prev_channel,confidence,no_of_parts

def get_rand_date() -> tuple:
    """Generates random date from the year 2022"""

    year_now,month_now,day_now = str(date.today()).split("-")
    rand_year = random.randint(2022, int(year_now))
    rand_month = random.randint(1, int(month_now))
    rand_day = random.randint(1, int(day_now))
    return rand_year,rand_month,rand_day

def add_seconds(row: pandas.Series) -> str:
    '''Add seconds to the created_at property'''

    new_date = (parser.parse(row["created_at"]) + timedelta(seconds=row["start"])).isoformat()
    return new_date

def build_examples(row: pandas.Series) -> pandas.Series:
    '''Build the examples'''

    example_id = re.sub("^convo-","",row.name[0])
    # build examples
    example = humanfirst.HFExample(
        id=f"example-{example_id}-{row.name[1]}",
        text=row['utterance'],
        created_at=row['created_at'],
        intents=[], # no intents as unlabelled
        tags=[], # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        # this links the individual utterances into their conversation
        context=humanfirst.HFContext(
            str(row.name[0]), # any ID can be used recommend a hash of the text which is repeatable or the external conversation id if there is one.
            'conversation', # the type of document
            row["role"] # the speakers role in the conversations
        )
    )
    row['example'] = example
    return row

def create_metadata(row: pandas.Series, metadata_keys_to_extract: list) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''
    
    metadata = {} # metadata is a simple dict object 
    for key in metadata_keys_to_extract:
            if isinstance(row[key],list):
                # ensures empty cells are not added to metadata
                # this prevents the conflict that arises due to the presence of properties with similar semantics
                if not pandas.isna(row[key]).any(): 
                    metadata[key] = ','.join(row[key])
            else:
                # ensures empty cells are not added to metadata
                # this prevents the conflict that arises due to the presence of properties with similar semantics
                if not pandas.isna(row[key]):
                    metadata[key] = str(row[key])

    # all key value pairs must be strings
    for key in metadata.keys():
        try:
            assert(isinstance(metadata[key],str))
        except Exception:
            print(f'Key: {key} value {metadata[key]} is not a string')

    return metadata

def assign_role(row: pandas.Series) -> str:
    """Assign role depending on channel number 0-client/1-expert"""

    if row == 0:
        return "client"
    else:
        return "expert"

if __name__ == '__main__':
    main()