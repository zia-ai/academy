#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python cx_convo_to_hf_json.py -f filepath
#
# *****************************************************************************

# standard imports
import pandas
import json
from dateutil import parser
import numpy
import sys
sys.path.insert(1,"/home/ubuntu/source/academy")

# 3rd party imports
import click

# custom imports
import humanfirst

@click.command()
@click.option('-f','--filepath',type=str,required=True,help='Cx conversation json file')
def main(filepath: str):
    """Main Function"""

    # read file
    with open(filepath,mode="r",encoding="utf8") as f:
        data = json.load(f)
    
    df = pandas.json_normalize(data=data,sep="_")

    # Rows with input query has no other information other than input timestamp
    df_input_text_timestamps = df[pandas.isna(df["jsonPayload_queryResult_diagnosticInfo_Session Id"])][["timestamp"]].reset_index(drop=True)
    df_input_text_timestamps =  df_input_text_timestamps.rename(columns={"timestamp":"input_timestamp"})

    df = df[pandas.notna(df["jsonPayload_queryResult_diagnosticInfo_Session Id"])]

    rename = {
        "jsonPayload_queryResult_diagnosticInfo_Session Id": "convo_id",
        "jsonPayload_queryResult_text": "input_text",
        "jsonPayload_queryResult_responseMessages":"response_text",
        "timestamp":"response_timestamp",
        "jsonPayload_queryResult_match_confidence": "match_confidence",                                           
        "jsonPayload_queryResult_intent_name": "fully_qualified_intent_name",
        "jsonPayload_queryResult_intent_displayName": "intent_displayName",
        "jsonPayload_queryResult_match_matchType": "intent_matchType",
        "jsonPayload_queryResult_match_event": "match_event",
        "resource_labels_project_id": "project_id",
        "labels_session_id":"session_id",
        "labels_agent_id":"agent_id",
        "labels_location_id":"location_id"
    }
    df = df.rename(columns=rename).reset_index(drop=True)
    df = pandas.concat([df, df_input_text_timestamps], axis=1)

    df["response_text"] = df["response_text"].apply(get_response_text)
    print(df[["convo_id","input_text","response_text","input_timestamp","response_timestamp"]])
    
    # set the intent display name
    df["intent_displayName"] = df[["intent_displayName",                                           
                                    "intent_matchType",
                                    "match_event"]].apply(set_intent_name,axis=1)
    
    # This info lets you filter for the first or last thing the client says - this is very useful in boot strapping bot design
    df['idx'] = df.groupby(["convo_id"]).cumcount()
    df['first_customer_utt'] = df['idx'] == 0
    df['second_customer_utt'] = df['idx'] == 1
    df['idx_client_max'] = df.groupby(["convo_id"])['idx'].transform(numpy.max)
    df['final_customer_utt'] = df['idx'] == df['idx_client_max']
    
    # created metadata field
    metadata_keys = ["match_confidence",
                    "intent_displayName",
                    "intent_matchType",
                    "project_id",
                    "session_id",
                    "agent_id",
                    "location_id",
                    "first_customer_utt",
                    "second_customer_utt",
                    "final_customer_utt"]
    
    df["metadata"] = df[metadata_keys].apply(create_metadata,axis=1)

    # split the timestamps across multiple responses
    df[["response_text",
        "input_timestamp",
        "response_timestamp"]] = df[["response_text",
                                    "input_timestamp",
                                    "response_timestamp"]].apply(split_timestamps,axis=1)

    # HumanFirst Data Frame
    hf_df_user = df[['input_text',
                     'input_timestamp',
                     'convo_id',
                     'metadata']].copy().apply(humanfirst_format_user,args=["client"],axis=1)

    hf_df_expert = df[['response_text',
                     'response_timestamp',
                     'convo_id',
                     'metadata']].copy().apply(humanfirst_format_expert,args=["expert"],axis=1)
    
    hf_df_expert = hf_df_expert.explode(['created_at','utterance'],ignore_index=True)
    hf_df_expert = hf_df_expert.sort_values(['created_at']).reset_index(drop=True)
    hf_df = pandas.concat([hf_df_user,hf_df_expert])
    hf_df = hf_df.sort_values(['created_at']).reset_index(drop=True)
    hf_df["idx"] = hf_df.groupby(["conversation_id"]).cumcount()
    hf_df = hf_df.set_index(["conversation_id","idx"])
    print(hf_df)

    # build examples
    hf_df = hf_df.apply(build_examples,axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    for example in hf_df['example']:
        unlabelled.add_example(example)

    # write to output
    output_filepath = f"{filepath.split('.json')[0]}_hf.json"
    file_out = open(output_filepath,mode='w',encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"{output_filepath} is successfully created")

def split_timestamps(row: pandas.Series) -> pandas.Series:
    """splits timestamps accroding to the response timestamps"""

    # calculate individual logical unit level start and end
    row.input_timestamp = parser.parse(row.input_timestamp)
    row.response_timestamp = parser.parse(row.response_timestamp)
    time_per_string = (row.response_timestamp-row.input_timestamp)/len(row.response_text)
    start = [row.input_timestamp]
    end = []
    for i in range(len(row.response_text)):
        end.append((start[i] + time_per_string))
        if i+1 >= len(row.response_text):
            break
        start.append(end[i])
    
    # convert all the timestamps to isoformat
    for i in range(len(end)):
        end[i] = end[i].isoformat().split("+")[0]+"Z"
    row.response_timestamp = end
    row.input_timestamp = row.input_timestamp.isoformat().split("+")[0]+"Z"
    return row

def humanfirst_format_user(row : pandas.Series, role:str) -> pandas.Series:
    '''Creates client data'''

    hf_user = pandas.Series(data = [row.input_timestamp, role, row.input_text, row.convo_id, row.metadata],
                        index = ['created_at','role','utterance','conversation_id',"metadata"])
    return hf_user

def humanfirst_format_expert(row : pandas.Series, role: str) -> pandas.Series:
    '''Creates expert data'''

    hf_expert = pandas.Series(data = [row.response_timestamp, role, row.response_text, row.convo_id, row.metadata],
                          index = ['created_at','role','utterance','conversation_id',"metadata"])
    return hf_expert

def build_examples(row: pandas.Series) -> pandas.Series:
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
    

def get_response_text(responses: list) -> list:
    """Extracts responses"""

    response_text = []
    for response in responses:
        if "text" in response.keys():
            if "text" in response["text"].keys():
                response_text.append(response["text"]["text"][0])
    return response_text

def set_intent_name(row: pandas.Series) -> str:
    """Sets the intent dispayname depending on intent match type"""
    if row.intent_matchType == "NO_MATCH":
        return row.match_event
    return row.intent_displayName

def create_metadata(row: pandas.Series) -> dict:
    """Creates metadata"""

    metadata = {}
    for col_name in list(row.index):
        metadata[col_name] = str(row[col_name])
    return metadata

if __name__=="__main__":
    main()