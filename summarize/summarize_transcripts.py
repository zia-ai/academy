#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./summarize/summarize.py
#
# *****************************************************************************

# standard imports
import json
import re
from os.path import exists
from datetime import datetime
from datetime import timedelta
import os
from os.path import join
from pathlib import Path
import sys
import logging
import random
from multiprocessing import Pool
import time
from time import perf_counter
START_TIME = perf_counter()

dir_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(Path(dir_path).parent)
sys.path.insert(1,hf_module_path)

# 3rd party imports
import openai
import pandas
import numpy
import click

# custome imports
import humanfirst

@click.command()
@click.option('-i','--input_filepath',type=str,required=True,help='Path containing HF Unlabelled conversations in json format')
@click.option('-o','--output_filepath',type=str,default='',help='Path to store HF Unlabelled summarization of conversations in json format')
@click.option('-a','--openai_api_key',type=str,required=True,help='OpenAI API key')
@click.option('-n','--num_cores',type=int,default=8,help='Number of cores for parallelisation')
@click.option('-c','--conversation_count',type=int,default=100,help='Number of conversations to process')
@click.option('-s','--server',type=str,default='',help='Server log file path')
def main(input_filepath: str, output_filepath: str, openai_api_key: str, num_cores: int, conversation_count: str, server: str):
    '''Main Function'''
    process(input_filepath,output_filepath, openai_api_key, num_cores, conversation_count, server)

def process(input_filepath: str, output_filepath: str, openai_api_key: str, num_cores: int, conversation_count: str, server: str):
    '''Summarization of Conversations'''

    # logging config
    if server == "":
        server = join(str(Path(input_filepath).parent),"server.log")

    logging.basicConfig(filename=server, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(process)d - %(levelname)s -- %(message)s', datefmt='%d-%b-%y %H:%M:%S')

    openai.api_key = openai_api_key

    # load input data
    with open(input_filepath,mode="r",encoding="utf8") as f:
        data = json.load(f)
    df = pandas.json_normalize(data=data["examples"],sep="-")

    # set index as conversation ID
    df.set_index(["context-context_id"],drop=True,inplace=True)
    
    # randomly generate samples
    samplings_ids = list(set(df.index))
    if len(samplings_ids) > conversation_count:
        sampling_ids = random.sample(samplings_ids,conversation_count)
        df = df[df.index.isin(sampling_ids)]
    assert isinstance(df,pandas.DataFrame)

    dfs = numpy.array_split(df,num_cores)
    parallelization_input = []
    for dataframe in dfs:
        parallelization_input.append([dataframe,input_filepath])

    dfs = numpy.array_split(df,num_cores)
    parallelization_input = []
    for dataframe in dfs:
        parallelization_input.append([dataframe,input_filepath])
    
    # parallelization
    with Pool(num_cores) as p:
        parallelization_output = p.map(summarization,parallelization_input)

    logging.info(f"Total number of conversations summarized is {len(df.index.unique().to_list())}")

    key_reason_for_calling = []
    how_issue_resolved = []
    action_taken = []
    hindrance = []
    large_convo_id = []

    for output in parallelization_output:
        key_reason_for_calling.extend(output[0])
        action_taken.extend(output[1])
        hindrance.extend(output[2])
        how_issue_resolved.extend(output[3])
        large_convo_id.extend(output[4])

    df = {
        "issue": pandas.json_normalize(data=key_reason_for_calling),
        "resolution":pandas.json_normalize(data=how_issue_resolved),
        "action": pandas.json_normalize(data=action_taken),
        "hindrance": pandas.json_normalize(data=hindrance)
    }

    df["action"] = df["action"].explode(["text","seq"])
    df["hindrance"] = df["hindrance"].explode(["text","seq"])

    for key in df.keys():
        if key in ["issue","resolution"]:
            df[key].set_index(["id"],drop=True,inplace=True)
        else:
            df[key].set_index(["id","seq"],drop=True,inplace=True)

        # convert the summaries into unlabelled HF format
        unlabelled_workspace = humanfirst.HFWorkspace()        
        df[key] = df[key].apply(parse_utterances,axis=1,args=[unlabelled_workspace,key])

        if output_filepath == '':
            filename_split = input_filepath.split('.json')
            output_filepath_json = f"_summarized_{key}.json".join(filename_split)
        else:
            output_filepath_json = output_filepath

        output_filepath_csv = ".csv".join(output_filepath_json.split('.json'))
        df[key].to_csv(output_filepath_csv,sep=",",encoding="utf8")
        logging.info(f'{key}- Unlabelled CSV is saved at {output_filepath_csv}')
                
        with open(output_filepath_json, 'w', encoding='utf8') as file_out:
            unlabelled_workspace.write_json(file_out)
        logging.info(f'{key} - Unlabelled json is saved at {output_filepath_json}')
    
    logging.info(f'Total duration to run the script- {time.strftime("%H:%M:%S", time.gmtime(perf_counter() - START_TIME))}')

def summarization(input: list) -> list:
    '''Summarization'''

    df = input[0]
    input_filepath = input[1]

    indices = df.index.unique().to_list()

    # Summarization of every conversation
    key_reason_for_calling = []
    action_taken = []
    hindrance = []
    how_issue_resolved = []
    large_convo_id = []

    list_of_summary_paths = []
    len_of_indices = len(indices)
    i=0

    while i<len_of_indices:
        index = indices[i]
        example_df = df.loc[index]

        # get the conversation as client-expert dialogue
        conversation = get_conversation(example_df)

        summary_path = f"{input_filepath.split('.json')[0]}_conversation_{index}_summary.txt"
        list_of_summary_paths.append(summary_path)
        try:
            if not exists(summary_path):
                logging.warning(f"Summary for conversation ID {index} doesn't exists in the path {summary_path}")
                summary = call_api(index,conversation,summary_path)

            else:
                logging.info(f"Summary for conversation ID {index} already exists\nReading the summary from file {summary_path}")
                with open(summary_path, mode="r", encoding = "utf8") as f:
                    summary = f.read()
                    if summary == "":
                        logging.warning(f"Summary file {summary_path} is empty for conversation ID {index}")
                        summary = call_api(index,conversation,summary_path)
                 
            i=i+1
        except Exception as e:
            logging.error(f"Error upon calling API - {e}")
            if f"{e}".find("4097") != -1:
                large_convo_id.append(index)
                print(f"Large Conversation ID: {index}")
                i=i+1
                continue
            else:
                sec = 5
                time.sleep(sec)
                logging.info(f"Retrying API call for conversation - {index}")
                continue

        # parsing through the summary(openai response) to get individual utterances
        summary_turns = summary.split("\n")
        key_reason_for_calling_dict = {}
        action_taken_dict = {}
        hindrance_dict = {}
        how_issue_resolved_dict = {}
        action_text = []
        action_seq = []
        hindrance_text = []
        hindrance_seq = []

        for turn in summary_turns:
            turn = turn.split(": ")
            turn = [sent.strip() for sent in turn]
            if "key_reason_for_calling" in turn[0]:
                key_reason_for_calling_dict.update({
                    "id": index,
                    "text": turn[1],
                    "created_at": datetime.now()
                })
            elif turn[0].find("action_taken") != -1:
                action_text.append(turn[1])
                action_seq.append(str(len(action_seq) + 1))
            elif turn[0].find("hindrance") != -1:
                hindrance_text.append(turn[1])
                hindrance_seq.append(str(len(hindrance_seq) + 1))
            elif turn[0].find("how_issue_resolved") != -1:
                how_issue_resolved_dict.update({
                    "id": index,
                    "text": turn[1],
                    "created_at": datetime.now()
                })
            elif turn[0].find("whether_customer_issue_successfully_resolved") != -1:
                resolved = turn[1]
            else:
                if turn[0].find("...") != -1 or turn[0] == "":
                    logging.warning(f"Unknown key - {turn[0]} present in the summary of conversation id {index}")
                else:
                    raise Exception(f"Unknown key - {turn[0]} present in the summary")
        
        key_reason_for_calling_dict.update({"resolved":resolved})
        how_issue_resolved_dict.update({"resolved":resolved})

        if action_text:
            action_taken_dict.update({
                "id": index,
                "text": action_text,
                "seq": action_seq,
                "created_at": datetime.now(),
                "resolved": resolved
            })
            action_taken.append(action_taken_dict)

        if hindrance_text:
            hindrance_dict.update({
                "id": index,
                "text": hindrance_text,
                "seq": hindrance_seq,
                "created_at": datetime.now(),
                "resolved": resolved
            })
            hindrance.append(hindrance_dict)

        key_reason_for_calling.append(key_reason_for_calling_dict)
        how_issue_resolved.append(how_issue_resolved_dict)
    
    return [key_reason_for_calling, action_taken, hindrance, how_issue_resolved, large_convo_id]

def call_api(index: str, conversation: str, summary_path: str) -> str:
    '''Call OpenAI API for summarization'''

    logging.info(f"Calling OpenAI to summarize conversation - {index}")
    summary, total_tokens = summarize(conversation)
    if int(total_tokens) >= 4097:
        raise Exception(f"Total number of tokens {total_tokens} exceed 4097. It should be within this limit")
    logging.info(f"Conversation - {index} is summarized")
    with open(summary_path, mode="w", encoding = "utf8") as f:
        f.write(summary)
    logging.info(f"Summary is saved at {summary_path}")

    return summary

def summarize(text) -> tuple:
    '''Summarizes single conversation using prompt'''

    prompt = f"""The following is a transcription of an audio conversation between a customer of an online fashion retailer and a customer service agent.

    {text}

    Provide a concise list of short sentences describing
    the customers key reason for calling,
    a list of what actions the agent took to try and solve the issue,
    a list of issues (if any) that hindered the agent's ability to solve the customer's issue
    whether the customer issue was successfully resolved as either true, false or unclear
    how the issue was resolved
    
    Provide your answer in the format
    key_reason_for_calling:
    action_taken_1:
    action_taken_2:
    ...
    hindrance_1:
    hindrance_2:
    ...
    whether_customer_issue_successfully_resolved: True, False or Unclear
    how_issue_resolved:"""
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages =  [
            {"role": "user", "content": prompt}
        ],
        temperature=0.0,
        max_tokens=256,
        top_p=1,                # default value
        frequency_penalty=0.0,  # default value
        presence_penalty=0.0    # default value
    )
    return response.choices[0].message.content, response.usage.total_tokens

def get_conversation(example_df: pandas.DataFrame) -> str:
    '''Converts the conversations in HF format to customer-agent dialogue'''

    utterances = []
    for key, row in example_df.iterrows():
        if row["context-role"] == "client":
            utterances.append(f'Customer: {row["text"]}')
        else:
            utterances.append(f'Agent: {row["text"]}')
    return "\n".join(utterances)

def parse_utterances(row: pandas.Series, unlabelled_workspace: humanfirst.HFWorkspace, key: str) -> None:
    '''parse a single utterance to an example'''

    row["resolved"] = re.sub(r'[^\w\s]', '', str(row["resolved"])).lower()
    if key in ["issue", "resolution"]:
        row["created_at"] = (row["created_at"]).isoformat()
        metadata = {
            "id": str(row.name),
            "resolved": row["resolved"]
        }
        example_id = f'example-{row.name}'
        conversation_id = row.name
    else:
        row["created_at"] = (row["created_at"] + timedelta(seconds=int(row.name[1]))).isoformat()
        metadata = {
            "id": str(row.name[0]),
            "seq": str(row.name[1]),
            "resolved": row["resolved"]
        }
        example_id = f'example-{row.name[0]}-{row.name[1]}'
        conversation_id = row.name[0]
    
    # Will load these as conversations where it is only the client speaking
    context = humanfirst.HFContext(conversation_id,'conversation','client')

    # Create the example
    example = humanfirst.HFExample(
        text=row['text'], 
        id=example_id, 
        created_at=row["created_at"],
        intents=[], 
        tags=[], 
        metadata=metadata, 
        context=context
    )

    # add to the unlabelled_workspace
    unlabelled_workspace.add_example(example)
    return row

if __name__=="__main__":
    main()