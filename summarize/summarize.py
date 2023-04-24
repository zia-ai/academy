#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./summarize/summarize.py
#
# *****************************************************************************

# standard imports
import json
from os.path import exists
from datetime import datetime
from datetime import timedelta
import os
from pathlib import Path
import sys

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
def main(input_filepath: str, output_filepath: str, openai_api_key: str):
    '''Main Function'''
    process(input_filepath,output_filepath, openai_api_key)

def process(input_filepath: str, output_filepath: str, openai_api_key: str):
    '''Summarization of Conversations'''
    
    openai.api_key = openai_api_key

    # load input data
    with open(input_filepath,mode="r",encoding="utf8") as f:
        data = json.load(f)
    df = pandas.json_normalize(data=data["examples"],sep="-")
    
    # set index as conversation ID
    df.set_index(["context-context_id"],drop=True,inplace=True)
    indices = df.index.unique().to_list()

    # Summarization of every conversation
    summarized_conversations = []
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
                print(f"Summary for conversation ID {index} doesn't exists in the path {summary_path}")
                summary = call_api(index,conversation,summary_path)

            else:
                print(f"Summary for conversation ID {index} already exists\nReading the summary from file {summary_path}")
                with open(summary_path, mode="r", encoding = "utf8") as f:
                    summary = f.read()
                    if summary == "":
                        print(f"Summary file {summary_path} is empty for conversation ID {index}")
                        summary = call_api(index,conversation,summary_path)
                 
            i=i+1
        except Exception as e:
            print(f"Error upon calling API - {e}")
            print(f"Retrying API call for conversation - {index}")
            continue

        # parsing through the summary(openai response) to get individual utterances
        summary_turns = summary.split("\n")
        summary_turns = [turn.strip().strip("-").strip() for turn in summary_turns]

        summary_dict = {
            "id": index,
            "text": summary_turns,
            "seq": [i for i,j in enumerate(summary_turns,start=1)],
            "created_at": datetime.now()
        }
        summarized_conversations.append(summary_dict)
    
    print(f"Total number of conversations summarized is {len(summarized_conversations)}")

    summarized_conversations_df = pandas.json_normalize(data=summarized_conversations)
    summarized_conversations_df = summarized_conversations_df.explode(["text","seq"])

    summarized_conversations_df['first_convo_step'] = summarized_conversations_df['seq'] == 1
    summarized_conversations_df['seq_max'] = summarized_conversations_df.groupby(["id"])['seq'].transform(numpy.max)
    summarized_conversations_df['final_convo_step'] = summarized_conversations_df['seq'] == summarized_conversations_df['seq_max']
    summarized_conversations_df.drop(columns=['seq_max'],axis=1,inplace=True)

    summarized_conversations_df.set_index(["id","seq"],drop=True,inplace=True)
    print(summarized_conversations_df["text"])

    # convert the summaries into unlabelled HF format
    unlabelled_workspace = humanfirst.HFWorkspace()        
    summarized_conversations_df = summarized_conversations_df.apply(parse_utterances,axis=1,args=[unlabelled_workspace])

    if output_filepath == '':
        filename_split = input_filepath.split('.json')
        output_filepath_json = "_summarized.json".join(filename_split)

    output_filepath_csv = ".csv".join(output_filepath_json.split('.json'))
    summarized_conversations_df.to_csv(output_filepath_csv,sep=",",encoding="utf8")
    print(f'Unlabelled CSV is saved at {output_filepath_csv}')
            
    with open(output_filepath_json, 'w', encoding='utf8') as file_out:
        unlabelled_workspace.write_json(file_out)
    print(f'Unlabelled json is saved at {output_filepath_json}')

def call_api(index: str, conversation: str, summary_path: str) -> str:
    '''Call OpenAI API for summarization'''

    print(f"Calling OpenAI to summarize conversation - {index}")
    summary = summarize(conversation)
    print(f"Conversation - {index} is summarized")
    with open(summary_path, mode="w", encoding = "utf8") as f:
        f.write(summary)
    print(f"Summary is saved at {summary_path}")

    return summary

def summarize(text) -> str:
    '''Summarizes single conversation using prompt'''

    prompt = f"""Here is a transcript of a conversation between customer and agent working for a online clothes retailer. The clothes retailer has a wesbite which offers a subscription model and ships clothing items to customers. Please summarize the exchange in a few bullet points making sure to convey the full meaning of the conversation. Pay particular intention for the issue causing the customer to have this conversation and whether the conversation ended with the customer's issue being solved. Use the following format "- Short summary of what was discussed".

    Transcription begins:
    {text}
    End of transcription.
    Summary:
    -"""
    
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
    return response.choices[0].message.content

def get_conversation(example_df: pandas.DataFrame) -> str:
    '''Converts the conversations in HF format to customer-agent dialogue'''

    utterances = []
    for key, row in example_df.iterrows():
        if row["context-role"] == "client":
            utterances.append(f'Customer: {row["text"]}')
        else:
            utterances.append(f'Agent: {row["text"]}')
    return "\n".join(utterances)

def parse_utterances(row: pandas.Series, unlabelled_workspace: humanfirst.HFWorkspace) -> None:
    '''parse a single utterance to an example'''

    row["created_at"] = (row["created_at"] + timedelta(seconds=row.name[1])).isoformat()
    metadata = {
        "abcd_id": str(row.name[0]),
        "conversation_step": str(row.name[1]),
        "first_conversation_step": str(row["first_convo_step"]),
        "final_conversation_step": str(row["final_convo_step"])
    }

    # Will load these as conversations where it is only the client speaking
    context = humanfirst.HFContext(row.name[0],'conversation','client')

    # Create the example
    example = humanfirst.HFExample(
        text=row['text'], 
        id=f'example-{row.name[0]}-{row.name[1]}', 
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