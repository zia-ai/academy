#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./summarize/summarize.py
#
# *****************************************************************************

# standard imports
import json
from datetime import datetime
from datetime import timedelta
import sys
sys.path.insert(1,'/home/ubuntu/source/academy')

# 3rd party imports
import openai
import pandas
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
    for index in indices:
        example_df = df.loc[index]

        # get the conversation as client-expert dialogue
        conversation = get_conversation(example_df)

        print(f"Calling OpenAI to summarize conversation - {index}")
        summary = summarize(conversation)
        print(f"Conversation - {index} is summarized")

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
    summarized_conversations_df.set_index(["id","seq"],drop=True,inplace=True)
    print(summarized_conversations_df["text"])

    # convert the summaries into unlabelled HF format
    unlabelled_workspace = humanfirst.HFWorkspace()        
    summarized_conversations_df.apply(parse_utterances,axis=1,args=[unlabelled_workspace])

    if output_filepath == '':
        filename_split = input_filepath.split('.json')
        output_filepath = "_summarized.json".join(filename_split)
            
    with open(output_filepath, 'w', encoding='utf8') as file_out:
        unlabelled_workspace.write_json(file_out)
    print(f'Unlabelled json is saved at {output_filepath}')

def summarize(text) -> str:
    '''Summarizes single conversation using prompt'''

    prompt = f"""Here is a transcription of conversation between customer and agent working for a online clothes retailer who offers a subscription model, a website, and ships products to people. Please summarize the exchange in a few bullet points, making sure to convey the full meaning of the conversation. Pretend that you are summarizing this in order to keep a record of the reasons customer having this conversation, and in order to improve on the internal processes of the company. Use  the following format "- Short summary of what was discussed". The summary should clearly indicate what the customer wanted, and how the agent managed to provide help, if they did.
    Transcription begins:
    {text}
    End of transcription.
    Summary:
    -"""
    
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=prompt,
        temperature=0.9,
        max_tokens=256,
        top_p=1,                # default value
        best_of=1,              # default value
        frequency_penalty=0.0,  # default value
        presence_penalty=0.0    # default value
    )
    return response.choices[0].text

def get_conversation(example_df: pandas.DataFrame) -> str:
    '''Converts the conversations in HF format to client-expert dialogue'''

    utterances = []
    for key, row in example_df.iterrows():
        utterances.append(f'{row["context-role"]}: {row["text"]}')
    return "\n".join(utterances)

def parse_utterances(row: pandas.Series, unlabelled_workspace: humanfirst.HFWorkspace) -> None:
    '''parse a single utterance to an example'''

    row["created_at"] = (row["created_at"] + timedelta(seconds=row.name[1])).isoformat()
    metadata = {
        "abcd_id": str(row.name[0]),
        "conversation_step": str(row.name[1])
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

if __name__=="__main__":
    main()