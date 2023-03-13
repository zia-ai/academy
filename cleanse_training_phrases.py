#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python cleanse_training_phrases.py -f filepath
#
# *****************************************************************************

# standard imports
import json
import re

# 3rd party imports
import pandas
import click

# custom imports
import humanfirst

@click.command()
@click.option('-f','--input_filepath',type=str,required=True,help='HF labelled json file')
@click.option('-o','--output_filepath',type=str,default="",help='Output file path with .json extension')
def main(input_filepath: str, output_filepath: str):

    cleanse_training_phrases(input_filepath, output_filepath)

def cleanse_training_phrases(input_filepath: str, output_filepath: str):
    """Ensures the training phrases are unique in the workspace level"""

    with open(input_filepath,mode="r",encoding="utf8") as f:
        data = json.load(f)
    
    df = pandas.json_normalize(data=data["examples"])

    df["text_base"] = df["text"].apply(cleanse_text)
    deduplicated_df = df.loc[~df["text_base"].duplicated()].reset_index(drop=True)
    deduplicated_df =  deduplicated_df.drop(columns=["text_base"])
    examples_json = deduplicated_df.to_json(orient="records")
    examples_json = json.loads(examples_json)
    data["examples"] = examples_json

    duplicated_df = df.loc[df["text_base"].duplicated()].reset_index(drop=True)
    workspace_only_with_intents = humanfirst.HFWorkspace.from_json({"intents":data["intents"]})
    intent_index = workspace_only_with_intents.get_intent_index(delimiter="-")
    print("------Duplicated Texts -> Intent it was removed from------")
    duplicated_df["intent_id"] = duplicated_df["intents"].apply(lambda intent_list: intent_list[0]["intent_id"])
    duplicated_df.apply(lambda row: print(f"{row.text} -> {intent_index[row.intent_id]}"),axis=1)

    if output_filepath == "":
        output_filepath = f"{input_filepath.split('.json')[0]}_deduplicated.json"
    else:
        output_filepath = f"{output_filepath.split('.json')[0]}.json"
    with open(output_filepath,mode="w",encoding="utf8") as f:
        json.dump(data,f,indent=3)
    print(f"\nAfter removing all the duplicate training phrases the json file is stored at {output_filepath}")

def cleanse_text(text: str) -> str:
    '''Remove punctuations and spaces'''

    # remove punctuations
    text = re.sub(r'\W','',text)

    # to lower case
    text = text.lower()

    return text

if __name__=="__main__":
    main()