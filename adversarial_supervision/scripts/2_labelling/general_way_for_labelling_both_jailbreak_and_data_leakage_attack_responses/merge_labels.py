#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./adversarial_supervision/scripts\
#         /2_labelling\
#         /general_way_for_labelling_both_jailbreak_and_data_leakage_attack_responses\
#         /merge_labels.py                                                               # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# standard imports
import json
import math

# 3rd party imports
import pandas
import click
# TODO: sort out 1.26.4 numpy depednencies
import tensorflow_hub as hub
import numpy as np

class UnexpectedIntentName(Exception):
    """This happens when the intent name is neither 'appropriate' or 'inappropriate'"""

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='File containing responses of prompt attack')
@click.option('-h', '--hf_exported_filename', type=str, required=True, help='This file contains all the labels')
def main(filename: str, hf_exported_filename: str):
    """Main Function"""
    process(filename, hf_exported_filename)

def process(filename: str, hf_exported_filename: str) -> None:
    """Merge labels to the dataset"""

    # read the input csv
    df = pandas.read_csv(filename, sep=",", encoding='utf8')

    # index
    df = df.set_index(['id'], drop=True)

    with open(hf_exported_filename,mode="r",encoding="utf8") as f:
        data = json.load(f)

    hf_df_examples = pandas.json_normalize(data=data["examples"],sep="-")
    hf_df_examples["intent_id"] = hf_df_examples["intents"].apply(lambda x: x[0]["intent_id"])
    hf_df_examples.set_index(["metadata-id"],drop=True,inplace=True)

    hf_df_intents = pandas.json_normalize(data=data["intents"],sep="-")
    hf_df_intents.set_index(["id"],drop=True,inplace=True)

    hf_df_examples["inappropriate"] = hf_df_examples["intent_id"].apply(merge_intent_name,args=[hf_df_intents])

    hf_df_examples.drop(columns=['id', 'text', 'intents', 'created_at', 'updated_at',
       'context-context_id', 'context-type', 'intent_id'],inplace=True)

    df = pandas.concat([df,hf_df_examples],axis=1)

    # df with no labels
    df_with_no_labels = df.loc[df["inappropriate"].isna()]

    # df with labels
    df = df.loc[~df["inappropriate"].isna()]

    # Load the Universal Sentence Encoder model
    # this helps in labelling deduplicated utterances that did not get labelled in the HF tool
    model = hub.load("https://tfhub.dev/google/universal-sentence-encoder/3")
    df_with_no_labels["inappropriate"] = df_with_no_labels[["response"]].apply(find_matching_text,
                                                                               args=[df, model],
                                                                               axis=1)

    df = pandas.concat([df,df_with_no_labels])

    print(df[["response","inappropriate"]])

    output_filename = filename.replace(".csv","_labelled.csv")
    df.index.name = "id"
    df.to_csv(output_filename,sep=",",encoding="utf8",index=True)
    print(f"The labelled file is stored at {output_filename}")

def merge_intent_name(intent_id: str, df: pandas.DataFrame) -> str:
    """return intent name"""

    name = df.loc[intent_id]["name"]
    if name == "appropriate":
        return "no"
    elif name == "inappropriate":
        return "yes"
    else:
        raise UnexpectedIntentName(f"{name} does not belong to [appropriate, inappropriate]")

def find_matching_text(row1: pandas.Series, df: pandas.DataFrame, model) -> str:
    """Find matching text and return its label"""

    try:
        df_match = df.loc[df["response"]==row1["response"]]
        result = df_match.loc[df.index[0]]["inappropriate"]
        print("Inside try")
        print(result)
    except Exception: # pylint: disable=broad-exception-caught
        for row2 in df.iterrows():
            # Encode the sentences using the Universal Sentence Encoder
            embd = np.array(model([row2[1]["response"], row1["response"]])["outputs"]).tolist()
            similarity = np.inner(embd[0],embd[1])
            if math.floor(similarity) == 1:
                result = row2[1]["inappropriate"]
                break
            elif similarity >= 0.9:
                result = row2[1]["inappropriate"]
                break

    return result

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
