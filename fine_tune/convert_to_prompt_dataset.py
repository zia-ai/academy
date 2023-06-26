#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ******************************************************************************************************************120
#
# python convert_to_prompt_dataset.py
#
# *********************************************************************************************************************

# standard imports
import json
import random # pylint: disable=unused-import
import re

# 3rd party imports
import click
import pandas

@click.command()
@click.option("-i", "--input_file", type=str, required=True, help="input file path")
@click.option("-l", "--incremental_prompt", is_flag=True, default=False, help="Sets the incremental prompt output")
@click.option("-s", "--sample_size", type=int, default=0, help="sample number of conversations")
@click.option("-f", "--intent_specific_id_file", type=str, default="", help="intent specific ids file path")
@click.option("-x", "--suffix", type=str, required=True, help="suffix to the output file path")
def main(input_file: str,
         sample_size: int,
         intent_specific_id_file: str,
         suffix: str,
         incremental_prompt: bool) -> None:
    """Main Function"""

    with open(input_file, mode="r", encoding="utf8") as file_obj:
        data = json.load(file_obj)

    df = pandas.json_normalize(data=data["examples"],sep="-")

    print(f"Size of dataframe: {df.shape}")
    # print(df.columns)

    # enforce id is string
    df["context-context_id"] = df["context-context_id"].astype(str)

    # give a sequence number to each utterance
    df = df.sort_values(["context-context_id","created_at"])
    df['seq'] = df.groupby("context-context_id").cumcount()

    # set context-context_id and seq as index
    df.set_index(["context-context_id","seq"],drop=True,inplace=True)

    if intent_specific_id_file == "":
        context_ids = set(df.index.unique(level=0))
    else:
        with open(intent_specific_id_file, mode="r", encoding="utf8") as f_obj:
            context_ids = f_obj.read()
            context_ids = context_ids.split("\n")
            context_ids = [id.strip() for id in context_ids]

    context_ids = sorted(list(context_ids))

    # print(context_ids)
    print(f"Number of conversations: {len(context_ids)}")

    if sample_size > 0:
        # context_ids = random.sample(context_ids,sample_size)
        context_ids = context_ids[0:sample_size]

    print(f"Number of conversations chosen: {len(context_ids)}")

    # select down the data frame to that.
    df = df.loc[context_ids, :]

    training_samples = {}

    for context_id in context_ids:

        training_data = []

        # choose a single conversation
        df_convo = df.loc[[context_id], :]

        # get its sequence of utterances
        seq = list(df_convo.index.unique(level=1))

        full_convo = []

        for i in seq:
            role = df_convo.loc[context_id,i]["context-role"]
            text = df_convo.loc[context_id,i]["text"]

            # check if the text has period at the end, otherwise add a period
            if text[-1] != ".":
                text = f"{text}."

            if role == "client":
                full_convo.append(f"Customer: {text}")
            elif role == "expert":
                full_convo.append(f"Agent: {text}")
            elif role == "action":
                full_convo.append(f"Action: {text}")
            else:
                pass

        # having dialog start with client utterance and end with bot utternace
        # helps with avoiding irregular prompt-completion pairs
        if re.findall(r"^Agent: |^Action: ",full_convo[0]):
            full_convo.insert(0,"Customer: Hi")

        if re.findall(r"^Customer: |^Action: ",full_convo[-1]):
            full_convo.append("Agent: Have a nice day")

        print("\n".join(full_convo))
        print()

        i = 0
        while i < len(full_convo):
            customer_count = 0
            action_count = 0
            agent_count = 0
            j = i

            while j<len(full_convo) and re.findall(r"^Customer: ", full_convo[j]):
                customer_count = customer_count+1
                j = j+1

            while  j<len(full_convo) and re.findall(r"^Action: |^Agent: ", full_convo[j]):
                if re.findall(r"^Action: ", full_convo[j]):
                    action_count = action_count+1
                else:
                    agent_count = agent_count+1
                j = j+1

            if incremental_prompt:
                prompt = "\n".join(full_convo[0:i+customer_count])
            else:
                prompt = "\n".join(full_convo[i:i+customer_count])

            # recommended to end the prompt with a seperator
            prompt = f"{prompt}\n\n###\n\n"
            completion = "\n".join(full_convo[i+customer_count:i+customer_count+action_count+agent_count])

            # recommended to have whitespace prefix for completion
            completion = f" {completion}"

            # recommended to have stop words at the end - \n
            completion = f"{completion}###"

            print(prompt)
            print(completion)
            training_data.append({
                "prompt": prompt,
                "completion": completion
            })
            i = i + customer_count + action_count + agent_count

        training_samples[context_id] = training_data

    file_output = input_file.replace(".json",f"_{suffix}_fine_tune_set.json")
    with open(file_output, mode="w", encoding="utf8") as file_obj:
        json.dump(training_samples, file_obj, indent=3)

if __name__ == "__main__":
    main() # pylint: disable=no-value-for-parameter
