#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./summarize/10_turn       # pylint: disable=invalid-name
#
# Makes a tagging file csv
#
# ********************************************************************************************************************

# standard imports
import os
import sys
import json
import pathlib
import datetime

# 3rd party imports
import click
import pandas

# Custom Imports
import_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(pathlib.Path(import_path).parent)
sys.path.insert(1, hf_module_path)
import humanfirst

@click.command()
@click.option('-j', '--jointo', type=str, required=True, help='Path of original data to join to')
@click.option('-s', '--spliton', type=str, required=False, default='/', help='Parent Child Hierarchical delimiter')
def main(jointo:str, spliton:str):
    '''Main function'''

    # read original workspace
    with open(jointo,mode="r",encoding="utf8") as original_file:
        original_dict = json.load(original_file)
    df_original = pandas.json_normalize(original_dict["intents"])
    all_names = df_original["name"].to_list()
    tags = []
    for name in all_names:
        assert isinstance(name,str)
        potential_tags = name.split(spliton)
        if isinstance(potential_tags,str):
            tags.append(potential_tags)
        else:
            tags.extend(potential_tags)
    tag_names = list(set(tags))
    try:
        tag_names.remove('')
    except Exception as e:
        print(f"Didn't have \'\' {e}")
    print(f'Total tag names to create: {len(tag_names)}')

    tags = []
    for tag_name in tag_names:
        tags.append(build_tags(tag_name))
    original_dict["tags"] = tags

    intents = original_dict["intents"]
    for intent in intents:
        tag_list = []
        metadata = {}
        for tag in tags:
            intent_name = intent["name"]
            assert isinstance(intent_name,str)
            if intent_name.find(tag["name"]) > 0:
                tag_list.append(tag)
                metadata[tag["name"]] = datetime.datetime.now().isoformat()
        intent["tags"] = tag_list.copy()
        intent["metadata"] = metadata.copy()

    original_dict["intents"] = intents

   # work out a file name
    output_file_candidate = jointo.replace(".json","_output.json")

    # write to filename
    with open(output_file_candidate,mode='w',encoding='utf8') as output_file:
        json.dump(original_dict,output_file,indent=2)

    print(f'Write complete: {output_file_candidate}')

def build_tags(tag_name:str) -> dict:
    """Build the tags"""
    return {
        "id": f'tagid-{tag_name}',
        "name": tag_name,
        "color": humanfirst.generate_random_color()
    }

def no_args_gen_color(row: pandas.Series) -> str:
    """Do the thing with no args"""
    return humanfirst.generate_random_color()
if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
