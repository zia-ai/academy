#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./summarize/05_enrich_metadata.py                       # pylint: disable=invalid-name
#
# Assuming that your summaries are single line assign them to a field on the original json
#
# ********************************************************************************************************************

# standard imports
import os
import sys
import pathlib
import json

# 3rd party imports
import click
import pandas

# Custom Imports
import_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(pathlib.Path(import_path).parent)
sys.path.insert(1, hf_module_path)

@click.command()
@click.option('-i', '--input_filepath', type=str, required=True,
              help='Path containing HF Unlabelled conversations in json format')
@click.option('-s', '--summaries_dir', type=str, default='./summaries', help='Summaries input file path')
@click.option('-w', '--workspaces_dir', type=str, default='./workspaces', help='Workspace output file path')
@click.option('-c', '--clientsuffix', type=str, required=True, help='to differentiate outputs')
@click.option('-m', '--metadata_name', type=str, required=True, help='What to call the new metadata key value field')
def main(input_filepath: str, summaries_dir: str, workspaces_dir: str, clientsuffix:str, metadata_name: str):
    '''Main function'''

    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'

    # find summary directory
    summaries_dir = check_directory(dir_path, summaries_dir)

    # read all the files
    file_names = os.listdir(summaries_dir)
    completed_ids = []
    metadata_additions = []

    # get all the ids and additions.
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            completed_ids.append(completed_id)
            file_name = f'{summaries_dir}{file_name}'
            file = open(file_name, mode='r', encoding='utf8')
            contents = file.read()
            file.close()
            # trim trailing new line
            if contents.endswith('\n'):
                contents = contents[:-1]
            metadata_additions.append(contents)

    # make a df with the name passed
    zipped = zip(completed_ids, metadata_additions)
    df = pandas.DataFrame(zipped, columns=["id", metadata_name])
    print(df.groupby(metadata_name).count())
    df.set_index("id",drop=True,inplace=True)
    print(df)


    # load the original json
    # load input data
    file_obj =  open(input_filepath, mode="r", encoding="utf8")
    workspace = json.load(file_obj)
    file_obj.close # pylint: disable=pointless-statement

    # cycle through adding metadata
    for example in workspace["examples"]:
        # poor assumption here that it's like example-CA824a74952214b84d87bdd449c75ccd80-0
        # but some sort of lookup on example id.
        example_id = example["id"]
        assert isinstance(example_id, str)
        example_id = example_id.split('-')[1]
        example["metadata"][metadata_name] = df.loc[example_id,metadata_name]


    # work out a file name
    output_file_candidate = summaries_dir.strip("./")
    output_file_candidate = "_".join(output_file_candidate.split("/"))
    workspaces_dir = check_directory(dir_path, workspaces_dir)
    output_file_name = f'{workspaces_dir}{output_file_candidate}_{clientsuffix}.json'

    # write to filename
    with open(output_file_name,mode="w",encoding="utf8") as output_file:
        json.dump(workspace,output_file)
    print(f'Write complete: {output_file_name}')

def check_directory(dir_path: str, dir_string: str) -> str:
    '''Check directory local or absolute properties'''
    if dir_string.startswith('./'):
        dir_string = dir_string[2:]
        dir_string = f'{dir_path}{dir_string}'
    if not dir_string.endswith('/'):
        dir_string = f'{dir_string}/'
    return dir_string


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
