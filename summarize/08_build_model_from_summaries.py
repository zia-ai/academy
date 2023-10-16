#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./summarize/08_build_model_from_summaries.py                     # pylint: disable=invalid-name
#
# looks through directory
# used for performance test build very rough
#
# ********************************************************************************************************************

# standard imports
import os

# 3rd party imports
import click
import pandas

@click.command()
@click.option('-s', '--summaries_dir', type=str, default='./summaries', help='Summaries input file path')
@click.option('-w', '--workspaces_dir', type=str, default='./workspaces', help='Workspace output file path')
@click.option('-c', '--client', type=str, required=True, help='Which client this is for')
def main(summaries_dir: str, workspaces_dir: str, client: str):
    '''Main function'''

    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'

    # find summary directory
    summaries_dir = check_directory(dir_path, summaries_dir)

    # read all the files
    file_names = os.listdir(summaries_dir)
    completed_ids = []
    values = []
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            completed_ids.append(completed_id)
            file_name = f'{summaries_dir}{file_name}'
            file = open(file_name, mode='r', encoding='utf8')
            contents = file.read()
            file.close()
            contents = contents.split("\n")[0:-1] # Split also removes "-" bullets. Ignore empty first
            contents = [str(s).strip('\"') for s in contents] # Strip newlines from the end
            values.append(contents)

    # create a df
    df = pandas.DataFrame(zip(completed_ids, values),columns=["intent_name","utterance"])

    # explode df
    df = df.explode("utterance",ignore_index=True)

    # reverse column order for HF
    df = df[["utterance","intent_name"]]

    # work out a file name
    output_file_candidate = summaries_dir.strip("./")
    output_file_candidate = "_".join(output_file_candidate.split("/"))
    workspaces_dir = check_directory(dir_path, workspaces_dir)
    output_file_name = f'{workspaces_dir}{output_file_candidate}_{client}.csv'

    # write to filename
    df.to_csv(output_file_name,index=False,header=False)
    print(f'Wrote to {output_file_name}')


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
