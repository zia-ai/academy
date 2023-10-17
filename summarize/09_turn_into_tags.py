#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./summarize/09_turn_into_tags.py                       # pylint: disable=invalid-name
#
# Makes a tagging file csv
#
# very rough specific script
#
# ********************************************************************************************************************

# standard imports
import os
import sys
import pathlib

# 3rd party imports
import click
import pandas

# Custom Imports
import_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(pathlib.Path(import_path).parent)
sys.path.insert(1, hf_module_path)

@click.command()
@click.option('-s', '--summaries_dir', type=str, default='./summaries', help='Summaries input file path')
@click.option('-w', '--workspaces_dir', type=str, default='./workspaces', help='Workspace output file path')
@click.option('-c', '--clientsuffix', type=str, required=True, help='to differentiate outputs')
@click.option('-j', '--jointo', type=str, required=False, default='', help='Path of original data to join to')
def main(summaries_dir: str, workspaces_dir: str, clientsuffix:str, jointo:str):
    '''Main function'''

    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'

    # find summary directory
    summaries_dir = check_directory(dir_path, summaries_dir)

    # read all the files
    file_names = os.listdir(summaries_dir)
    completed_ids = []
    fulltags = []
    parents = []
    children = []
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            completed_ids.append(completed_id)
            file_name = f'{summaries_dir}{file_name}'
            file = open(file_name, mode='r', encoding='utf8')
            contents = file.read()
            file.close()
            if contents.endswith('\n'):
                contents = contents[:-1]
            parent = ""
            child = ""
            parent = contents.split('/')[0]
            child = contents.split('/')[1]
            parents.append(parent)
            children.append(child)
            fulltags.append(contents)

    # make a df
    df = pandas.DataFrame(zip(completed_ids, fulltags, parents, children), columns=[
                          'Key', 'fulltag', 'parent', 'child'])

    # join to original data
    if jointo != '':
        df.set_index("Key",drop=True,inplace=True)
        df_original = pandas.read_csv(jointo,index_col=["Key"])
        print(df_original)
        df = df_original.join(df,on=["Key"])

    # work out a file name
    output_file_candidate = summaries_dir.strip("./")
    output_file_candidate = "_".join(output_file_candidate.split("/"))
    workspaces_dir = check_directory(dir_path, workspaces_dir)
    output_file_name = f'{workspaces_dir}{output_file_candidate}_{clientsuffix}.csv'
    print(f'filename: {output_file_name}')

    # write to filename
    df.to_csv(output_file_name,index=True)
    print('Write complete')


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
