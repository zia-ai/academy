#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./summarize/02_make_labelled.py
#
# Looks through the summaries directory and turns any *.txt files there
# into an labelled workspace with a metadata field of the original id
#
# ********************************************************************************************************************

# standard imports
import os
import datetime
import sys
import pathlib

# 3rd party imports
import click
import pandas

# Custom Imports
import_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(pathlib.Path(import_path).parent)
sys.path.insert(1, hf_module_path)
import humanfirst # pylint: disable=wrong-import-position

@click.command()
@click.option('-s', '--summaries_dir', type=str, default='./summaries', help='Summaries input file path')
@click.option('-w', '--workspaces_dir', type=str, default='./workspaces', help='Workspace output file path')
def main(summaries_dir: str, workspaces_dir: str):
    '''Main function'''

    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'

    # find summary directory
    summaries_dir = check_directory(dir_path, summaries_dir)

    # read all the files
    file_names = os.listdir(summaries_dir)
    print(f'There are {len(file_names)} files')
    df = pandas.DataFrame()
    all_professions = set()
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            splits = str(completed_id).split("-")
            profession = splits[0:-5]
            profession = '-'.join(profession)
            if profession == '':
                print("BLANK")
            if len(profession) > 40:
                print("LONG")
                print(profession)
            all_professions.add(profession)
            file_name = f'{summaries_dir}{file_name}'
            file = open(file_name, mode='r', encoding='utf8')
            contents = file.read()
            file.close()
            contents = contents.split("\n")
            lines = []
            ids = []
            professions = []
            for content in contents:
                content = content.strip(' ')
                content = content.strip('-')
                content = content.strip(' ')
                content = content.strip('"')
                lines.append(content)
                ids.append(completed_id)
                professions.append(profession)
            # Add just the profession to it's training example
            lines.append(profession)
            ids.append(completed_id)
            professions.append(profession)
            df_temp = pandas.DataFrame(zip(professions,lines,ids),columns=["profession",'summary','ids'])
            df = pandas.concat([df,df_temp])
        else:
            print(f'Skipped {file_name}')

    df = df[df["summary"]!=""]            

    # declare an labelled workspace
    labelled = humanfirst.HFWorkspace()
    
    df.apply(do_process,args=[labelled],axis=1)
    
    print(f'Processed {len(labelled.examples)} examples')
    print(f'Processed {len(labelled.intents)} intents')

    # write to output
    workspaces_dir = check_directory(dir_path, workspaces_dir)
    output_file_name = f'{workspaces_dir}summaries.json'
    file_out = open(f'{output_file_name}', mode='w', encoding='utf8')
    labelled.write_json(file_out)
    print(f'Wrote to {output_file_name}')
    file_out.close()


def do_process(row: pandas.Series, labelled: humanfirst.HFWorkspace):
    intent = labelled.intent(name_or_hier=list([row["profession"]]))
    
    # build example
    example = humanfirst.HFExample(
        text=row["summary"],
        id=humanfirst.hash_string(row["summary"],'example'),
        created_at=datetime.datetime.now(),
        intents=[intent],
        tags=[],
        metadata={
            "profession": row["profession"]
        },
        context={}
    )

    # add example to workspace
    labelled.add_example(example)

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
