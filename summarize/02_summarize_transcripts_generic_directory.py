#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./summarize/01_summarize_transcripts_generic_directory.py
# 
# Looks through the summaries directory and turns any *.txt files there
# into an unlabelled workspace with a metadata field of the original id
# 
# *****************************************************************************

# standard imports
import os
import sys
import pathlib
dir_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(pathlib.Path(dir_path).parent)
sys.path.insert(1,hf_module_path)
import humanfirst
import click
import datetime

@click.command()
@click.option('-s','--summaries_dir',type=str,default='./summaries',help='Summaries input file path')
@click.option('-w','--workspaces_dir',type=str,default='./workspaces',help='Workspace output file path')
def main(summaries_dir: str, workspaces_dir: str):
    
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'
         
    # find summary directory
    summaries_dir = check_directory(dir_path, summaries_dir)

    # read all the files
    file_names = os.listdir(summaries_dir)
    completed_ids = []
    summaries = {}
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            completed_ids.append(completed_id)
            file_name = f'{summaries_dir}{file_name}'   
            file = open(file_name,mode='r',encoding='utf8')
            summaries[completed_id] = file.read()
            file.close()
            
    # declare an unlabelled workspace
    unlabelled = humanfirst.HFWorkspace()
    
    # create an example for each file    
    for c in completed_ids:
        # build example
        example = humanfirst.HFExample(
                text=summaries[c],
                id=c,
                created_at=datetime.datetime.now(),
                intents=[],
                tags=[],
                metadata={
                    "id":c
                },
                context={}
        )

        # add example to workspace
        unlabelled.add_example(example)
        
    print(f'Processed {len(unlabelled.examples)} examples')
    
    # write to output
    workspaces_dir = check_directory(dir_path, workspaces_dir)
    output_file_name = f'{workspaces_dir}summaries.json'
    file_out = open(f'{output_file_name}',mode='w',encoding='utf8')
    unlabelled.write_json(file_out)
    print(f'Wrote to {output_file_name}')
    file_out.close()
    
def check_directory(dir_path: str, dir_string:str) -> str:
    if dir_string.startswith('./'):
        dir_string = dir_string[2:]
        dir_string = f'{dir_path}{dir_string}'
    if not dir_string.endswith('/'):
        dir_string = f'{dir_string}/'
    return dir_string

if __name__=="__main__":
    main()