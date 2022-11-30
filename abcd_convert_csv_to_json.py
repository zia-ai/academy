#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python abcd_convert_csv_to_json.py --input abcd-exercise03-base-model.csv
# 
# Produces
# ./data/abcd_labelled.json
#
# Example of converting labelled csv format to JSON
#
# *****************************************************************************

# standard imports

# third party imports
import pandas
import click

# custom imports
import common

@click.command()
@click.option('-i','--input',type=str,required=True,help='The input CSV for instance "abcd-exercise03-base-model.csv"')
@click.option('-w','--workspace',type=str,default='abcd',help='The folder within ./workspaces')
def main(input: str, workspace: str):
    process(input,workspace)

def process(input: str, workspace: str):
    ''' Read a csv source controlled labelled set and produce HF format'''
    
    labelled_workspace = common.HFWorkspace()

    df = pandas.read_csv(f'./workspaces/{workspace}/{input}',delimiter=',',names=['utterance','slash_sep_hier'])
    assert isinstance(df,pandas.DataFrame)
    df.apply(build_intents_from_file, args=[labelled_workspace],axis=1)

    file_out = open(f'./data/{input.split(".csv")[0]}.json', 'w', encoding='utf8')
    labelled_workspace.write_json(file_out)
    file_out.close()

def build_intents_from_file(row: pandas.Series, labelled_workspace: common.HFWorkspace):
    '''Creates the labelled examples without tags from the saved csv hf export format'''
    hierarchy = str(row['slash_sep_hier']).split('/')
    labelled_workspace.example(row['utterance'],intents=[labelled_workspace.intent(hierarchy)])

if __name__ == '__main__':
    main()