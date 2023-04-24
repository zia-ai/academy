#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./voc_analysis/01_voc_csv_to_hf_unlabelled.py 
#        -f ./data/voc.csv
#        -r "Reason for Score Comment" 
#        -t "Responsedate" 
#        -d "Survey ID" 
#        --sentence_split
#
# *****************************************************************************

# standard imports
import datetime
from dateutil import parser
from datetime import timedelta  
import os
from pathlib import Path
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(Path(dir_path).parent)
sys.path.insert(1,hf_module_path)

# third party imports
import pandas
import click
import uuid
import nltk
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

# custom imports
import humanfirst
import voc_helper

@click.command()
@click.option('-f','--input_filename',type=str,required=True,help='Input File')
@click.option('-o','--output_filename',type=str,default='',help='Output File')
@click.option('-r','--review_col',type=str,required=True,help='Column name of the user review')
@click.option('-t','--review_time_col',type=str,required=True,help='Column name of review time')
@click.option('-d', '--document_id_col', type=str, required=True, help='Document id of the review')
@click.option('-s', '--sentence_split', is_flag=True, default=False, help='Splits sentences into utterances')
def main(input_filename: str, output_filename: str, review_col:  str, review_time_col: str, document_id_col: str, sentence_split: bool) -> None:
    
    pt = nltk.tokenize.PunktSentenceTokenizer()
    load_file(input_filename, output_filename, review_col, review_time_col, document_id_col, sentence_split, pt)

def load_file(input_filename: str, output_filename: str, review_col: str, review_time_col: str, document_id_col: str, sentence_split: bool, pt: nltk.tokenize.PunktSentenceTokenizer) -> None:

    # convert csv to dataframe
    df = voc_helper.get_df_from_input(input_filename, review_col)

    # check if all reviews have review time
    df = df.apply(check_time_and_assign_convo_id,args=[review_time_col, document_id_col],axis=1)
    
    # split each review into segments
    if sentence_split:
        print('Using punkt to segement the reviews')
        df = voc_helper.sentence_split_and_explode(df, pt, review_col)

    unlabelled_workspace = humanfirst.HFWorkspace()        
    df.apply(parse_utterances,axis=1,args=[unlabelled_workspace,review_col,review_time_col])

    if output_filename == '':
        filename_split = input_filename.split('/')
        filename_split[-1] = "voc_unlabelled.json"
        output_filename = '/'.join(filename_split)
            
    with open(output_filename, 'w', encoding='utf8') as file_out:
        unlabelled_workspace.write_json(file_out)
    print(f'Unlabelled json is saved at {output_filename}')

def check_time_and_assign_convo_id(row: pandas.Series, review_time_col: str, document_id_col: str) -> pandas.Series:
    '''if nan replace it with current time and also assign conversation_id'''

    row['conversation_id'] = str(row[document_id_col])
    if pandas.isna(row[review_time_col]):
        row[review_time_col] = str(datetime.datetime.now())
    return row

def parse_utterances(row: pandas.Series, unlabelled_workspace: humanfirst.HFWorkspace, review_col: str, review_time_col: str) -> None:
    '''parse a single utterance to an example'''

    row[review_time_col] = (parser.parse(row[review_time_col]) + timedelta(seconds=row['seq'])).isoformat()
    metadata = create_metadata(row,review_col)

    # Will load these as conversations where it is only the client speaking
    context = humanfirst.HFContext(row['conversation_id'],'conversation','client')

    # Create the example
    example = humanfirst.HFExample(
        text=row['utterance'], 
        id=f'example-{row.name}-{row["seq"]}', 
        created_at=row[review_time_col],
        intents=[], 
        tags=[], 
        metadata=metadata, 
        context=context
    )

    # add to the unlabelled_workspace
    unlabelled_workspace.add_example(example)

def create_metadata(row: pandas.Series, review_col: str) -> dict:
    '''Creates metadata'''

    metadata = {}

    # HFMetadata values must be strings
    for index, value in row.items():
        if not pandas.isna(value):
            if index not in ['utterance','seq',review_col,'conversation_id','fully_qualified_intent_name','confidence','parent_intent','child_intent']:
                metadata[index] = str(value)
            if index == 'seq':
                metadata[index] = str(value+1)
    return metadata

if __name__ == '__main__':
    main()