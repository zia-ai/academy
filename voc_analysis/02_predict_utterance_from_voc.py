#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./voc_analysis/02_predict_utterance_from_voc.py 
#        -f ./data/voc.csv 
#        -r <review col name>
#        -d "Survey ID" 
#        -u <hf username> 
#        -p <hf password> 
#        -n <namepsace> 
#        -b <playbook id>
#        --background
#
# *****************************************************************************

# standard imports
import json
import os
from pathlib import Path
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(Path(dir_path).parent)
sys.path.insert(1,hf_module_path)

# third party imports
import click
import pandas
import nltk
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

# custom imports
import humanfirst_apis
import voc_helper

@click.command()
@click.option('-f','--input_filename',type=str,required=True,help='Input File')
@click.option('-o','--output_filename',type=str,default='',help='Output File')
@click.option('-r','--review_col',type=str,required=True,help='Column name of the user review')
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-h', '--background', is_flag=True, default=False, help='Adds background info to the CSV')
@click.option('-d', '--doc_col_id', type=str, default='Survey ID', help='Document ID Column Name')
@click.option('-c', '--chunk', type=int, default=500, help='size of maximum chunk to send to batch predict')
@click.option('-s', '--sentencize', type=bool, required=False, default=True, help='whether to sentencize the review_col or not')
def main(input_filename: str, output_filename: str, review_col:  str,
         username: str, password: int, namespace: bool, playbook: str, 
         background: bool, bearertoken: str, doc_col_id: str, chunk: int, sentencize: bool) -> None:
    
    pt = nltk.tokenize.PunktSentenceTokenizer()
    load_file(input_filename, output_filename, review_col, pt, username, password, namespace, playbook, bearertoken, background, doc_col_id, chunk, sentencize)

def load_file(input_filename: str, output_filename: str, review_col: str, pt: nltk.tokenize.PunktSentenceTokenizer,
              username: str, password: int, namespace: bool, playbook: str, bearertoken: str, background: bool, doc_col_id: str, chunk: int, sentencize: bool) -> None:

    # convert csv to dataframe
    df = voc_helper.get_df_from_input(input_filename, review_col)

    # split each review into segments
    if sentencize:
        print('Using punkt to segement the reviews')
        df = voc_helper.sentence_split_and_explode(df, pt, review_col)
        print(f'Shape after using punkt {df.shape}')
    else:
        df['utterance'] = df[review_col]

    headers = humanfirst_apis.process_auth(username=username, password=password)
    # predict = humanfirst_apis.predict(headers=headers,namespace=namespace,playbook=playbook,sentence="The refund was not too hard to organise, but I do not like substitution without consultation.")
    # print(json.dumps(predict,indent=3))
    # quit()

    fully_qualified_intent_name = []
    confidence = []
    parent_intents = []
    child_intents = []
    num_processed = 0
    for i in range(0, df['utterance'].size, chunk):
        utterance_chunk = list(df['utterance'][i : i + chunk])
        response_dict = humanfirst_apis.batchPredict(headers=headers, sentences=utterance_chunk, namespace=namespace, playbook=playbook)
  
        for j in range(len(utterance_chunk)):
            confidence.append(response_dict[j]['matches'][0]['score'])
            hierarchy =  response_dict[j]['matches'][0]['hierarchyNames']
            intent_name = hierarchy[0]
            for i in range(1,len(hierarchy)):
                intent_name = f'{intent_name}-{hierarchy[i]}'
            fully_qualified_intent_name.append(intent_name)
            predicted_intent = response_dict[j]['matches'][0]['name']
            if predicted_intent != fully_qualified_intent_name[-1]:
                child_intents.append(predicted_intent)
            else:
                child_intents.append(None)
            parent_intents.append(hierarchy[0])
        num_processed = num_processed + chunk 
        print(f'Completed: {num_processed} utterances')
    print(f'Completed: {df["utterance"].size} utterances')
    df['fully_qualified_intent_name'] = fully_qualified_intent_name
    df['confidence'] = confidence
    df['parent_intent'] = parent_intents
    df['child_intent'] = child_intents

    if background:
        df_doc = df[[doc_col_id, "parent_intent","child_intent"]].set_index([doc_col_id, "parent_intent"],drop=True)
        df_doc.sort_index(inplace=True)
        df = df.apply(assign_background_child_intent,args=[df_doc,doc_col_id],axis=1)
    
    if output_filename == '':
        filename_split = input_filename.split('/')
        filename_split[-1] = "voc_predictions.csv"
        output_filename = '/'.join(filename_split)
            
    df.to_csv(output_filename, index=False, encoding='utf8')
    print(f'VOC predictions CSV is saved at {output_filename}')

def assign_background_child_intent(row: pandas.Series, df_doc: pandas.DataFrame, doc_col_id: str) -> pandas.Series:
    '''Splits the child intents into seniors, parents, disabled, and others'''

    row["background-seniors"] = False
    row["background-parents_and_families"] = False
    row["background-disabled_illness_condition"] = False
    row["background-others"] = False
    
    count = 0
    if "background" in df_doc.loc[row[doc_col_id]].index:
        for background_child in df_doc.loc[row[doc_col_id],"background"]["child_intent"].to_list():
            if background_child in ["seniors","parents_and_families","disabled_illness_condition"]:
                row[f"background-{background_child}"] = True
                count = count + 1
        if count == 0:
           row["background-others"] = True
    else:
        row["background-others"] = True
    
    return row

if __name__ == '__main__':
    main()