#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python liu_benchmark.py -n liuetal
#
# download the input file first using liu_download.sh into ./data
# 
# Uses the data set published in this paper
# Liu, X., Eshghi, A., Swietojanski, P. and Rieser, V., 2019. 
# Benchmarking natural language understanding services for building conversational agents. 
# arXiv preprint arXiv:1903.05566."
#
# https://arxiv.org/pdf/1903.05566.pdf
# 
# https://github.com/xliuhw/NLU-Evaluation-Data
#
# *****************************************************************************

# standard imports
import datetime
import json
import copy
import re

# third party imports
import pandas
import click

# custom imports
import humanfirst

# define regex
re_deannotate = re.compile(r"\[\s*([A-Za-z0-9-_]+)\s*:\s*([A-Za-z0-9@-_’'\. ]+)\]")

@click.command()
@click.option('-n','--name',type=str,default='liuetal',help='Name used for all files')
@click.option('-s','--sample',type=int,default=0,help='n conversations to sample from dataset')
@click.option('-w','--workspace',type=bool,default=False,help='whether to reload workspace')
def main(name: str, sample: int, workspace: str):
    # TODO - test workspace function

    # read file    
    dtypes = {
        'userid': str,
        'answerid': str,
        'status': str,
        'intent': str,
        'status': str,
        'answer_annotation': str,
        'notes': str,
        'suggested_entities': str,
        'answer_normalised': str,
        'answer': str,
        'question': str
    }
    df = pandas.read_csv(f'./data/{name}.csv', encoding='utf8', sep=';', dtype=dtypes, keep_default_na=False)
    assert isinstance(df,pandas.DataFrame)
    print(f'Read file of df.shape: {df.shape}')

    # need to drop from original set items that the paper decided where irrelevant/wrong    
    drop_statuses = ['IRR','IRR_XL','IRR_XR']
    # full list of statuses kept ADD (addition by annotator), MOD (see notes for modification), MOD_XL (see notes for modication), null, blank)
    df = df[~df['status'].isin(drop_statuses)]

    # we have music as an intent and as a scenario.  We have intent query identically named in many scenarios. 
    # TODO: look at humanfirst.py and decide if need update on how taxonmy managed. 
    # in meantime workaround is makeing intent scenario_intent under parent scenario.
    df['scenario_intent'] = df['scenario'] + '_' + df['intent']

    # allow sampling for a smaller subset
    if sample > 0:
        df = df.sample(sample)
        print(f'Sampled down to {sample} conversations')
    else:
        print('No sampling down, full set')
    
    # create workspaces
    labelled_workspace = humanfirst.HFWorkspace()
    unlabelled_workspace = humanfirst.HFWorkspace()

    # if we have already created intents reload them from source control
    # we are reading from CSV to represent a cleansed source control dataset without tags etc.
    # i.e like a user might have before starting to use humanfirst
    if workspace:
        df_intents = pandas.read_csv(f'./workspaces/{workspace}/{workspace}-intents.csv',delimiter=',',names=['utterance','slash_sep_hier'])
        assert isinstance(df_intents,pandas.DataFrame)
        df_intents.apply(build_intents_from_file, args=[labelled_workspace],axis=1)

    # Liu set has no dateimtes on the data - set everything on one created date
    created_at = datetime.datetime(2022,5,1,0,0,0)
    print(f'All utteraances being treated as if created_at: {created_at}')
    df['created_at'] = created_at

    # roles are all client from HF choices of client|expert
    df['role'] = 'client'

    # create a recreatable conversation_id
    df = df.apply(create_convoid,axis=1)

    # create example for each row.
    df = df.apply(create_example,axis=1,args=[unlabelled_workspace,labelled_workspace])

    # write unlabelled
    with open(f'./data/{name}_unlabelled.json', 'w', encoding='utf8') as file_out:
        unlabelled_workspace.write_json(file_out)

    # write labelled
    with open(f'./data/{name}_labelled.json', 'w', encoding='utf8') as file_out:
        labelled_workspace.write_json(file_out)

def create_convoid(row: pandas.Series):
    # going to use the original full answer
    row['conversation_id'] = humanfirst.hash_string(json.dumps(row.answer),'convo')
    return row

def create_example(row: pandas.Series, unlabelled_workspace: humanfirst.HFWorkspace, labelled_workspace: humanfirst.HFWorkspace) -> list:
    '''parse a single utterance to an example'''

    # HFMetadata just dict[str,all]
    # extract these keys into the metadata dict
    keys_to_extract = ['userid', 'answerid','scenario','intent','status','notes','suggested_entities','question']
    convo_metadata_dict = {}
    for key in keys_to_extract:
        if row[key]:
            convo_metadata_dict[key] = str(row[key])
        else:
            convo_metadata_dict[key] = ''

    # Tags 
    tags=[]
    tags_to_label = ['scenario'] # 'scenario_subflow' - removed as too noisy with speaker role and seq
    for tag in tags_to_label:
        tags.append(unlabelled_workspace.tag(row[tag]))
        labelled_workspace.tag(row[tag])

    # Context with the conversation_id linking utterances (here none just role)
    context = humanfirst.HFContext(row['conversation_id'],'conversation',row['role'])

    # Create the unlablled example without intents
    example = humanfirst.HFExample(text=deannotate(row['answer']),id=humanfirst.hash_string(row['answer'],'example'),created_at=row['created_at'],intents=[],tags=tags,metadata=copy.deepcopy(convo_metadata_dict),context=context)

    # add to the unlabelled_workspace
    unlabelled_workspace.add_example(example)

    # get or create the intent in the labelled_workspace using parent as scenario and child as intent i.e alarm/query, alarm/remove with no metadata
    intents = [labelled_workspace.intent(name_or_hier=[row['scenario'],row['scenario_intent']])]
    deannotated_text = deannotate(row['answer_annotation'])
    labelled_example = humanfirst.HFExample(text=deannotated_text,id=humanfirst.hash_string(deannotated_text,'example'),created_at=row['created_at'],intents=intents)
    labelled_workspace.add_example(labelled_example)

def deannotate(text: str) -> str:
    findalls = re_deannotate.findall(text)       
    output_text = ''
    for f in findalls:
        matches = re_deannotate.search(text)
        assert isinstance(matches, re.Match)
        output_text = output_text + text[0:matches.start()] + matches.group(2)
        text = text[matches.end():]
    output_text = output_text + text
    return output_text

# TODO: move into humanfirst
def build_intents_from_file(row: pandas.Series, labelled_workspace: humanfirst.HFWorkspace):
    hierarchy = str(row['slash_sep_hier']).split('/')
    labelled_workspace.example(row['utterance'],intents=[labelled_workspace.intent(hierarchy)])

if __name__ == '__main__':
    main()