#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80**************************************120
#
# Set of pytest humanfirst.py tests
#
# ***************************************************************************80**************************************120

import humanfirst
import pandas
import json

def test_load_testdata():
    dtypes={
        'external_id': str,
        'timestamp': str,
        'utterance': str,
        'speaker': str,
        'nlu_detected_intent': str,
        'nlu_confidence': str,
        'overall_call_star_rating': int
    }

    # read the input csv
    df = pandas.read_csv('./examples/simple_example.csv',encoding='utf8',dtype=dtypes)
    assert(isinstance(df,pandas.DataFrame))
    assert(df.shape==(5,7))

def test_intent_hierarchy_numbers():
    labelled = humanfirst.HFWorkspace()
    assert(len(labelled.intents)==0)
    # multi hierachy
    intent = labelled.intent(
        name_or_hier=['billing','billing_issues','payment_late']
    )
    assert(isinstance(intent,humanfirst.HFIntent))
    assert(intent.id=='intent-2')
    assert(intent.name=='payment_late')
    assert(intent.parent_intent_id=='intent-1')
    assert(len(labelled.intents)==3)

def test_create_intent_second_time():
    labelled = humanfirst.HFWorkspace()
    intent = labelled.intent(name_or_hier=['billing','billing_issues','payment_late'])
    assert(isinstance(intent,humanfirst.HFIntent))
    assert(intent.name=='payment_late')
    assert(intent.id=='intent-2')
    assert(intent.parent_intent_id=='intent-1')
    assert(len(labelled.intents)==3)
    intent = labelled.intent(name_or_hier=['billing'])
    assert(isinstance(intent,humanfirst.HFIntent))
    assert(intent.name=='billing')
    assert(intent.id=='intent-0')
    assert(intent.parent_intent_id==None)
    assert(len(labelled.intents)==3)


def test_metadata_intent():
    labelled = humanfirst.HFWorkspace()
    metadata = {
        'somekey':'somevalue',
        'anotherkey':'anothervalue'
    }
    intent = labelled.intent(name_or_hier=['billing','billing_issues','payment_late'],metadata=metadata)
    assert(isinstance(intent,humanfirst.HFIntent))
    assert(intent.metadata['anotherkey']=='anothervalue')
    assert(len(labelled.intents)==3)
    # this is the possibly undesirable bit
    assert(labelled.intents['billing'].metadata['anotherkey']=='anothervalue')
    assert(labelled.intents['billing_issues'].metadata['anotherkey']=='anothervalue')
    assert(labelled.intents['payment_late'].metadata['anotherkey']=='anothervalue')

def test_tag_color_create():
    labelled = humanfirst.HFWorkspace()
    tag = labelled.tag(tag='exclude')
    assert(isinstance(tag,humanfirst.HFTag))
    assert(tag.color.startswith('#'))
    assert(len(tag.color)==7)
    old_color=tag.color
    new_color='#ffffff'
    #TODO: if try to recreate, already exists tag doesn't change
    tag = labelled.tag(tag='exclude',color=new_color)
    assert(tag.color==old_color)
    # creating new works
    tag = labelled.tag(tag='exclude-white',color=new_color)
    assert(tag.color==new_color)
    
def test_write_csv():
    workspace = "./examples/write_csv_example.json"
    with open(workspace,mode="r",encoding="utf8") as f:
        data = json.load(f)
    labelled_workspace = humanfirst.HFWorkspace.from_json(data)
    assert(isinstance(labelled_workspace,humanfirst.HFWorkspace))
    output_file = "./examples/write_csv_example.csv"
    labelled_workspace.write_csv(output_file)
    df = pandas.read_csv(output_file,encoding="utf8")
    columns = list(df.columns)
    assert("intent_name" in columns)
    assert("intent_id" in columns)
    assert("text" in columns)
    # checking if the no of examples in workspace and no of rows in csv are same
    # assuming the parent intents don't have any examples
    no_of_parent_intents = df["text"].isna().sum()
    no_of_rows = df.shape[0]
    no_of_examples = len(labelled_workspace.examples)
    assert(no_of_rows - no_of_parent_intents == no_of_examples)