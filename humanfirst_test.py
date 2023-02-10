#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80**************************************120
#
# Set of pytest humanfirst.py tests
#
# ***************************************************************************80**************************************120

import os
import humanfirst
import pandas
import json
import numpy

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
    # delete output file so can sure we are testing fresh each time
    if os.path.exists("./examples/write_csv_example.csv"):
        os.remove("./examples/write_csv_example.csv")
    workspace = "./examples/write_csv_example.json"
    
    with open(workspace,mode="r",encoding="utf8") as f:
        data = json.load(f)
    labelled_workspace = humanfirst.HFWorkspace.from_json(data)
    assert(isinstance(labelled_workspace,humanfirst.HFWorkspace))
    output_file = "./examples/write_csv_example.csv"
    labelled_workspace.write_csv(output_file)
    df = pandas.read_csv(output_file,encoding="utf8")
    
    # Check column names
    columns_should_be = []
    # utterance and full name
    columns_should_be.extend(["utterance","fully_qualified_intent_name"]) 
    # four different intent keys
    columns_should_be.extend(["intent_metadata-intent_metadata1","intent_metadata-intent_metadata2","intent_metadata-intent_metadata3","intent_metadata-intent_metadata4"])
    # two different metadata keys
    columns_should_be.extend(["example_metadata-example_metadata1","example_metadata-example_metadata2"])
    columns_should_be.sort()
    
    columns = list(df.columns)
    columns.sort()
    
    assert(columns==columns_should_be)
    
    # Check intent level values
    assert(list(df["intent_metadata-intent_metadata1"].unique())==[numpy.nan,'value1','value5'])
    assert(df[df["intent_metadata-intent_metadata1"]=='value1'].shape[0]==5)
    assert(df[df["intent_metadata-intent_metadata1"]=='value5'].shape[0]==1)
    assert(df[df["intent_metadata-intent_metadata1"].isna()].shape[0]==5)
    
    assert(list(df["intent_metadata-intent_metadata2"].unique())==[numpy.nan,'value2','value6'])
    assert(df[df["intent_metadata-intent_metadata2"]=='value2'].shape[0]==5)
    assert(df[df["intent_metadata-intent_metadata2"]=='value6'].shape[0]==1)
    assert(df[df["intent_metadata-intent_metadata2"].isna()].shape[0]==5)
    
    # Check example level values
    assert(list(df["example_metadata-example_metadata1"].unique())==['valueA',numpy.nan])
    assert(df[df["example_metadata-example_metadata1"]=='valueA'].shape[0]==1)
    assert(df[df["example_metadata-example_metadata1"].isna()].shape[0]==10)

    

    