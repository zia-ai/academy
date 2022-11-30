#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80**************************************120
#
# Set of pytest humanfirst.py tests
#
# ***************************************************************************80**************************************120

import humanfirst
import pandas

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
    assert(len(labelled.intents)==3)

def test_create_intent_second_time():
    labelled = humanfirst.HFWorkspace()
    intent = labelled.intent(name_or_hier=['billing','billing_issues','payment_late'])
    assert(isinstance(intent,humanfirst.HFIntent))
    assert(intent.name=='payment_late')
    assert(len(labelled.intents)==3)
    intent = labelled.intent(name_or_hier=['billing'])
    assert(isinstance(intent,humanfirst.HFIntent))
    assert(intent.name=='billing')
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
    
