#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80**************************************120
#
# Set of pytest df_es_tagging.py tests
#
# ***************************************************************************80**************************************120

import df_es_tagging
import pandas


def setup():
    config = df_es_tagging.validate_agent_directory("./workspaces/df/es/priorities")
    intents = df_es_tagging.load_intents(config["intent_dir"])
    return pandas.json_normalize(intents)

def test_df_shape():
    df = setup()
    assert(isinstance(df,pandas.DataFrame))
    assert(df.shape[0]==12)
    assert(df.shape[1]==15)
    columns = df.columns.to_list() 
    assert(columns == ['id', 'name', 'auto', 'contexts', 'responses', 'priority',
       'webhookUsed', 'webhookForSlotFilling', 'fallbackIntent', 'events',
       'conditionalResponses', 'condition', 'conditionalFollowupEvents',
       'parentId', 'rootParentId']
    )

def test_intent_types():
    df = setup()
    df = df_es_tagging.process_intent_types(df)
    intent_types = df[["intent_type","name"]].groupby("intent_type").count()
    print(intent_types)
    assert(intent_types.loc["context","name"]==2)
    assert(intent_types.loc["follow_on","name"]==2)
    assert(intent_types.loc["top_level","name"]==8)

def test_priorities():
    df = setup()
    df = df_es_tagging.process_priorities(df)
    priorities = df[["priority","name"]].groupby("priority").count()
    print(priorities)
    assert(priorities.loc["highest","name"]==1)
    assert(priorities.loc["high","name"]==1)
    assert(priorities.loc["normal","name"]==8)
    assert(priorities.loc["low","name"]==1)
    assert(priorities.loc["ignore","name"]==1)
    

