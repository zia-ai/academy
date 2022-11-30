#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python word_weight.py 
#    -input       "If I'm late with payment will I still get premium membership gold benefits?" 
#    --playbook   <playbook-J227FZ54QFC2TAHY3B....>  check the url for your studio (make sure NLU is trained)
#    --username   <xyz@wherever.com>
#    --password   <your password>
#    --namespace  <your namespace  
#    --comparions <n> 1-3 typically
# 
# Optional 
#     --output    <directory path>    # harden analysis to CSV
#     --display                       # product a visualisation
#
# Produces for a given utterance for the first n predicted intents by the classifier
# a) Prediction Significance 
#    (for each token if it is removed how signficantly does the prediction reduce for the intent)
# b) TF-IDF - Term Frequency - Inverse Document Frequency 
#    So a metric of how often the token appears in each of the intent training compared to other intents
#    Treating the intent training for each intent as a document
#    How many times does the token appear in the document as a proporition of the total tokens in the document
#    Multiplied by the log of the total number of documents over the number of documents the term appears in.
#
# Doesn't do any lemmatisation, stemming, punctuation removal, stop word removal, case removal 
# which would have been common when using word level statistics or embedding.
# These could be added depending on your NLU approach and underlyling unit of NLU processing.
# This just looks at each token's significance on prediction and occurance (based on Spacy tokenization) 
# 
# *****************************************************************************

# standard imports
import requests
import json
import base64 
import math
import os
import copy

# third party imports
import pandas
import click
import spacy
from spacy.tokens import Span
from spacy import displacy
from dframcy import DframCy
import seaborn


@click.command()
@click.option('-i', '--input', type=str, required=True, help='Input utterance')
@click.option('-u', '--username', type=str, default='Blah', help='HumanFirst username')
@click.option('-p', '--password', type=str, default='Blah', help='HumanFirst password')
@click.option('-n', '--namespace', type=str, default='Blah', help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, default='Blah', help='HumanFirst playbook id')
@click.option('-d', '--display', is_flag=True, default=False, help='Whether to display in Displacy or not')
@click.option('-c', '--comparisons', type=int, required=True, help='How many of the top x intents to compare for the utterance')
@click.option('-o', '--output', type=str, default='', help='Optional directory to output CSVs to')
def main(input: str, username: str, password: int, namespace: bool, playbook: str, display: str, comparisons: int, output: str):

    # determine which intents to analyse
    headers = authorize(username, password)

    # get the workspace/playbook
    workspace_dict = get_workspace(headers, namespace, playbook)

    # names of the intent ids with fully qualified path
    intents_dict = extract_intents_dict(workspace_dict)

    # all the intents to produce frequency statistics.
    df_intents = extract_examples_df(workspace_dict, intents_dict)
    
    # get what the top intents are for the provided utterance
    response_dict = predict(headers, input, namespace, playbook)
    intents = get_top_n_intents(comparisons, response_dict, intents_dict)
    print('')
    print(f'Producing comparison for {",".join(intents)}')
    

    # tokenize all the intents and calculate various frequency metrics
    nlp = spacy.load("en_core_web_md")
    df_tfidf = calc_tfidf(df_intents,nlp)

    # analyse each of the top n intents for the utterance.
    docs = []
    labels = []
    colors = []
    dfs = []
    for intent in intents:
        print('')
        print(f'Begin {intent}')
        # This script rather grew, so return form this is rather messsy TODO tidy.
        doc_label_color_df_doc2_lab2_col2 = process(headers, input, namespace, playbook, intent, df_tfidf)

        # Spacy doc and labels for the Prediction Significance visualisation
        docs.append(doc_label_color_df_doc2_lab2_col2[0])
        labels.extend(doc_label_color_df_doc2_lab2_col2[1])
        colors.extend(doc_label_color_df_doc2_lab2_col2[2])

        # DFs supporting the analysis
        dfs.append(doc_label_color_df_doc2_lab2_col2[3])

        # Spacy docs and labels for the tf-idf visualisation
        docs.append(doc_label_color_df_doc2_lab2_col2[4])
        labels.extend(doc_label_color_df_doc2_lab2_col2[5])
        colors.extend(doc_label_color_df_doc2_lab2_col2[6])

    # turn colors and labels into a lookup for spacy.
    color_lookup = dict(zip(labels, colors))

    # if we want an output write it to csv in provided directory
    if output != '':
        assert(os.path.isdir(output))
        if not output.endswith('/'):
            output = output + '/'
        for i, intent in enumerate(intents):
            assert(isinstance(intent, str))
            df_output = dfs[i]
            assert(isinstance(df_output,pandas.DataFrame))
            for key in ['token_score','tf','df','idf','tfidf']:
                assert(isinstance(df_output,pandas.DataFrame))
                df_output[key] = df_output[key].round(5)
            keys = ["token_text","token_start","token_end","test_utterance","utt_score",\
                    "score","token_score","label","intent","color","count_tokens","total_tokens",\
                    "tf","count_intents_in","df","idf","tfidf"]
            df_output[keys].to_csv(f'{output}{intent.replace("/","--")}.csv',mode='w',index=False)

    # display if required (otherwise just output the DF score results)
    if display:
        displacy.serve(docs, style='ent', options={"ents": labels, "colors": color_lookup})


def calc_tfidf(df_intents: pandas.DataFrame, nlp: spacy.language.Language) -> pandas.DataFrame:
    '''Treats each set of training for an intent as a document
    For that document then calculates
    * term frequency (tf)
    * document frequency (df)
    * inverse document frequency (idf)
    * tf-idf
    
    '''
    # tokenize and explode by token
    df_intents['tokens'] = df_intents['utterance'].apply(tokenize,args=[nlp])
    df_intents = df_intents.explode(['tokens'])

    # count each tokens by intent
    df_count_tokens = df_intents.groupby(['intent','tokens']).count()
    df_count_tokens = df_count_tokens.rename(columns={'utterance':'count_tokens'})

    # count total tokens by intent
    df_total_tokens = df_intents[['intent','utterance']].groupby(['intent']).count()
    df_total_tokens = df_total_tokens.rename(columns={'utterance':'total_tokens'})
        
    # calculate term frequency tf
    df_tf = df_count_tokens.join(df_total_tokens)
    df_tf['tf'] = df_tf['count_tokens'] / df_tf['total_tokens']
    
    # calculate doc frequency

    # for each token the documents it appears in.
    # Group multiple occurances of a token across utterances
    df_docs = df_intents[['tokens','intent','utterance']].groupby(['tokens','intent']).count()
    df_docs = df_docs.reset_index(drop=False)
    # Group that group to count the documents then the token appears in
    df_docs = df_docs[['tokens','intent']].groupby(['tokens']).count()
    df_docs = df_docs.rename(columns={'intent':'count_intents_in'})

    # total nubmer of documents
    total_docs = df_intents['intent'].nunique()

    # Calculate stats
    df_docs['df'] = total_docs / df_docs['count_intents_in']
    df_docs['idf'] = df_docs['df'].apply(calc_log)

    # join document information to term information
    df_tfidf = df_tf.join(df_docs,on='tokens')
    df_tfidf['tfidf'] = df_tfidf['tf'] * df_tfidf['idf']   

    return df_tfidf

def calc_log(n: float) -> float:
    '''Calculate log of passed float'''
    return math.log(n)

def get_top_n_intents(comparison: int, response_dict: dict, intents_dict: dict) -> list:
    '''Find the top n intents from the prediciton to do word weight analysis on'''
    intents = []
    for i in range(comparison):
        id = response_dict['matches'][i]['id']
        intents.append(intents_dict[id]['fullpath'])
    return intents

def process(headers: dict, input: str, namespace: bool, playbook: str, target: str, df_tfidf: pandas.DataFrame):
    '''Calculate prediction significance for an utterance'''

    # build a dataframe of the tokens in the utterance
    nlp = spacy.load("en_core_web_md")
    dframcy = DframCy(nlp)
    doc = dframcy.nlp(input)
    # "pos_", "tag_", "dep_", "head", "ent_type_", "lemma_", "lower_", "is_punct", "is_quote", "is_digit"]
    columns = ["id", "text", "start", "end", ]
    df = dframcy.to_dataframe(doc, columns=columns)

    # for each assemble a test utterance leaving out the individual token
    df = df.apply(create_test_utterance, args=[input], axis=1)

    # get the score for the utterance without ommision
    # could avoid this call by doing earlier
    utt_score = extract_score(
        predict(headers, input, namespace, playbook), target.split('/')[-1]) # TODO better qualified path in case two intents identically named
    df['utt_score'] = utt_score

    # get the scores for all the utterances with the token ommitted
    df['response_dict'] = batchPredict(
        headers, df['test_utterance'].to_list(), namespace, playbook)
    df['score'] = df['response_dict'].apply(extract_score, args=[target.split('/')[-1]]) # TODO better qualified path in case two intents identically named

    # give the token a score of the difference between the utterance in full and the utterance with the token removed nd create a string label
    # this is a fairly simple prediction significance estimate
    # Other things like LIME are available, further reading: https://arxiv.org/pdf/1602.04938.pdf
    df['intent'] = target
    df['token_score'] = df['utt_score'] - df['score']
    df['label'] = df['token_score'].apply(label_pad)

    # Produce tdidf labels and join to each intent and token.
    df = df.join(df_tfidf, on=['intent','token_text'])
    for key in ['tf','df','idf','tfidf']:
        df[key] = df[key].fillna(0)
    df['presence_label'] = df['tfidf'].apply(pad_tf)
    print(f'input: {input}')
    
    # create the doc for display
    doc = nlp(doc)

    # get a gradient of colour away from zero
    cp_plus = list(seaborn.color_palette("light:g", 100).as_hex())
    cp_neg = list(seaborn.color_palette("light:r", 100).as_hex())
    cp_presence = list(seaborn.color_palette("light:b", 100).as_hex())

    # create the spans as both ent and spans for different display styles
    df = df.apply(create_span, args=[cp_plus, cp_neg, cp_presence, doc], axis=1)
   
    # create predictive significance documents
    doc.spans["sc"] = df['span'].to_list()
    doc.ents = df['span'].to_list()

    # create separate tdidf documents
    doc2 = copy.deepcopy(doc)
    presence_spans = []
    for span in list(df['presence_span']):
        if not span == None:
            presence_spans.append(span)
    doc2.spans["sc"] = presence_spans
    doc2.ents = presence_spans

    # create a title for each intent analysed
    first_part = f'{utt_score:.2f} #{target}'
    pad_mid = ''
    for i in range(80 - len(first_part)):
        pad_mid = pad_mid + '_'
    doc.user_data["title"] =  f'{first_part} {pad_mid} (Prediction Signficance)'
    doc2.user_data["title"] = f'{first_part} {pad_mid} (Tf-idf)'

    # Return all the stuff built TODO tidy functionisation
    return (
        doc, 
        df['label'].to_list(), 
        df['color'].to_list(), 
        df, 
        doc2, 
        df['presence_label'].to_list(),
        df['presence_color'].to_list()
    )

def label_pad(token_score: float) -> str:
    '''Formating of the word labels for the prediction signficance'''
    if token_score >= 0:
        return f'+{token_score:.2f}'
    else:
        return f'{token_score:.2f}'

def pad_tf(tf: float) -> str:
    '''Formating of the word labels for the frequency metric'''
    if tf == 0:
        return f'_{tf:.2f}'
    else:
        return f'_{tf:.2f}'

def tokenize(utterance: str, nlp) -> list:
    '''Spacy tokenisation'''
    doc = nlp(utterance)
    tokens = []
    for t in doc:
        tokens.append(t.text)
    return tokens

def extract_score(response_dict: dict, target_intent: str) -> int:
    '''Extract the score for a certain intent from the lowest match level from predict json'''
    if 'matches' in response_dict.keys():
        for match in response_dict['matches']:
            if match['name'] == target_intent:
                score = round(match['score'], 2)
                return score
    return 0


def authorize(username: str, password: str) -> dict:
    '''Get bearer token for a username and password'''

    print(f'Hello {username} getting auth token details')

    auth_url = 'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyA5xZ7WCkI6X1Q2yzWHUrc70OXH5iCp7-c'
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json'
    }
    auth_body = {
        "email": username,
        "password": password,
        "returnSecureToken": True
    }
    auth_response = requests.request(
        "POST", auth_url, headers=headers, data=json.dumps(auth_body))
    if auth_response.status_code != 200:
        raise Exception(
            f'Not authorised, google returned {auth_response.status_code} {auth_response.json()}')
    idToken = auth_response.json()['idToken']
    headers['Authorization'] = f'Bearer {idToken}'
    print('Retrieved idToken and added to headers')
    return headers


def predict(headers: str, sentence: str, namespace: str, playbook: str) -> dict:
    '''Get response_dict of matches and hier matches for an input'''
    payload = {
        "namespace": "string",
        "playbook_id": "string",
        "input_utterance": sentence
    }

    url = f'https://api.humanfirst.ai/v1alpha1/nlu/predict/{namespace}/{playbook}'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response - predict")
        print(response.status_code)
        quit()
    response_dict = response.json()
    return response_dict


def batchPredict(headers: str, sentences: list, namespace: str, playbook: str) -> dict:
    '''Get response_dict of matches and hier matches for a batch of sentences'''
    print(f'Batch Prdiction on {len(sentences)} different variations of the input utterance')
    payload = {
        "namespace": "string",
        "playbook_id": "string",
        "input_utterances": sentences
    }

    url = f'https://api.humanfirst.ai/v1alpha1/nlu/predict/{namespace}/{playbook}/batch'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response - batch predict")
        print(response.status_code)
        quit()
    response_dict = response.json()['predictions']
    return response_dict


def get_workspace(headers: str, namespace: str, playbook: str) -> pandas.DataFrame:
    '''Download the intents for a workspace'''
    payload = {
        "namespace": namespace,
        "playbook_id": playbook,
        "format": 7,  # json
        "format_options": {
            "hierarchical_intent_name_disabled": False,
            "hierarchical_delimiter": "/",  # standard suggested delimiter
            "zip_encoding": False,
            "hierarchical_follow_up": False,
            "include_negative_phrases": False
        }
        # "intent_ids": [] # get everything then filter?
    }

    url = f'https://api.humanfirst.ai/v1alpha1/workspaces/{namespace}/{playbook}/intents/export'

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print("Did not get a 200 response - retrieve intents")
        print(response.status_code)
        print(response.content)
        quit()
    json_str = base64.b64decode(response.json()['data']).decode('utf8')
    workspace_dict = json.loads(json_str)
    return workspace_dict

def extract_intents_dict(workspace_dict: dict) -> dict:
    ''' Return a dict lookup of intent id to fullpath intent hierarchy'''
    # build full path of intent names
    delim = '/'
    intents = workspace_dict['intents']
    intents_dict = {}
    for intent in intents:
        intents_dict[intent['id']] = {'name':intent['name']}
    for intent in intents:
        fullpath = intent['name']
        working = intent
        while 'parent_intent_id' in working:
            fullpath = f'{intents_dict[working["parent_intent_id"]]["name"]}{delim}{fullpath}'
            working = intents_dict[working["parent_intent_id"]]
        intents_dict[intent['id']]['fullpath'] = fullpath
    return intents_dict

def extract_examples_df(workspace_dict: dict, intents_dict: dict) -> pandas.DataFrame:
    '''Return a DataFrame of all the training examples in the downloaded workspace
       Use the fullpath intent hierachcy
    '''
    # lookup the examples
    examples = workspace_dict['examples']
    intent_labels = []
    example_data = []
    for example in examples:
        example_data.append(example['text'])
        intent_labels.append(intents_dict[example['intents'][0]['intent_id']]['fullpath'])

    # zip the result into a dataframe
    df = pandas.DataFrame(zip(example_data, intent_labels),columns=['utterance','intent'])
    return df

def create_test_utterance(row: pandas.DataFrame, input: str):
    '''Mask token and produce new utterance to test prediction signifcance'''
    row['test_utterance'] = input[:row.token_start] + input[row.token_end:]
    return row


def create_span(row: pandas.DataFrame, cp_plus: list, cp_neg: list, cp_presence: list, doc):
    '''
    Create span and asign a color with a positive and negative colour gradient away from zero for prediction signficance
    Create a second span for the tdidf with a third colour increasing from zero
    Create white/transparent colors for 0 values
    '''
    
    # Predicition signficance bi directional gradient
    row['span'] = Span(doc=doc, start=row.name,
                       end=row.name+1, label=row.label)
    index = int(row.token_score*100)
    if index > 0:
        row['color'] = cp_plus[index]
    elif index == 0:
        row['color'] = '#ffffff'
    else:
        row['color'] = cp_neg[index*-1]
    
    # term frequncy mono directional
    row['presence_span'] = Span(doc=doc, start=row.name,end=row.name+1, label=row.presence_label)
    index = int(round(row.tfidf,2)*100)
    if index > 0:
        row['presence_color'] = cp_presence[index]
    else:
        row['presence_color'] = '#ffffff'
   
    return row

if __name__ == '__main__':
    main()
