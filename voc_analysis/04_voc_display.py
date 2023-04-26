#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# Reloads a random number (-g) of surveys and then calls HF batchPredict
# to label them and displays in displacy
#  
# python3 ./voc_analysis/04_voc_display.py -i ./data/voc.csv 
#         -r <review col name> 
#         -g 10
#         -u <hf username> 
#         -p <hf password> 
#         -n <namepsace> 
#         -b <playbook id>
#         -t <title to be diplayed on top of each doc>
#
# *****************************************************************************

# standard imports
import random

# custom imports
import spacy
import click
import pandas
from spacy.matcher import PhraseMatcher
from spacy.tokens import Span
from spacy import displacy
import nltk
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

# third party imports
import voc_helper
import humanfirst_apis


@click.command()
@click.option('-i', '--input', type=str, required=True,help='Input File')
@click.option('-o', '--output', type=str, default='',help='Output File - .html extension')
@click.option('-r', '--review_col', type=str, required=True, help='Review Column name')
@click.option('-g', '--generate', type=int, default='10', help='Generate this number')
@click.option('-k', '--document_id', type=str, default="", help='Specific Document ID')
@click.option('-d', '--document_id_col', type=str, default="Survey ID", help='Document id of the review')
@click.option('-t', '--title', type=str, required=True, help='Title to be displayed on top of each document')
@click.option('-u', '--username', type=str, required=True, help='HumanFirst username')
@click.option('-p', '--password', type=str, required=True, help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id of format playbook-<GUID>')
@click.option('-s', '--style', type=click.Choice(['span', 'ent'], case_sensitive=False), default='ent',help='two different annotation styles for displacy')
@click.option('-c', '--confidence', type=float, default=0.35, help='Confidecnce clip to label an utterance')

def main(input: str, output: str, review_col: str, generate: int, document_id: str, document_id_col: str, title: str, username: str, password: str, style: str, namespace: str, playbook: str, confidence: float):
    '''Main function'''

    process(input, output, review_col, generate, document_id, document_id_col, title, username, password, style, namespace, playbook, confidence)
    
def process(input: str, output: str, review_col: str, generate: int, document_id: str, document_id_col: str, title: str, username: str, password: str, style: str, namespace: str, playbook: str, confidence: float):

    # get headeres with authorization token in.
    headers = humanfirst_apis.process_auth(username=username, password=password)

    # load from csv to pandas
    df = voc_helper.get_df_from_input(input,review_col)

    df[document_id_col] = df[document_id_col].astype(str)
    df.set_index([document_id_col],drop=True,inplace=True)
    
    if document_id == "":
        if df.shape[0] >= generate:
            df = df.sample(generate)
        else:
            print(f"Given number of samples {generate} is greater than number of non-empty documents ({df.shape[0]}) available")
            quit()
    else:
        df = df.loc[[document_id]]

    assert isinstance(df,pandas.DataFrame)

    ids = list(df.index)
    pt = nltk.tokenize.PunktSentenceTokenizer()

    nlp = spacy.load("en_core_web_lg")
    docs = []
    names = set()
    colors = {}
    print(ids)
    for d in ids:
        row = df.loc[d]
        print(f'Working on Document id: {d}')
        print(row[review_col])

        sentences = pt.tokenize(row[review_col])
        
        response_dict = humanfirst_apis.batchPredict(headers=headers,sentences=sentences,namespace=namespace,playbook=playbook)

        doc = nlp(row[review_col])
        assert isinstance(doc,spacy.tokens.doc.Doc)
        doc.user_data["title"] = build_title(row, title)
        show_tokens = {}
        for i,t in enumerate(doc):
            show_tokens[i]=t
        print(show_tokens)

        spans = []
        token_pointer = 0
        for i, sent in enumerate(sentences):
            print(f'sentence: {i} - {sent}')
            matcher = PhraseMatcher(nlp.vocab)

            hierarchy =  response_dict[i]['matches'][0]['hierarchyNames']
            intent_name = f"#{hierarchy[0]}"
            for index in range(1,len(hierarchy)):
                intent_name = f'{intent_name}-{hierarchy[index]}'
            if response_dict[i]['matches'][0]['score'] < confidence:
                print(f"Neglecting this sentence since predicted confidence {response_dict[i]['matches'][0]['score']} is lower than the threshold {confidence}")
                continue

            matcher.add(intent_name, [nlp(sent)])
            matches = matcher(doc)
            print(matches)
            if matches:
                for j in range(len(matches)):
                    print('Have matches')
                    print(len(matches))
                    print(f'tokens: {len(show_tokens)}')
                    print(f'token_pointer: {token_pointer}')
                    start = matches[j][1]
                    end = matches[j][2]
                    if start < token_pointer:
                        continue
                    print(f'start: {start} end: {end} name:{intent_name}')
                    span = Span(doc, start, end, intent_name)
                    
                    names.add(intent_name)
                    spans.append(span)

                    # give the label a color if it doesn't have one
                    if intent_name in colors.keys():
                        print('Color assigned')
                    else:
                        colors[intent_name] = '#' + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
                        print(f'Random color generated for {str(intent_name)}')
                        print(colors)
                    token_pointer = end

        doc.spans["sc"] = spans
        doc.ents = spans
        docs.append(doc)
    
    print(f'Have processed {len(docs)} docs')

    # Create a html file
    if output == '':
        file_out_name = f"{input.split('.csv')[0]}-{generate}-docs.html"
    else:
        file_out_name = output

    with open(file_out_name,'w',encoding='utf8') as file_out:
        file_out.write(displacy.render(docs, style=style, options={"ents": list(colors),"colors": colors},page=True))
        print(file_out_name)

def build_title(row: pandas.Series, title: str) -> str:
    '''Construct a nice title for the display format'''

    return f'{title} {row.name}'   

if __name__ == '__main__':
    main()
