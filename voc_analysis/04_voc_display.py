#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#  
# python3 ./voc_analysis/04_voc_display.py -i ./data/voc.csv 
#         -r <review col name> 
#         -g 10
#         -t <title to be diplayed on top of each doc>
#         -l <background filter - filters seniors, disablled, and parents>
#         -m <match string - Considers documents with specific strings in them>
#
# *****************************************************************************

# standard imports
import random
import re
import os

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

@click.command()
@click.option('-i', '--input', type=str, required=True,help='Input File')
@click.option('-o', '--output', type=str, default='',help='Output File - .html extension')
@click.option('-r', '--review_col', type=str, required=True, help='Review Column name')
@click.option('-g', '--generate', type=int, default='10', help='Generate this number')
@click.option('-d', '--document_id_col', type=str, default="Survey ID", help='Document id of the review')
@click.option('-t', '--title', type=str, required=True, help='Title to be displayed on top of each document')
@click.option('-m', '--match_string', type=str, default='', help='Only those documents containing this string should be taken into account')
@click.option('-l','--background', is_flag=True, default=False, help='Filter documents given only by seniors, families and disabled persons')
@click.option('-s', '--style', type=click.Choice(['span', 'ent'], case_sensitive=False), default='ent',help='two different annotation styles for displacy')
@click.option('-c', '--confidence', type=float, default=0.35, help='Confidecnce clip to label an utterance')

def main(input: str, output: str, review_col: str, generate: int, document_id_col: str, title: str, match_string: str, background: bool, style: str, confidence: float):
    '''Main function'''

    process(input, output, review_col, generate, document_id_col, title, match_string, background, style, confidence)
    
def process(input: str, output: str, review_col: str, generate: int, document_id_col: str, title: str, match_string: str, background: bool, style: str, confidence: float):
    '''Produces HTML file'''

    if not os.path.exists(input):
        print("Couldn't find file at the location you provided: {input}")
        quit()
    df = pandas.read_csv(input,encoding='utf8')
    assert isinstance(df,pandas.DataFrame)

    # converts the type of document id to string in case if it is in int type
    df[document_id_col] = df[document_id_col].astype(str)

    df.set_index([document_id_col],drop=True,inplace=True)

    docs_background_ids=[]
    # Filter documents given only by seniors, families and disabled persons
    if background:

        # remove all the document that contains only background-others intent
        docs_background_ids = list(set(df.loc[~((df["background-seniors"]==True) | (df["background-parents_and_families"]==True) | (df["background-disabled_illness_condition"]==True))].index))
        df.drop(labels=docs_background_ids,inplace=True,axis=0)
        
        # confidence clip for background
        docs_background_ids = list(set(df.index))
        ids_below_confidence_threshold_for_background = []
        for id in docs_background_ids:
            df_doc = df.loc[id]
            if isinstance(df_doc["fully_qualified_intent_name"],str):
                df_doc_intents = [df_doc["fully_qualified_intent_name"]]
                df_doc_confidence = [df_doc["confidence"]]
            else:
                df_doc_intents = df_doc["fully_qualified_intent_name"].tolist()
                df_doc_confidence = df_doc["confidence"].tolist()
            
            below_threshold_count = 0
            no_of_background = 0
            for i, intent_name in enumerate(df_doc_intents):
                if intent_name in ["background-seniors","background-parents_and_families","background-disabled_illness_condition"]:
                    no_of_background = no_of_background + 1
                    if df_doc_confidence[i] < confidence:
                        below_threshold_count = below_threshold_count + 1
            
            if no_of_background == below_threshold_count:
                ids_below_confidence_threshold_for_background.append(id)
        
        df.drop(labels=ids_below_confidence_threshold_for_background,inplace=True,axis=0)        

    # Takes only those documents that contain the match_string
    if match_string != "":
        df["has_matched_string"] = df[review_col].apply(lambda x: True if re.findall(fr"\b{match_string.lower()}\b",x.lower()) else False)
        unmatched_string_ids =list(set(df.loc[df["has_matched_string"]==False].index))
        if unmatched_string_ids:
            df.drop(labels=unmatched_string_ids,inplace=True,axis=0)

    # randomly generate samples
    samplings_ids = list(set(df.index))
    if len(samplings_ids) > generate:
        sampling_ids = random.sample(samplings_ids,generate)
        df = df[df.index.isin(sampling_ids)]
    assert isinstance(df,pandas.DataFrame)

    ids = sorted(list(set(df.index)))
    nlp = spacy.load("en_core_web_lg")
    docs = []
    names = set()
    colors = {}
    for d in ids:

        df_doc = df.loc[d]
        print(f'Working on Document id: {d}')

        if isinstance(df_doc[review_col],str):
            review = df_doc[review_col]
            sentences = [df_doc["utterance"]]
            confidences = [df_doc["confidence"]]
            intent_names = [df_doc["fully_qualified_intent_name"]]
        else:
            review = df_doc[review_col].unique()[0]
            sentences = df_doc["utterance"].to_list()
            confidences = df_doc["confidence"].to_list()
            intent_names = df_doc["fully_qualified_intent_name"].to_list()

        doc = nlp(review)
        assert isinstance(doc,spacy.tokens.doc.Doc)

        doc.user_data["title"] = f"{title} {d}"

        show_tokens = {}
        for i,t in enumerate(doc):
            show_tokens[i]=t
        print(show_tokens)

        spans = []
        token_pointer = 0
        for i, sent in enumerate(sentences):
            print(f'sentence: {i} - {sent}')
            matcher = PhraseMatcher(nlp.vocab)
            
            if confidences[i] < confidence:
                print(f"Neglecting this sentence since predicted confidence {confidences[i]} is lower than the threshold {confidence}")
                continue

            matcher.add(intent_names[i], [nlp(sent)])
            matches = matcher(doc)
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
                    print(f'start: {start} end: {end} name:{intent_names[i]}')
                    span = Span(doc, start, end, intent_names[i])
                    
                    names.add(intent_names[i])
                    spans.append(span)

                    # give the label a color if it doesn't have one
                    if intent_names[i] in colors.keys():
                        print('Color assigned')
                    else:
                        colors[intent_names[i]] = '#' + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
                        print(f'Random color generated for {str(intent_names[i])}')
                        print(colors)
                    token_pointer = end

        doc.spans["sc"] = spans
        doc.ents = spans
        docs.append(doc)
    
    print(f'Have processed {len(docs)} docs')

    # Create a html file
    if output == '':
        if match_string != '':
            file_out_name = f"{input.split('.csv')[0]}_{len(ids)}_{match_string}.html"
        else:
            file_out_name = f"{input.split('.csv')[0]}_{len(ids)}.html"

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
