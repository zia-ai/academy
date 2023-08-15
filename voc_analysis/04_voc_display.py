#!/usr/bin/env python # pylint:disable=missing-module-docstring
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
@click.option('-i', '--input_file', type=str, required=True,
              help='Input File - output from predict script')
@click.option('-o', '--output', type=str, default='',
              help='Output File - .html extension')
@click.option('-r', '--review_col', type=str, required=True,
              help='Review Column name')
@click.option('-g', '--generate', type=int, default='10',
              help='Generate this number')
@click.option('-d', '--document_id_col', type=str, default="Survey ID",
              help='Document id of the review')
@click.option('-t', '--title', type=str, required=True,
              help='Title to be displayed on top of each document')
@click.option('-m', '--match_string', type=str, default='',
              help='Only those documents containing this string should be taken into account')
@click.option('-s', '--style', type=click.Choice(['span', 'ent'], case_sensitive=False), default='ent',
              help='two different annotation styles for displacy')
@click.option('-c', '--confidence', type=float, default=0.35,
              help='Confidecnce clip to label an utterance')
def main(input_file: str, output: str, review_col: str, generate: int,
         document_id_col: str, title: str, match_string: str,
         style: str, confidence: float):
    '''Main function'''

    process(input_file, output, review_col, generate, document_id_col,
            title, match_string, style, confidence)


def process(input_file: str, output: str, review_col: str, generate: int, document_id_col: str,
            title: str, match_string: str, style: str, confidence: float):
    '''Produces HTML file'''

    if not os.path.exists(input_file):
        print("Couldn't find file at the location you provided: {input_file}")
        quit()
    df = pandas.read_csv(input_file, encoding='utf8')
    assert isinstance(df, pandas.DataFrame)

    # converts the type of document id to string in case if it is in int type
    df[document_id_col] = df[document_id_col].astype(str)

    # index by that id, we then have seq?
    df.set_index([document_id_col], drop=True, inplace=True)

    # Takes only those documents that contain the match_string
    if match_string != "":
        df["has_matched_string"] = df[review_col].apply(lambda x: True if re.findall(
            fr"\b{match_string.lower()}\b", x.lower()) else False)
        unmatched_string_ids = list(
            set(df.loc[df["has_matched_string"] == False].index))
        if unmatched_string_ids:
            df.drop(labels=unmatched_string_ids, inplace=True, axis=0)

    # df = df.loc[['1670963083940x254704074734388700']]

    # randomly generate samples
    samplings_ids = list(set(df.index))
    if len(samplings_ids) > generate:
        sampling_ids = random.sample(samplings_ids, generate)
        df = df[df.index.isin(sampling_ids)]
    assert isinstance(df, pandas.DataFrame)

    print(df['utterance'].groupby(level=0).count())

    # sorted lists of all the IDs?  Why not apply
    ids = sorted(list(set(df.index)))

    # why large model?
    nlp = spacy.load("en_core_web_lg")
    docs = []
    names = set()
    colors = {}

    # ok so iterate through data frame by loc - because we want the whole set
    for docid in ids:

        # list within columns forces dataframe
        df_doc = df.loc[[docid]]
        assert isinstance(df_doc, pandas.DataFrame)
        print(f'Working on Document id: {docid} has shape{df_doc.shape}')

        # every utterance line repeats the review take the first
        review = df_doc[review_col][0]
        sentences = df_doc["utterance"].to_list()
        confidences = df_doc["confidence"].to_list()
        intent_names = df_doc["fully_qualified_intent_name"].to_list()

        # print(review)
        doc = nlp(review)
        assert isinstance(doc, spacy.tokens.doc.Doc)

        doc.user_data["title"] = f"{title} {docid}"

        show_tokens = {}
        for i, t in enumerate(doc):
            show_tokens[i] = t
        # print(show_tokens)

        spans = []
        token_pointer = 0
        for i, sent in enumerate(sentences):

            # print(f'\nsentence: {i} - {sent}')
            matcher = PhraseMatcher(nlp.vocab)

            if confidences[i] < confidence:
                print(
                    f"Confidence: {confidences[i]} lower than the threshold: {confidence}")
                continue

            matcher.add(intent_names[i], [nlp(sent)])
            matches = matcher(doc)
            if matches:
                for j in range(len(matches)):
                    # print('Have matches')
                    # print(len(matches))
                    # print(f'tokens: {len(show_tokens)}')
                    # print(f'token_pointer: {token_pointer}')
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
                        colors[intent_names[i]] = '#' + \
                            ''.join([random.choice('0123456789ABCDEF')
                                    for j in range(6)])
                        print(
                            f'Random color generated for {str(intent_names[i])}')
                        print(colors)
                    token_pointer = end

        doc.spans["sc"] = spans
        doc.ents = spans
        docs.append(doc)

    print(f'Have processed {len(docs)} docs')

    # Create a html file
    if output == '':
        if match_string != '':
            file_out_name = f"{input_file.split('.csv')[0]}_{len(ids)}_{match_string}.html"
        else:
            file_out_name = f"{input_file.split('.csv')[0]}_{len(ids)}.html"

    else:
        file_out_name = output

    with open(file_out_name, 'w', encoding='utf8') as file_out:
        file_out.write(displacy.render(docs, style=style, options={
                       "ents": list(colors), "colors": colors}, page=True))
        print(file_out_name)


def build_title(row: pandas.Series, title: str) -> str:
    '''Construct a nice title for the display format'''

    return f'{title} {row.name}'


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
