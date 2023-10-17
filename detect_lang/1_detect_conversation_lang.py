#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *********************************************************************************************************************
#
# python 1_detect_conversation_lang.py
# This script uses spacy large model to detect languages of conversations
# Google translator was taking more time compared to spacy while handling large datasets
#
# This script only detects language of an entire conversation
#
# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import pandas
import click
from spacy_langdetect import LanguageDetector
import spacy
from spacy.language import Language

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input HF JSON File Path')
def main(filename: str) -> None:
    """Main Function"""

    input_filepath = filename

    # load input data
    with open(input_filepath, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-",)

    # enforce id is string
    df["context-context_id"] = df["context-context_id"].astype(str)

    # give a sequence number to each utterance
    df = df.sort_values(["context-context_id", "created_at"])
    df['seq'] = df.groupby("context-context_id").cumcount()

    print(f"Total number of conversations: {len(df['context-context_id'].unique())}")

    # set context-context_id and seq as index
    df.set_index(["context-context_id", "seq"], drop=True, inplace=True)

    # detect language of utterances
    nlp_model = spacy.load("en_core_web_lg")
    # this helps in processing large text upto 1140000 characters
    nlp_model.max_length = 1140000
    Language.factory("language_detector", func=get_lang_detector)
    nlp_model.add_pipe('language_detector', last=True)

    df_convo = df.groupby('context-context_id')['text'].agg(lambda x: ' '.join(x)) # pylint:disable=unnecessary-lambda
    df_convo = pandas.DataFrame(df_convo)
    df_convo["lang"] = df_convo["text"].apply(detect_lang, args=[nlp_model])
    
    print(f"Languages detected:{df_convo['lang'].unique()}")
    print(df_convo.groupby(["lang"]).count())

    file_output = filename.replace(".json","_languages.csv")
    df_convo.to_csv(file_output,sep=",",encoding="utf8", index=True)
    print(f"Wrote the output to {file_output}")


def get_lang_detector() -> LanguageDetector:
    """Returns language detector"""

    return LanguageDetector()


def detect_lang(text: str, nlp_model: spacy) -> str:
    """Detects language of utterances"""

    doc = nlp_model(text)
    language = doc._.language

    return language["language"]


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
