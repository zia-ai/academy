#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python summarize/03_summarize_diff_hf_json.py                                          # pylint: disable=invalid-name
#
# Compare workspaces a and b as json for instance comparing the summmary of two different models
#
# ********************************************************************************************************************

# standard imports
import os
import sys
import pathlib
import json

# 3rd party imports
import click
import pandas
import numpy
import tensorflow_hub # pylint: disable=import-error


# Custom Imports
import_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(pathlib.Path(import_path).parent)
sys.path.insert(1, hf_module_path)

@click.command()
@click.option('-a', '--lhs', type=str, required=True, help='URI for HF Workspace Json for LHS of compare')
@click.option('-b', '--rhs', type=str, required=True, help='URI for HF Workspace Json for RHS of compare')
def main(lhs: str, rhs: str):
    '''Main function'''

    # universal-sentence-encoder-multilingual
    module_url = "https://tfhub.dev/google/universal-sentence-encoder/3"
    model = tensorflow_hub.load(module_url)
    print(type(model))
    sentences_a = ["Whatever the fuck is this","What the fuck might this be?","Third sentence"]
    embeddings_a = numpy.array(model(sentences_a)["outputs"]).tolist()
    sentences_b = ["Whatever the shitting fuck is this","What the sod might this be?","Third sentence"]
    embeddings_b = numpy.array(model(sentences_b)["outputs"]).tolist()
    df = pandas.DataFrame(zip(sentences_a,embeddings_a,sentences_b,embeddings_b),
                          columns=["sentences_a","embeddings_a","sentences_b","embeddings_b"])
    df["similarity"] = df.apply(similarity,axis=1)
    with pandas.option_context('display.max_colwidth', 75):
        print(df[["sentences_a","sentences_b","similarity"]])

    df_lhs = get_df(lhs)
    print(df_lhs)
    print(rhs)

def similarity(row:pandas.Series) -> numpy.float64:
    "Calculate dot product cosine similarity for already normalised USE results"
    similarty_results = numpy.inner(row["embeddings_a"],row["embeddings_b"])
    return similarty_results

def get_df(uri:str) -> pandas.DataFrame:
    "Get dataframe of examples"
    print(f"Reading {uri}")
    file_obj = open(uri,mode="r",encoding="utf8")
    file_json = json.load(file_obj,encoding="utft8")
    return pandas.json_normalize(file_json["examples"])

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
