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
   
    df_lhs = get_df(lhs)
    df_rhs = get_df(rhs)
    df = df_lhs

    # universal-sentence-encoder-multilingual
    module_url = "https://tfhub.dev/google/universal-sentence-encoder/3"
    model = tensorflow_hub.load(module_url)

    # 
    df["embeddings_lhs"] = numpy.array(model(df_lhs['text'])["outputs"]).tolist()
    df["embeddings_rhs"] = numpy.array(model(df_rhs['text'])["outputs"]).tolist()
    df.rename(columns={"text":"text_lhs"},inplace=True)
    df["text_rhs"] = df_rhs["text"]
    df["similarity"] = df.apply(calc_similarity,axis=1)
    df["quartile"] = df["similarity"].apply(calc_quartile)

    with pandas.option_context('display.max_colwidth', 75):
        print(df[["quartile","similarity","text_lhs","text_rhs"]])
        print(df[["quartile","similarity","text_lhs","text_rhs"]])
        print(df[["quartile","similarity","text_lhs","text_rhs"]].sort_values("similarity",ascending=True).head(50))
    
    print(df[["quartile","similarity"]].groupby("quartile").count())
        
def calc_quartile(sim: numpy.float64) -> int:
    "Work out what quadrant"
    if sim > 0.75:
        return 4
    elif sim > 0.5:
        return 3
    elif sim > 0.25:
        return 2
    else:
        return 1
    
def calc_similarity(row:pandas.Series) -> numpy.float64:
    "Calculate dot product cosine similarity for already normalised USE results"
    similarty_results = numpy.inner(row["embeddings_lhs"],row["embeddings_rhs"])
    return similarty_results

def get_df(uri:str) -> pandas.DataFrame:
    "Get dataframe of examples"
    print(f"Reading {uri}")
    file_obj = open(uri,mode="r",encoding="utf8")
    file_json = json.load(file_obj,encoding="utft8")
    df = pandas.json_normalize(file_json["examples"])
    df.set_index('id',inplace=True)
    return df

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
