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
@click.option('-s', '--sentence_similarity', is_flag=True, default=False, help='Use sentence similarity rather than a diff')
@click.option('-c', '--convo_files', type=str,required=True, help='files with examples')

def main(lhs: str, rhs: str, sentence_similarity: bool, convo_files: str):
    '''Main function'''
    convo_files = convo_files.split(',')
    print(convo_files)
    df = pandas.DataFrame()
    for file in convo_files:
        df = pandas.concat([df,get_df(file)]).reset_index()
    print(df.shape)
    print(df.loc[0,:]) 
    
    df_lhs = get_df(lhs)
    print(df_lhs.shape)
    df_rhs = get_df(rhs)

    if sentence_similarity:
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
    else:
        df_rhs = df_rhs.join(df_lhs[["text"]],rsuffix="_lhs")
        df_rhs.rename(columns={"text_lhs":"metadata.gpt35_text"},inplace=True)
        df_rhs["metadata.gpt4_text"] = df_rhs["text"]
        df_rhs["metadata.diff_bool"] = df_rhs["metadata.gpt35_text"] != df_rhs["text"]
        df_rhs["metadata.diff_bool"] = df_rhs["metadata.diff_bool"].astype(str)
        df_rhs["metadata.diff_summary"] = df_rhs["metadata.gpt35_text"] + " <-> " + df_rhs["text"]
        print(df_rhs.groupby("metadata.diff_summary").count())
          
        df_join_data = df_rhs[["id","metadata.diff_bool","metadata.gpt35_text","metadata.gpt4_text","metadata.diff_summary"]]
        df_join_data.set_index("id",inplace=True,drop=True)

        print(df.shape)
        df = df.join(df_join_data,how="left",on="context.context_id")
        print(df.shape)
        df.drop(columns=["level_0","index"],inplace=True)
        df.reset_index(inplace=True)
        print(df)
        print(df.loc[0,:])
        write_workspace("/home/ubuntu/source/hf/academy/summarize/workspaces/abcd_unlabelled05.json",df)

def write_workspace(uri:str, df: pandas.DataFrame):
    "Write back to a copy of the original file"
    uri_out = uri.replace(".json","_diff.json")
    print(f"Rereading {uri}")
    file_obj = open(uri,mode="r",encoding="utf8")
    file_json = json.load(file_obj,encoding="utft8")
    file_obj.close()
    
    # metadata
    metadata_cols = []
    metadata_new_cols = []
    for col in df.columns.to_list():
        if str(col).startswith("metadata."):
            metadata_cols.append(col)
            metadata_new_cols.append(str(col).replace("metadata.",""))
    df_metadata = df[metadata_cols].copy(deep=True)
    df_metadata.rename(columns=dict(zip(metadata_cols,metadata_new_cols)),inplace=True)
    df["metadata"] = df_metadata.to_dict(orient="records")
    df.drop(columns=metadata_cols,inplace=True)

    # context
    context_cols = []
    context_new_cols = []
    for col in df.columns.to_list():
        if str(col).startswith("context."):
            context_cols.append(col)
            context_new_cols.append(str(col).replace("context.",""))
    df_context = df[context_cols].copy(deep=True)
    df_context.rename(columns=dict(zip(context_cols,context_new_cols)),inplace=True)
    df["context"] = df_context.to_dict(orient="records")
    df.drop(columns=context_cols,inplace=True)
    
    file_json["examples"] = df.to_dict(orient="records")
    file_out = open(uri_out,mode='w',encoding='utf8')
    json.dump(file_json,file_out,indent=2)
    print(f"Wrote to: {uri_out}")
    

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
