#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# reduce_confusion_matrix.py -f <input_filename> -c <no of top mispredictions>
#
# *****************************************************************************

# standard imports
import heapq
from os.path import isfile, isdir, join
import zipfile, io

# third party imports
import pandas
import numpy
import click
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import squarify
import plotly.express as px

# custom imports
import humanfirst_apis

# Typical phrases.csv from HF contains
# Labelled Phrase,Detected Phrase,Intent Id,Intent Name,Top Match Intent Id,Top Match Intent Name,Top Match Score,Entropy,Uncertainty,Margin Score,Result Type

@click.command()
@click.option('-m', '--top_mispredictions', type=int, default=5, help='Number of top mispredictions')
@click.option('-f', '--filedir', type=str, default='./data', help='All the files from evaluations gets extracted at this directory')
@click.option('-o', '--output_filepath', type=str, default='./data/reduced_confusion_matrix.csv', help='Output filepath for reduced confusion matrix')
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-e', '--evaluation_id', type=str, required=True, help='HumanFirst evaluation id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with if not providing username/password')
def main(filedir: str, output_filepath: str, top_mispredictions: int, username: str, password: int, namespace: bool, playbook: str, bearertoken: str, evaluation_id: str) -> None:
    '''Main function'''

    if not isdir(filedir):
        raise Exception(f"{filedir} is not a directory")
    
    headers = humanfirst_apis.process_auth(bearertoken, username, password)
    response = humanfirst_apis.get_evaluation_zip(headers,namespace,playbook, evaluation_id)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall(filedir)
    
    phrases_filename = join(filedir,"phrases.csv")
    process(phrases_filename, output_filepath, top_mispredictions)

def process(phrases_filename: str, output_filepath: str, top_mispredictions: int) -> None:
    '''Controls the script flow'''

    # validate file path
    if (not isfile(phrases_filename)) or (phrases_filename.split('.')[-1] != 'csv'):
        raise Exception("Incorrect file path or not a CSV file")
    
    # read phrases file
    df = pandas.read_csv(phrases_filename)
    labels = sorted(list(set(df["Intent Name"])))
    print(f"Total Number of intents: {len(labels)}")
    
    # summarise
    print('Summary of df')
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())
    total_mispredictions = df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count().loc["Fail"]["Labelled Phrase"]

    # create confusion matrix
    matrix = confusion_matrix(df["Intent Name"],df["Top Match Intent Name"],labels = labels)

    # reduce confusion matrix
    reduced_matrix = reduce_confusion_matrix(matrix, labels, top_mispredictions)
    print("\nTrue positives are replaced with 0\n")
    print("Reduced Confusion Matrix")
    print(reduced_matrix)
    print(f"\nPercentage of confusions represented by the matrix: {round((reduced_matrix.loc['Total']['Total']/total_mispredictions)*100,2)} %")
    reduced_matrix.to_csv(output_filepath,sep=",",encoding="utf8")
    print(f"\nReduced confusion matrix is stored at {output_filepath}")
    # determine top intent pairs that are most confused
    sorted_intent_pairs = find_top_intent_pair(reduced_matrix, total_mispredictions)
    summarize_top_intent_pair(sorted_intent_pairs, top_mispredictions, total_mispredictions)

def reduce_confusion_matrix(matrix: numpy.matrix, labels: list, top_mispredictions: int) -> None:
    '''Reduces Confusion matrix'''

    # edge cases
    no_of_matrix_cells = len(labels) * len(labels)
    if top_mispredictions <= 0 or top_mispredictions > no_of_matrix_cells:
        raise Exception(f"Top mispredictions should be > 0 and less than or equal to {no_of_matrix_cells}")
    
    # replace true positives to 0 and sum all the incorrect predictions
    for i in range(len(matrix)):
        for j in range(len(matrix[i])):
            if i == j:
                matrix[i][j] = 0

    # confusion matrix into dataframe
    df_matrix = pandas.DataFrame(data=matrix,index=labels,columns=labels)
    
    # get the list of top mispredictions e.g. top 10 largest number of incorrect predictions
    top_mispredictions_list = (heapq.nlargest(top_mispredictions,matrix.max(1)))

    # determining the row and col name of those top x incorrect predictions
    intents_col = set()
    intents_row = set()
    for row in df_matrix.iterrows():
        for index in row[1].index:
            if row[1][index] in top_mispredictions_list:
                intents_col.add(index)
                intents_row.add(row[0])

    # create reduced matrix
    reduced_matrix = df_matrix.loc[list(intents_row)][list(intents_col)]
    reduced_matrix.loc["Total"] = reduced_matrix.sum(axis = 0)
    reduced_matrix = reduced_matrix.sort_values(by=["Total"],axis=1,ascending=False)
    reduced_matrix["Total"] = reduced_matrix.sum(axis = 1)
    reduced_matrix.sort_values(by=["Total"],axis=0,inplace=True,ascending=False)
    
    # reorder Total row as last row
    reorder_list = list(reduced_matrix.index)
    reorder_list.remove("Total")
    reorder_list.append("Total")
    reduced_matrix = reduced_matrix.reindex(reorder_list)

    return reduced_matrix

def find_top_intent_pair(reduced_matrix: pandas.DataFrame, total_mispredictions: int) -> list:
    '''determine top intent pair the model is getting confused'''

    pair = {}
    for row in reduced_matrix.iterrows():
        if row[0] != "Total":
            for index in row[1].index:
                if index != "Total":
                    key = f"{row[0]} <-> {index}"
                    key_reverse = f"{index} <-> {row[0]}"
                    if key in pair.keys():
                        pair[key] = pair[key] + row[1][index]
                    elif key_reverse in pair.keys():
                        pair[key_reverse] = pair[key_reverse] + row[1][index]
                    else:
                         pair[key] = row[1][index]
    pair_percentage = {}
    for key in pair.keys():
        pair_percentage[key] = [pair[key],round((pair[key]/total_mispredictions)*100,2)]
    sorted_pair = sorted(pair_percentage.items(), key=lambda x:x[1][1],reverse=True)

    return sorted_pair

def summarize_top_intent_pair(sorted_pair: list, top_mispredictions: int, total_mispredictions: int) -> None:
    '''Summarizes about top confused intent pairs'''

    count = 1
    sum_of_x_pair_mispredictions = 0
    print(f"\nPercentage of confusions for the top {top_mispredictions} pairs(Intent Pair----Sum of Confusion----Confusion %)")
    sorted_pair_list_of_dicts = []
    for pair_tuple in sorted_pair:
        pair_dict = {}
        print(f"{pair_tuple[0]:80} {pair_tuple[1][0]:25} {pair_tuple[1][1]:20}%")
        pair_dict["labels"] = pair_tuple[0]
        pair_dict["values"] = pair_tuple[1][1]
        sorted_pair_list_of_dicts.append(pair_dict)
        if count >= top_mispredictions:
            break
        count = count+1
        sum_of_x_pair_mispredictions = sum_of_x_pair_mispredictions + pair_tuple[1][0]
    print(f"\nSum of mispredictions between all of the above intent-pairs: {sum_of_x_pair_mispredictions}")
    print(f"Percentage of confusion: {round((sum_of_x_pair_mispredictions/total_mispredictions)*100,2)} %")
    
    df = pandas.json_normalize(data=sorted_pair_list_of_dicts)
    fig = px.treemap(df, path=['labels'],values='values', width=1600, height=800)
    fig.update_layout(
        margin = dict(t=50, l=25, r=25, b=25))
    fig.update_traces(hovertemplate='Confused_intent_pair=%{label}<br>Percentage of confusion=%{value}%<extra></extra>')
    fig.show()

if __name__ == '__main__':
    main()