#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# f1_calcs.py -f ./data/phrases.csv
#
# *****************************************************************************

# standard imports

# third party imports
import pandas
import click
import time
import heapq
from sklearn.metrics import precision_score, recall_score, f1_score, precision_recall_fscore_support, classification_report, accuracy_score, balanced_accuracy_score, confusion_matrix

# Typical phrases.csv from HF contains
# Labelled Phrase,Detected Phrase,Intent Id,Intent Name,Top Match Intent Id,Top Match Intent Name,Top Match Score,Entropy,Uncertainty,Margin Score,Result Type

@click.command()
@click.option('-c', '--top_mispredictions', type=int, default=10, help='Number of top mispredictions')
@click.option('-n', '--phrases_filename', type=str, default='./data/phrases.csv', help='Phrases')
@click.option('-i', '--intents_summary_filename', type=str, default='./data/intents_summary.csv', help='Intents Summary')
@click.option('-f', '--f1_filter', type=float, default=1.0, help='f1-filter')
@click.option('-p', '--precision_filter', type=float, default=1.0, help='precision-filter')
@click.option('-r', '--recall_filter', type=float, default=1.0, help='recall-filter')
@click.option('-q', '--tp_filter', type=float, default=-1.0, help='tp-filte')
@click.option('-w', '--fp_filter', type=float, default=-1.0, help='fp-filter')
@click.option('-e', '--fn_filter', type=float, default=-1.0, help='fn-filter')
@click.option('-m', '--misprediction', type=float, default=5.0, help='misprediction count per intent')
def main(phrases_filename: str, intents_summary_filename: str, top_mispredictions: int,
         f1_filter:float, precision_filter: float, recall_filter: float, 
         tp_filter:float, fp_filter: float, fn_filter:float, misprediction: float):
    process(phrases_filename, intents_summary_filename, top_mispredictions,tp_filter, fp_filter, fn_filter, f1_filter, precision_filter, recall_filter, misprediction)

def filter_labels(df_cr,
                  tp_filter:float, fp_filter: float, fn_filter:float,
                  f1_filter:float, precision_filter: float, 
                  recall_filter: float):
    
    labels = sorted(list(set(df_cr.loc[(df_cr["F1"] <= f1_filter) & 
                            (df_cr["Precision"] <= precision_filter) & 
                            (df_cr["Recall"] <= recall_filter) &
                            (df_cr["True Positives"] >= tp_filter) & 
                            (df_cr["False Positives"] >= fp_filter) & 
                            (df_cr["False Negatives"] >= fn_filter)].index)))
    
    return labels

def process(phrases_filename: str,  intents_summary_filename: str, top_mispredictions: int,
            tp_filter:float, fp_filter: float, fn_filter:float, 
            f1_filter:float, precision_filter: float, recall_filter: float, misprediction: float):
    df = pandas.read_csv(phrases_filename)
    
    # summarise
    print('Summary of df before trimming labelled FN')
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())
    print('Summary after trimming labelled FN')
    df = df[df["Result Type"]!="FALSE_NEGATIVE"]
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())

    intents_summary_df = pandas.read_csv(intents_summary_filename)
    intents_summary_df = intents_summary_df.loc[intents_summary_df["Intent Name"] != "root"]
    intents_summary_df.set_index(["Intent Name"],drop=True,inplace=True)
    # print(intents_summary_df.columns)
    # filtered_labels = filter_labels(intents_summary_df, tp_filter, fp_filter, fn_filter, f1_filter, precision_filter, recall_filter)

    labels = sorted(list(set(intents_summary_df.index)))
    matrix = confusion_matrix(df["Intent Name"],df["Top Match Intent Name"],labels = labels)
    total_mispredictions = 0
    for i in range(len(matrix)):
        for j in range(len(matrix[i])):
            if i == j:
                matrix[i][j] = 0
            else:
                total_mispredictions = total_mispredictions + 1

    df_matrix = pandas.DataFrame(data=matrix,index=labels,columns=labels)
    top_mispredictions_list = (heapq.nlargest(top_mispredictions,matrix.max(1)))
    # print((heapq.nlargest(top_mispredictions,matrix.max(1))))
    intents_col = set()
    intents_row = set()
    for row in df_matrix.iterrows():
        for index in row[1].index:
            if row[1][index] in top_mispredictions_list:
                intents_col.add(index)
                intents_row.add(row[0])
    # print(intents_row)

    print("True positives are replaced with 0")

    reduced_matrix = df_matrix.loc[list(intents_row)][list(intents_col)]
    reduced_matrix.loc["Total"] = reduced_matrix.sum(axis = 0)
    reduced_matrix = reduced_matrix.sort_values(by=["Total"],axis=1,ascending=False)
    reduced_matrix["Total"] = reduced_matrix.sum(axis = 1)
    reduced_matrix.sort_values(by=["Total"],axis=0,inplace=True,ascending=False)
    print(reduced_matrix)

    print(f"\nPercentage of confusions represented by the matrix: {round((reduced_matrix.loc['Total']['Total']/total_mispredictions)*100,2)} %")

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
        pair_percentage[key] = round((pair[key]/total_mispredictions)*100,2)
    sorted_pair = sorted(pair_percentage.items(), key=lambda x:x[1],reverse=True)
    
    count = 0
    sum_of_10_pair_mispredictions = 0
    print(f"\nPercentage of confusions for the top {top_mispredictions} pairs\n")
    for pair_tuple in sorted_pair:
        print(f"{pair_tuple[0]:50} {pair[pair_tuple[0]]:25} {pair_tuple[1]:20}%")
        if count >= top_mispredictions:
            break
        count = count+1
        sum_of_10_pair_mispredictions = sum_of_10_pair_mispredictions + pair[pair_tuple[0]]
    print(f"\nSum of mispredictions between all of the above intent-pairs: {sum_of_10_pair_mispredictions}")
    print(f"Percentage of confusion: {round((sum_of_10_pair_mispredictions/total_mispredictions)*100,2)} %")

if __name__ == '__main__':
    main()