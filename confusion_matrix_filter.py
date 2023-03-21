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
from sklearn.metrics import precision_score, recall_score, f1_score, precision_recall_fscore_support, classification_report, accuracy_score, balanced_accuracy_score, confusion_matrix

# Typical phrases.csv from HF contains
# Labelled Phrase,Detected Phrase,Intent Id,Intent Name,Top Match Intent Id,Top Match Intent Name,Top Match Score,Entropy,Uncertainty,Margin Score,Result Type

@click.command()
@click.option('-n', '--phrases_filename', type=str, default='./data/phrases.csv', help='Phrases')
@click.option('-i', '--intents_summary_filename', type=str, default='./data/intents_summary.csv', help='Intents Summary')
@click.option('-f', '--f1_filter', type=float, default=1.0, help='f1-filter')
@click.option('-p', '--precision_filter', type=float, default=1.0, help='precision-filter')
@click.option('-r', '--recall_filter', type=float, default=1.0, help='recall-filter')
@click.option('-q', '--tp_filter', type=float, default=-1.0, help='tp-filte')
@click.option('-w', '--fp_filter', type=float, default=-1.0, help='fp-filter')
@click.option('-e', '--fn_filter', type=float, default=-1.0, help='fn-filter')
@click.option('-m', '--misprediction', type=float, default=5.0, help='misprediction count per intent')
def main(phrases_filename: str, intents_summary_filename: str, f1_filter:float, precision_filter: float, 
         recall_filter: float, tp_filter:float, fp_filter: float, fn_filter:float, misprediction: float):
    process(phrases_filename, intents_summary_filename, tp_filter, fp_filter, fn_filter, f1_filter, precision_filter, recall_filter, misprediction)

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

def process(phrases_filename: str,  intents_summary_filename: str, 
            tp_filter:float, fp_filter: float, fn_filter:float, 
            f1_filter:float, precision_filter: float, recall_filter: float, misprediction: float):
    df = pandas.read_csv(phrases_filename)
    
    # summarise
    print('Summary of df before trimming labelled FN')
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())
    print('Summary after trimming labelled FN')
    df = df[df["Result Type"]!="FALSE_NEGATIVE"]
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())
    
    # calculate correct and sumarise
    # df["correct"] = df["Intent Name"] == df["Top Match Intent Name"]
    # gb = df[["Intent Name","correct","Labelled Phrase"]].groupby(["Intent Name","correct"]).count()
    # gb.rename(columns={"Labelled Phrase":"count_correct"},inplace=True)
    # print(gb)

    # Values going to nlu_fallback
    print(df[df["Top Match Intent Name"]=="nlu_fallback"])
        
    # time.sleep(0.1)
    # print('')
    # print('Through individual functions')
    # print(f'precision: {precision_score(df["Intent Name"],df["Top Match Intent Name"],average="macro")}')
    # print(f'recall:    {recall_score(df["Intent Name"],df["Top Match Intent Name"],average="macro",zero_division="warn")}')
    # print(f'f1_score:  {f1_score(df["Intent Name"],df["Top Match Intent Name"],average="macro")}') 
    # print(f'accuracy:  {accuracy_score(df["Intent Name"],df["Top Match Intent Name"])}') 
    # print(f'balanced_accuracy_score:  {balanced_accuracy_score(df["Intent Name"],df["Top Match Intent Name"],)}')



    # print('Through combined - should be same as above')
    # print(precision_recall_fscore_support(df["Intent Name"],df["Top Match Intent Name"],average="macro",zero_division="warn"))

    # print('Full classification report by intent - should match with HF NLU tab')
    # output_dict = classification_report(df["Intent Name"],df["Top Match Intent Name"],zero_division="warn",output_dict=True)
    # assert(isinstance(output_dict,dict))
    
    # # remove summary and make dict key property
    # list_values = []
    # for key in output_dict.keys():
    #     if key in ["accuracy","macro avg","weighted avg"]:
    #         continue
    #     output_obj = output_dict[key]
    #     output_obj["Intent Name"] = key
    #     list_values.append(output_obj)
                
    # df_cr = pandas.json_normalize(list_values)
    # df_cr.set_index("Intent Name",drop=True,inplace=True)
    # df_cr.sort_values("f1-score",inplace=True)
    # print(df_cr["f1-score"])

    # final_df_matrix = original_df_matrix.apply(filter_based_on_tp_fp_fn,axis=1)

    intents_summary_df = pandas.read_csv(intents_summary_filename)
    intents_summary_df = intents_summary_df.loc[intents_summary_df["Intent Name"] != "root"]
    intents_summary_df.set_index(["Intent Name"],drop=True,inplace=True)
    # print(intents_summary_df.columns)
    filtered_labels = filter_labels(intents_summary_df, tp_filter, fp_filter, fn_filter, f1_filter, precision_filter, recall_filter)

    labels = sorted(list(set(intents_summary_df.index)))
    matrix = confusion_matrix(df["Intent Name"],df["Top Match Intent Name"],labels = labels)
    df_matrix = pandas.DataFrame(data=matrix,index=labels,columns=labels)
    # print(df_matrix.loc[filtered_labels])
    
    reduced_df_matrix = df_matrix.loc[filtered_labels]

    reduced_df_matrix["class_with_fn"] = reduced_df_matrix.apply(reduced_matrix_filter,args=[misprediction],axis=1)

    misclassified_intents = []
    for row in reduced_df_matrix.iterrows():
        misclassified_intents.extend(row[1]["class_with_fn"])

    # print(*misclassified_intents,sep="\n")
    print(df_matrix.loc[filtered_labels][misclassified_intents])
    # count={}
    # c = 0
    # for intent in list(intents_summary_df.index):
    #     if intent in labels:
    #         if intent in count:
    #             count[intent] = count[intent] + 1
    #             print(intent)
    #         else:
    #             count[intent] = 1
    #             c = c+1


def reduced_matrix_filter(row: pandas.Series, misprediction: float):
    class_with_false_negatives = []
    for index, value in row.items():
        if value >= misprediction:
            class_with_false_negatives.append(index)

    return class_with_false_negatives

if __name__ == '__main__':
    main()