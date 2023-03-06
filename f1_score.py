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
from sklearn.metrics import precision_score, recall_score, f1_score, precision_recall_fscore_support, classification_report, accuracy_score, balanced_accuracy_score

# Typical phrases.csv from HF contains
# Labelled Phrase,Detected Phrase,Intent Id,Intent Name,Top Match Intent Id,Top Match Intent Name,Top Match Score,Entropy,Uncertainty,Margin Score,Result Type

@click.command()
@click.option('-f', '--filename', type=str, default='./data/results_1.csv', help='Results')
def main(filename: str):
    process(filename)

def process(filename: str):
    df = pandas.read_csv(filename)
    
    # summarise
    print('Summary of df before trimming labelled FN')
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())
    print('Summary after trimming labelled FN')
    df = df[df["Result Type"]!="FALSE_NEGATIVE"]
    print(df[["Labelled Phrase","Result Type"]].groupby(["Result Type"]).count())
    
    # calculate correct and sumarise
    df["correct"] = df["Intent Name"] == df["Top Match Intent Name"]
    gb = df[["Intent Name","correct","Labelled Phrase"]].groupby(["Intent Name","correct"]).count()
    gb.rename(columns={"Labelled Phrase":"count_correct"},inplace=True)
    print(gb)

    # Values going to nlu_fallback
    print(df[df["Top Match Intent Name"]=="nlu_fallback"])
        
    time.sleep(0.1)
    print('')
    print('Through individual functions')
    print(f'precision: {precision_score(df["Intent Name"],df["Top Match Intent Name"],average="macro")}')
    print(f'recall:    {recall_score(df["Intent Name"],df["Top Match Intent Name"],average="macro",zero_division="warn")}')
    print(f'f1_score:  {f1_score(df["Intent Name"],df["Top Match Intent Name"],average="macro")}') 
    print(f'accuracy:  {accuracy_score(df["Intent Name"],df["Top Match Intent Name"])}') 
    print(f'balanced_accuracy_score:  {balanced_accuracy_score(df["Intent Name"],df["Top Match Intent Name"],)}') 
    
    print('Through combined - should be same as above')
    print(precision_recall_fscore_support(df["Intent Name"],df["Top Match Intent Name"],average="macro",zero_division="warn"))

    print('Full classification report by intent - should match with HF NLU tab')
    output_dict = classification_report(df["Intent Name"],df["Top Match Intent Name"],zero_division="warn",output_dict=True)
    assert(isinstance(output_dict,dict))
    
    # remove summary and make dict key property
    list_values = []
    for key in output_dict.keys():
        if key in ["accuracy","macro avg","weighted avg"]:
            continue
        output_obj = output_dict[key]
        output_obj["Intent Name"] = key
        list_values.append(output_obj)
                
    df = pandas.json_normalize(list_values)
    df.set_index("Intent Name",drop=True,inplace=True)
    df.sort_values("f1-score",inplace=True)
    print(df.to_string())
    
    

if __name__ == '__main__':
    main()