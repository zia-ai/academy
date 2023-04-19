#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./voc_analysis/bubble_chart.py 
#   -i ./data/voc_predictions.csv  
#   -c 0.35 
#   -p 3
#   -d "Survey ID"
#
# *****************************************************************************

# standard imports
import os
import random

# 3rd party imports
import click
import pandas
import matplotlib.pyplot as plt

@click.command()
@click.option('-i', '--input', type=str, required=True, help='Input File produced by predict_uuterance_from_voc.py script')
@click.option('-o', '--output', type=str, default='./data/scattery.png', help='Output path where the produced bubble chart should be stored')
@click.option('-d', '--document_id_col', type=str, required=True, help='Document id of the review')
@click.option('-p', '--scale', type=int, default = 3, help='Scales the bubble size')
@click.option('-c', '--confidence', type=float, default=0.35, help='Confidence clip to label an utterance')
def main(input: str, output: str, confidence: float, scale: int, document_id_col: str):
    if not os.path.exists(input):
        print("Couldn't find file at the location you provided: {input}")
        quit()
    df = pandas.read_csv(input,encoding='utf8')
    assert isinstance(df,pandas.DataFrame)

    df_tickets = calc_scores_for_plotting(confidence,
                                        document_id_col,
                                        df)
    plot_bubble_chart(df_tickets, scale, output)

def plot_bubble_chart(df_tickets: pandas.DataFrame,scale: int,output: str) -> None:
    '''Produces bubble chart'''

    df_tickets["count_of_utterance"] = df_tickets["count_of_utterance"] * scale
    # scatter plot with scatter() function
    plt.figure(figsize=(15, 11), dpi=300,)
    ax = plt.subplot()
    ax.spines.right.set_visible(False)
    ax.spines.top.set_visible(False)
    ax.set_xlim(right=-0.1)
    ax.set_xlim(left=1.0)
    ax.set_ylim(bottom=0.0)
    ax.set_ylim(top=1.1)
    plt.scatter('normalized_avg_digital_nps', 'normalized_avg_detractor_score', data=df_tickets,linewidths=0,s='count_of_utterance',alpha=0.5,color='color')
    plt.xlabel("Average NpsScore given on survey", size=10)
    plt.ylabel("Churn risk indicators score", size=10)
    plt.title("Aspects most likely to cause issues", size=12)

    i = 0
    for key, row in df_tickets.iterrows():
        if i % 2 == 0:
            xalign='left'
            yalign='bottom'
        else: 
            xalign='right'
            yalign='top'
        label = str(row.name).replace("aspects_negative","AN")
        plt.annotate(label, xy=(row['normalized_avg_digital_nps'], row["normalized_avg_detractor_score"]), size=8, verticalalignment=yalign ,horizontalalignment=xalign,rotation=-45, rotation_mode='anchor')
        i = i + 1
    
    plt.savefig(output)

def calc_scores_for_plotting(confidence: float, document_id_col: str, df: pandas.DataFrame) -> pandas.DataFrame:
    '''Calculates normalized avg nps and normalized avg detractor scores for plotting'''

    # document-level detractor score calculated from indicators_detractor
    rename = {"confidence":"detractor_score"}
    df_detractor_score = df[[document_id_col,
                             "parent_intent",
                             "confidence"]].groupby(["parent_intent",
                                                     document_id_col]).sum(numeric_only=True).rename(columns=rename).loc["indicators_detractor"]
    # choosing only aspects_negative
    df = df.loc[df["parent_intent"] == "aspects_negative"].reset_index(drop=True)
    print(df_detractor_score)

    # confidence clip is applied
    df = df.apply(confidence_clip,axis=1,args=[confidence])

    df["detractor_score"] = df[document_id_col].apply(assign_detractor_score,args=[df_detractor_score])
    
    df_tickets = df[["intents_satifying_confidence", "detractor_score", "Digital NPS"]].groupby(["intents_satifying_confidence"]).mean()

    df_tickets = df_tickets.rename(columns={"detractor_score":"avg_detractor_score",
                                    "Digital NPS":"avg_digital_nps"})
    df_tickets["sum_of_confidence"] = df[["intents_satifying_confidence", "confidence_above_threshold"]].groupby(["intents_satifying_confidence"]).sum()
    df_tickets["count_of_utterance"] = df[["intents_satifying_confidence", "confidence_above_threshold"]].groupby(["intents_satifying_confidence"]).count()
    df_tickets["normalized_avg_digital_nps"] = df_tickets["avg_digital_nps"]/10.0

    df_tickets = df_tickets.apply(get_color,axis=1)

    df_tickets.drop(labels="unknown",inplace=True)
    max_avg_score = df_tickets["avg_detractor_score"].max()
    df_tickets["normalized_avg_detractor_score"] = df_tickets["avg_detractor_score"]/(max_avg_score)
    print(df_tickets)
    return df_tickets

def assign_detractor_score(id: str, df_detractor_score: pandas.DataFrame) -> float:
    '''Assigns detractor score'''

    if id in list(df_detractor_score.index):
        return df_detractor_score.loc[id].detractor_score
    else:
        return 0.0

def confidence_clip(row: pandas.Series, confidence_threshold: float) -> pandas.Series:
    '''Predictions are clipped at a particualr confidence threshold'''

    if row['confidence'] < confidence_threshold:
        row["intents_satifying_confidence"] = "unknown"
        row["confidence_above_threshold"] = 0
        return row
    row["intents_satifying_confidence"] = row["fully_qualified_intent_name"]
    row["confidence_above_threshold"] = row["confidence"]
    return row

def get_color(row: pandas.Series) -> pandas.Series:
    '''Generate a color'''
    row['color'] = '#' + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
    return row

if __name__ == '__main__':
    main()