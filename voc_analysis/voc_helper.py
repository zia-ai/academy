#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python voc_helper.py
#
# *****************************************************************************

# standard imports
import os
import sys
sys.path.insert(1,"/home/ubuntu/source/academy")

# third party imports
import pandas
import nltk
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

def get_df_from_input(input: str,review_col: str) -> pandas.DataFrame:
    '''Converts the CSV to Dataframe and remove rows with no reviews'''
    
    if not os.path.exists(input):
        print("Couldn't find the dataset at the file location you provided:")
        print(input)
        quit()
    
    df = pandas.read_csv(input,header=0,encoding='utf8')
    assert isinstance(df,pandas.DataFrame)

    # drop all reviews which don't have any meaningful data
    print(f'Shape with all lines:                {df.shape}')
    df = df[~df[review_col].isna()]
    print(f'Shape with only non-blank verbatims: {df.shape}')
    return df


def sentence_split_and_explode(df: pandas.DataFrame, pt: nltk.tokenize.PunktSentenceTokenizer, review_col: str) -> pandas.DataFrame:
    '''Split the sentences and explode'''

    df = df.apply(sentence_split,args=[pt,review_col],axis=1)
    df = df.explode('sentence_list').reset_index(drop=True)
    
    # give a sequence number to each new segment
    df['seq'] = df.groupby(review_col).cumcount()

    # rename the column to utterance
    df.rename(columns={'sentence_list':'utterance'},inplace=True)
    return df

def sentence_split(row: pandas.Series, pt: nltk.tokenize.PunktSentenceTokenizer, review_col: str) -> pandas.Series:
    '''Splits a sentence'''

    row['sentence_list'] = pt.tokenize(row[review_col])
    return row
