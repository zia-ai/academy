#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python analyse_csv.py --input <exported_csv_intents>.csv
# 
# Reads a HumanFirst export CSV of intents to display the parent
# child relationship, check for zero intents
#
# *****************************************************************************

# standard imports

# third party imports
import pandas
import click

# custom imports

@click.command()
@click.option('-i','--input',type=str,required=True,help='The input CSV for instance "./data/workspace1-intents-1666257806036.csv.csv"')
def main(input: str):
    process(input)

def process(input: str):
    ''' Read a csv source controlled labelled set and produce HF format'''
    
    df = pandas.read_csv(input,delimiter=',',names=['utterance','slash_sep_hier'])
    assert isinstance(df,pandas.DataFrame)
    df['list_intents'] = df['slash_sep_hier'].str.split('/')
    df = pandas.concat([df,pandas.DataFrame(df['list_intents'].tolist())],axis=1)
    print(df)
    print(df.shape)
    gb_keys = list(range(df.columns[-1]+1)) # +1 as range exclusive and we need inclusive
    print(df.groupby(gb_keys).count())
    gb_df = df[['slash_sep_hier','utterance']].groupby(['slash_sep_hier']).count()
    gb_df = gb_df.sort_values('utterance',ascending=False)
    print(gb_df)
    

if __name__ == '__main__':
    main()