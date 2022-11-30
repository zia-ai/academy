#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python liu_resample.py -n liuetal -s 190 -v 0
# 
# *****************************************************************************

# standard imports

# third party imports
import pandas
import click

@click.command()
@click.option('-n','--name',type=str,default='liuetal',help='Name used for all files')
@click.option('-v','--version',type=str,default=0,help='version of intent file')
@click.option('-s','--sample',type=int,default=0,help='n conversations to sample from dataset')
def main(name: str, sample: int, version: int):
    # TODO - test workspace function

    # read file    
    dtypes = {
        'utterance': str,
        'label':str
    }
    df = pandas.read_csv(f'./workspaces/{name}/{name}{version}-intents.csv', names=['utterance','label'],encoding='utf8', sep=',', dtype=dtypes, keep_default_na=False)
    assert isinstance(df,pandas.DataFrame)
    print(df.groupby('label').count().sort_values)
    # sample randomly all of an intent or maximum 190
    df = df.groupby('label').apply(lambda x: x.sample(min([len(x),190]))).reset_index(drop=True)
    print(df.groupby('label').count().sort_values)
    df.to_csv(f'./workspaces/{name}/{name}{version}-resampled-intents.csv',index=False,encoding='utf8',header=False)

if __name__ == '__main__':
    main()