#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python simple_json_unlabelled.py -f <your input file relative path>
#
# *****************************************************************************

# standard imports
import json
from datetime import datetime
from dateutil import parser
from typing import Union



# third party imports
import pandas
import numpy
import click
import humanfirst

@click.command()
@click.option('-f','--filename',type=str,required=True,help='Input File')
def main(filename: str):
    
    file_in = open(filename,mode='r',encoding='utf8')
    file_out = filename.replace('.json','.csv')
    df_json = json.load(file_in)
    df = pandas.json_normalize(df_json)
    df.to_csv(file_out, encoding='utf8',index=False)
    print(df)
    
    
    
if __name__ == '__main__':
    main()