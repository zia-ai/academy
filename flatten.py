#!/usr/bin/env python # pylint:disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python write_csv.py
#
# *****************************************************************************

# standard imports
import json
import os

# third part imports
import click
import pandas


@click.command()
@click.option('-f', '--filename', type=str, default='', required=False,
              help='HumanFirst JSON file if not wanting to download')
@click.option('-d', '--directory', type=str, default='', required=False,
              help='Directory of humanfirst JSONs to concatenate and csv')
def main(filename: str, directory: str):
    '''main'''

    examples = []
    if directory != '':
        if not directory.endswith('/'):
            directory = directory + '/'
        list_files = os.listdir(directory)
        list_files.sort()
        for file_name in list_files:
            file_name = f'{directory}{file_name}'
            if file_name.endswith(".json"):
                file_in = open(file_name, encoding='utf8')
                data_dict = json.load(file_in)
                file_in.close()
                examples.extend(data_dict["examples"])
                print(f'Parsing:  {file_name}')
            else:
                print(f'Skipping: {file_name}')
    else:
        file_in = open(filename, encoding='utf8')
        data_dict = json.load(file_in)
        file_in.close()
        examples = data_dict["examples"]

    df = pandas.json_normalize(examples)
    if directory != '':
        file_out = directory.replace("/", "_") + ".csv"
    else:
        file_out = filename.replace(".json", ".csv")
    df.to_csv(file_out, encoding='utf8', index=False)
    print(f'Wrote to: {file_out}')


if __name__ == '__main__':
    main()  # pylint:disable=no-value-for-parameter
