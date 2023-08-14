#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *******************************************************************************************************************120
#
# python transpose-columns-rows.py
#
# Take a csv with one line per intent and examples as columns and turn into an intent,utterance format
#
# **********************************************************************************************************************

# standard imports

# 3rd party imports
import click
import pandas


@click.command()
@click.option('-f', '--file', type=str, required=True, help='Input CSV')
@click.option('-d', '--delimiter', type=str, required=False, default=',', help='Delimiter for csv')
@click.option('-e', '--encoding', type=str, required=False, default='utf8', help='Encoding for csv')
@click.option('-x', '--example_index_start', type=int, required=True, help='Index of column with first example data in')
def main(file: str, delimiter: str, encoding: str, example_index_start: int) -> None:
    '''Main Function'''
    df = pandas.read_csv(file, delimiter=delimiter, encoding=encoding)
    assert isinstance(df,pandas.DataFrame)
    df = df.fillna('')
    utterance_columns = df.columns[example_index_start:].to_list()
    df = df.apply(join_utterance_columns, args=[utterance_columns], axis=1)
    df.drop(columns=utterance_columns, inplace=True)
    df = df.explode("utterances")

    # clear out any columns that have no values or all the same value in
    for column in df.columns:
        if df[column].nunique() <= 1:
            df.drop(columns=[column],inplace=True)
            print(f'Dropped {column}')

    file_out_name = file.replace('.csv', '_output.csv')
    df.to_csv(file_out_name, index=False, header=True, encoding='utf8')

def join_utterance_columns(row: pandas.Series, utterance_columns: list) -> pandas.Series:
    ''' Joins the utterance columsn into a list which can be exploded'''
    row["utterances"] = list()
    for column in utterance_columns:
        if row[column] == '':
            return row
        else:
            progress = list(row["utterances"])
            progress.append(row[column])
            row["utterances"] = progress
    return row


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
