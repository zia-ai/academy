#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *********************************************************************************************************************
#
# python merge_multiple_csv.py
# Merges row-wise all the CSVs with same columns into a single CSV
#
# *********************************************************************************************************************

# third party imports
import click
import pandas

@click.command()
@click.option('-f', '--filenames', type=str, required=True, help='File list "," seperated')
@click.option('-o', '--output_filepath', type=str, required=True, help='Output file path')
@click.option('-d', '--delimiter', type=str, required=False, default=",",
              help='Delimiter for the csv file')
def main(filenames: str, output_filepath: str, delimiter: str) -> None:
    """Main Function"""

    merge(filenames, output_filepath, delimiter)


def merge(filenames: str, output_filepath: str, delimiter: str) -> None:
    """Merges row-wise all the CSVs with same columns into a single CSV"""

    filename_list = filenames.split(",")

    df_list = []

    for _, filename in enumerate(filename_list):
        df = pandas.read_csv(filename,delimiter=delimiter,encoding="utf8")
        df_list.append(df)

    final_df = pandas.concat(df_list,ignore_index=True)

    print(final_df)
    print(final_df.columns)

    final_df.to_csv(output_filepath, sep=",", index=False, encoding='utf8')
    print(f'Full data is saved at {output_filepath}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
