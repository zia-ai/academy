#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python csv_to_json_unlabelled.py
#
# *****************************************************************************

# standard imports
import uuid
from datetime import datetime
from typing import Union

# 3rd party imports
import pandas
import click

# custom imports
import humanfirst

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-m', '--metadata_keys', type=str, required=True, help='<metadata_col_1,metadata_col_2,...,metadata_col_n>')  # pylint: disable=line-too-long
@click.option('-u', '--utterance_col', type=str, required=True, help='Column name containing utterances')
def main(filename: str, metadata_keys: str, utterance_col: str) -> None:
    """Main Function"""

    # read the input csv
    df = pandas.read_csv(filename, encoding='utf8')
    metadata_keys = metadata_keys.split(",")

    # create metadata object per utterance
    df['metadata'] = df.apply(create_metadata, args=[metadata_keys], axis=1)

    # build examples
    df = df.apply(build_examples, args=[utterance_col], axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    filename_out = filename.replace('.csv', '.json')
    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()


def build_examples(row: pandas.Series, utterance_col: str):
    '''Build the examples'''

    # build examples
    example = humanfirst.HFExample(
        text=row[utterance_col],
        id=f'example-{uuid.uuid4()}',
        created_at=datetime.now().isoformat(),
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata']
    )
    row['example'] = example
    return row


def create_metadata(row: Union[pandas.Series, dict], metadata_keys_to_extract: list) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    metadata = {}
    for key in metadata_keys_to_extract:
        metadata[key] = str(row[key])

    return metadata


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
