#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *********************************************************************************************************************
#
# python csv_to_json_unlabelled.py
#
# *********************************************************************************************************************

# standard imports
import datetime
import re
from typing import Union

# 3rd party imports
import pandas
import numpy
import click
from dateutil import parser
import tqdm

# custom imports
import humanfirst


@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-h', '--head', type=int, required=False, default=0, help='Take the head values')
@click.option('-s', '--strip', is_flag=True, type=bool, required=False, default=False, help='Strip Speech VXML tags')
def main(filename: str, head: int, strip: bool):
    """main"""

    # our example input file looks like this
    dtypes = {
        "Time": str,  # created_at
        "Session UUID": str,  # convo_id
        "Intent name": str,  # intent predicted by DF
        "Utterance": str,  # client utterance
        "Speech": str  # bot utterance
    }

    # read the csv
    df = pandas.read_csv(filename,
                         encoding='utf8',
                         dtype=dtypes)
    assert isinstance(df, pandas.DataFrame)
    df.fillna('', inplace=True)
    print(f'Read dataframe {filename}')

    # slice client utterances
    df_client = df[["Time", "Session UUID",
                    "Utterance", "Intent name"]].copy(deep=True)
    df_client["role"] = "client"
    df_client["created_at"] = df_client["Time"].apply(get_timestamp, args=[0])
    print('Sliced client utterances')

    # slice bot utterances and bump on by 1 ms so it always comes just after client side
    df_expert = df[["Time", "Session UUID",
                    "Speech", "Intent name"]].copy(deep=True)
    df_expert["role"] = "expert"
    df_expert.rename(columns={"Speech": "Utterance"}, inplace=True)
    df_expert["created_at"] = df_client["Time"].apply(get_timestamp, args=[1])
    print('Sliced bot utterances')

    # replace
    if strip:
        regex_to_sub = re.compile(r"<[A-Za-z0-9 \"\'\/=\-\_]+>")
        df_expert["Utterance"] = df_expert["Utterance"].apply(sub_all,args=[regex_to_sub,""])

    # join to back to original clear up slices
    df = pandas.concat([df_client, df_expert], axis=0)
    del df_client
    del df_expert
    df.sort_values(["Session UUID", "created_at"],
                   inplace=True, ignore_index=True)

    if head > 0:
        df = df.head(head)

    print(df)
    print(df.loc[0, :])

    # build metadata for utterances or conversations
    dict_of_file_level_values = {
        'loaded_date': datetime.datetime.now().isoformat(),
        'script_name': 'csv_to_json_unlaballed_h.py'
    }
    all_keys = set(df.columns.to_list())
    remove_keys = set(["Utterance", "role", "Time"])
    metadata_keys = list(all_keys - remove_keys)


    convo_id_col = "Session UUID"
    utterance_col = "Utterance"
    # index the speakers
    df['idx'] = df.groupby([convo_id_col]).cumcount()
    df['idx_max'] = df.groupby([convo_id_col])['idx'].transform(numpy.max)

    # This info lets you filter for the first or last thing the client says
    # this is very useful in boot strapping bot design
    df['idx_client'] = df.groupby(
        [convo_id_col, 'role']).cumcount().where(df.role == 'client', 0)
    df['first_customer_utt'] = df['idx_client'] == 0
    df['second_customer_utt'] = df['idx_client'] == 1
    df['idx_client_max'] = df.groupby([convo_id_col])[
        'idx_client'].transform(numpy.max)
    df['final_customer_utt'] = df['idx_client'] == df['idx_client_max']
    metadata_keys.extend(
        ['idx', 'first_customer_utt', 'second_customer_utt', 'final_customer_utt'])

    # build metadata
    print(metadata_keys)
    df['metadata'] = df.apply(create_metadata,
                              args=[metadata_keys, dict_of_file_level_values],
                              axis=1)

    # build examples
    print("Commencing build examples")
    tqdm.tqdm.pandas()
    df = df.progress_apply(build_examples,
                           args=[utterance_col, convo_id_col, "created_at"],
                           axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    print("Adding examples to workpsace")
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    print("Commencing write")
    filename_out = filename.replace('.csv', '.json')
    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"Write complete to {filename_out}")

def sub_all(text:str, regex_to_sub: re, replacement: str) -> str:
    """use re.sub to replace using the re the text with the replacement"""
    return re.sub(regex_to_sub,replacement,text,count=0)


def build_examples(row: pandas.Series, utterance_col: str, convo_id_col: str = '', created_at_col: str = ''):
    '''Build the examples'''

    # if utterances use the hash of the utterance for an id
    if convo_id_col == '':
        external_id = humanfirst.hash_string(row[utterance_col], 'example')
        context = None

    # if convos use the convo id and sequence
    else:
        external_id = f'example-{row[convo_id_col]}-{row["idx"]}'
        context = humanfirst.HFContext(
            context_id=row[convo_id_col],
            type='conversation',
            role=row["role"]
        )

    # created_at
    if created_at_col == '':
        created_at = datetime.datetime.now().isoformat()
    else:
        created_at = row[created_at_col]

    # build examples
    example = humanfirst.HFExample(
        text=row[utterance_col],
        id=external_id,
        created_at=created_at,
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        context=context
    )
    row['example'] = example
    return row


def create_metadata(row: Union[pandas.Series, dict], metadata_keys_to_extract:
                    list, dict_of_values: dict = None) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    if dict_of_values is None:
        metadata = {}
    else:
        assert isinstance(dict_of_values, dict)
        metadata = dict_of_values

    for key in metadata_keys_to_extract:
        metadata[key] = str(row[key])

    return metadata


def get_timestamp(input_time_string: str, ms_to_add: int = 0) -> datetime.datetime:
    "get a timestamp from the input string using dateutil parser adding milliseconds where passed"
    output_time_string = parser.parse(input_time_string)
    output_time_string = output_time_string + \
        datetime.timedelta(milliseconds=ms_to_add)
    return output_time_string


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
