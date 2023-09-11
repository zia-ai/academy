#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *********************************************************************************************************************
#
# python csv_to_json_unlabelled.py
#
# *********************************************************************************************************************

# standard imports
import json
import datetime
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
@click.option('-m', '--metadata_keys', type=str, required=False, default='',
              help='<metadata_col_1,metadata_col_2,...,metadata_col_n>')
@click.option('-u', '--utterance_col', type=str, required=True,
              help='Column name containing utterances')
@click.option('-d', '--delimiter', type=str, required=False, default=",",
              help='Delimiter for the csv file')
@click.option('-c', '--convo_id_col', type=str, required=False, default='',
              help='If conversations which is the id otherwise utterances and defaults to hash of utterance_col')
@click.option('-t', '--created_at_col', type=str, required=False, default='',
              help='If there is a created date for utterance otherwise defaults to now')
@click.option('-x', '--unix_date', is_flag=True, type=bool, required=False, default=False,
              help='If created_at column is in unix epoch format')
@click.option('-r', '--role_col', type=str, required=False, default='',
              help='Which column the role in ')
@click.option('-p', '--role_mapper', type=str, required=False, default='',
              help='If role column then role mapper in format "source_client:client,source_expert:expert,*:expert}"')
@click.option('-e', '--encoding', type=str, required=False, default='utf8',
              help='Input CSV encoding')
@click.option('--filtering', type=str, required=False, default='', help='column:value,column:value')
def main(filename: str, metadata_keys: str, utterance_col: str, delimiter: str,
         convo_id_col: str, created_at_col: str, unix_date: bool, role_col: str,
         role_mapper: str, encoding: str, filtering: str) -> None:
    """Main Function"""

    excel = False
    if filename.endswith('.xlsx'):
        print("Processing excel")
        excel = True

    if metadata_keys == '':
        metadata_keys = []
    else:
        metadata_keys = list(metadata_keys.split(","))
    used_cols = metadata_keys
    assert isinstance(used_cols, list)
    for col in [utterance_col, convo_id_col, created_at_col, role_col]:
        if col != '':
            used_cols.append(col)
    print(f'used_cols: {used_cols}')
    print('\n')

    # read the input csv only for the columns we care about - all as strings
    if not excel:
        df = pandas.read_csv(filename, encoding=encoding,
                             usecols=used_cols, dtype=str, delimiter=delimiter)
    else:
        df = pandas.read_excel(filename, usecols=used_cols, dtype=str)
    assert isinstance(df, pandas.DataFrame)
    df.fillna('', inplace=True)

    assert isinstance(metadata_keys, list)

    # assume role all to start with and overwrite later
    df['role'] = 'client'

    # filtering
    if filtering != '':
        print(f'Before filtering: {df.shape[0]}')
        filters = filtering.split(',')
        print(filters)
        filtering = {}
        for filt in filters:
            pair = filt.split(':')
            filtering[pair[0]] = pair[1]
        print('Filtering on:')
        print(filtering)
        assert isinstance(filtering, dict)
        for key, value in filtering.items():
            df = df[df[key] == value]
        print(f'After filtering: {df.shape[0]}')
        print('\n')

    # if convos index them
    if convo_id_col != '':

        # must have created_at date if convo index
        if created_at_col == '':
            raise KeyError(
                'Must have created_at_col to sort the data if convo_id_col is present and these are conversations')
        df.sort_values([convo_id_col, created_at_col], inplace=True)

        # check whether have any column clashes
        if created_at_col == 'created_at':
            created_at_col = 'created_at_input'
            df.rename(columns={'created_at': created_at_col}, inplace=True)

        # parse dates
        if unix_date:
            df[created_at_col] = df[created_at_col].astype(float)
            df['created_at'] = df[created_at_col].apply(
                datetime.datetime.fromtimestamp)
            print('Dates are:')
            print(df)
            print('\n')
        else:
            df['created_at'] = df[created_at_col].apply(parser.parse)

        # check roles
        if role_col == '':
            raise KeyError('Must have role_col if conv')

        # work out role mapper
        assert isinstance(role_mapper, str)
        if role_mapper == '':
            print('Warning no role mapper using defaults:')
            role_mapper = {
                'client': 'client',
                'expert': 'expert'
            }
        else:
            roles = role_mapper.split(',')
            print('Roles are:')
            print(roles)
            print('\n')
            role_mapper = {}
            assert isinstance(role_mapper, dict)
            for role in roles:
                pair = role.split(':')
                role_mapper[pair[0]] = pair[1]
        print(json.dumps(role_mapper, indent=2))
        print('\n')

        # produce roles
        df['role'] = df[role_col].apply(translate_roles, args=[role_mapper])
        print('Role summary:')
        print(df[['role', role_col, convo_id_col]].groupby(
            ['role', role_col]).count())
        print('\n')

        # index the speakers
        df['idx'] = df.groupby([convo_id_col]).cumcount()
        df['idx_max'] = df.groupby([convo_id_col])[
            'idx'].transform(numpy.max)

        # This info lets you filter for the first or last thing the client says
        # this is very useful in boot strapping bot design
        df['idx_client'] = df.groupby(
            [convo_id_col, 'role']).cumcount().where(df.role == 'client', 0)
        df['first_customer_utt'] = df['idx_client'] == 0
        df['second_customer_utt'] = df['idx_client'] == 1
        df['idx_client_max'] = df.groupby([convo_id_col])[
            'idx_client'].transform(numpy.max)
        df['final_customer_utt'] = df['idx_client'] == df['idx_client_max']

        # make sure convo id on the metadata as well for summarisation linking
        metadata_keys.append(convo_id_col)

        # extend metadata_keys to indexed fields for conversations.
        # generated custom indexing fields
        metadata_keys.extend(
            ['idx', 'first_customer_utt', 'second_customer_utt', 'final_customer_utt'])

    # build metadata for utterances or conversations
    dict_of_file_level_values = {'loaded_date': datetime.datetime.now(
    ).isoformat(), 'script_name': 'csv_to_json_unlaballed.py'}
    print(f'metadata_keys: {metadata_keys}')
    print(f'file_level values: {dict_of_file_level_values}')
    df['metadata'] = df.apply(create_metadata, args=[
                              metadata_keys, dict_of_file_level_values], axis=1)

    # build examples
    print("Commencing build examples")
    tqdm.tqdm.pandas()
    df = df.progress_apply(build_examples,
                           args=[utterance_col, convo_id_col, "created_at"], axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    print("Adding examples to workpsace")
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    print("Commencing write")
    for ending in ['.csv', '.xlsx']:
        if filename.endswith(ending):
            filename_out = filename.replace(ending, '.json')
            break
    assert filename != filename_out

    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"Write complete to {filename_out}")


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

    metadata = {}
    if not dict_of_values is None:
        assert isinstance(dict_of_values, dict)
        metadata = dict_of_values

    for key in metadata_keys_to_extract:
        metadata[key] = str(row[key])
    return metadata.copy()


def translate_roles(role: str, mapper: dict) -> str:
    '''Translates abcd to hf role mapping'''
    try:
        return mapper[role]
    except KeyError as exc:
        if "*" in mapper.keys():
            return mapper["*"]
        raise KeyError(
            f'Couldn\'t locate role: "{role}" in role mapping') from exc


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
