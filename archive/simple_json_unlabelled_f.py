#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python simple_json_unlabelled.py -f <your input file relative path>
#
# *****************************************************************************

# standard imports
from datetime import datetime, timedelta
from typing import Union

# third party imports
import pandas
import numpy
import click
from dateutil import parser

# custom imports
import humanfirst


@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File')
def main(filename: str):
    """Main function"""

    # our example input file looks like this
    dtypes = {
        'created_at': str,
        'updated_at': str,
        'conversation_id': str,
        'conversation_message_id': str,
        'sender': str,
        'content': str,
        'content_type': str,
        'user_id': str,
        'user_name': str,
        'user_locale': str,
        'ref_uid': str,
        'type': str,
        'extra': str
    }

    # read the input csv
    df = pandas.read_excel(filename, dtype=dtypes, skiprows=1)
    df['external_id'] = df['conversation_id']
    df.rename(columns={'created_at':'convo_created_at','content':'utterance'},inplace=True)


    # map sender to role
    role_mapper = {
        'bot': 'expert',
        'user': 'client'  # by default only the client data is analysed
    }
    df['role'] = df['sender'].apply(translate_roles, args=[role_mapper])

    # index the speakers
    df['idx'] = df.groupby(["external_id"]).cumcount()
    df['idx_max'] = df.groupby(["external_id"])['idx'].transform(numpy.max)

    # This info lets you filter for the first or last thing the client says
    # this is very useful in boot strapping bot design
    df['idx_client'] = df.groupby(
        ['external_id', 'role']).cumcount().where(df.role == 'client', 0)
    df['first_customer_utt'] = df['idx_client'] == 0
    df['second_customer_utt'] = df['idx_client'] == 1
    df['idx_client_max'] = df.groupby(["external_id"])[
        'idx_client'].transform(numpy.max)
    df['final_customer_utt'] = df['idx_client'] == df['idx_client_max']
    
    # convert date_strings to get a created_at timestamp for each utterance (mandatory)
    # if you don't have one will need to generate one
    # must be 1ms difference minimum between utterances to sort properly
    # this input sheet doesn't have milliseconds
    # so we shall add the idx as milliseconds to maintain some difference
    df['created_at'] = df.apply(convert_timestamps,axis=1)

    # create metadata object per utterance
        
    metadata_keys_to_extract = ['created_at','updated_at','conversation_id','conversation_message_id','sender',
                                'content_type','user_id','user_name','user_locale','ref_uid','type','extra'] # utterance level info
    # generated custom indexing fields
    metadata_keys_to_extract.extend(
        ['idx', 'first_customer_utt', 'second_customer_utt', 'final_customer_utt'])
    dict_of_file_level_values = {'loaded_date': datetime.now().isoformat() } # if there is anythign you think relevant
    df['metadata'] = df.apply(create_metadata, args=[
                              metadata_keys_to_extract, dict_of_file_level_values], axis=1)

    # add any tags
    # generally we don't want to use tags on unlabelled data

    # index by conversation and utterance
    df = df.set_index(['external_id', 'idx'], drop=True)

    # build examples
    df = df.apply(build_examples, axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.objects.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    filename_out = filename.replace('.csv', '.json')
    filename_out = filename.replace('.xlsx', '.json')
    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    print(f'Wrote to {filename_out}')
    file_out.close()


def build_examples(row: pandas.Series):
    '''Build the examples'''

    # build examples
    example = humanfirst.objects.HFExample(
        text=row['utterance'],
        id=f'example-{row.name[0]}-{row.name[1]}',
        created_at=row['created_at'],
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        # this links the individual utterances into their conversation
        context=humanfirst.objects.HFContext(
            # any ID can be used recommend a hash of the text which is
            # repeatable or the external conversation id if there is one.
            str(row.name[0]),
            'conversation',  # the type of document
            row['role']  # the speakers role in the conversations
        )
    )
    row['example'] = example
    return row


def create_metadata(row: Union[pandas.Series, dict],
                    metadata_keys_to_extract: list,
                    dict_of_file_level_values:
                    dict = None
                    ) -> pandas.Series:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    metadata = {}  # metadata is a simple dict object
    if dict_of_file_level_values and len(dict_of_file_level_values.keys()) > 0:
        metadata = {}

    for key in metadata_keys_to_extract:
        if isinstance(row[key], list):
            metadata[key] = ','.join(row[key])
        else:
            metadata[key] = str(row[key])

    # all key value pairs must be strings
    keys = metadata.keys()
    for key in keys:
        try:
            assert isinstance(metadata[key], str)
        except AssertionError:
            print(f'Key: {key} value {metadata[key]} is not a string')

    return metadata


def convert_timestamps(row: pandas.Series) -> datetime:
    '''Add milliseconds based on idx to timestamp and produce datetime object'''
    return parser.parse(row["convo_created_at"]) + timedelta(milliseconds=row["idx"])


def translate_roles(role: str, mapper: dict) -> str:
    '''Translates abcd to hf role mapping'''
    try:
        return mapper[role]
    except KeyError as e:
        print(f'Couldn\'t locate role: "{role}" in role mapping')
        print(e)
        quit()


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
