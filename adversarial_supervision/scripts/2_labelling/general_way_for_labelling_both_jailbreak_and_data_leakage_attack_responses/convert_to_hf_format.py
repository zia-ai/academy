#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./adversarial_supervision/scripts\
#         /2_labelling\
#         /general_way_for_labelling_both_jailbreak_and_data_leakage_attack_responses\
#         /convert_to_hf_format.py                                                       # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# standard imports
import os
from pathlib import Path
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
HF_MODULE_PATH = str(Path(DIR_PATH).parent.parent.parent.parent)
sys.path.insert(1, HF_MODULE_PATH)

# 3rd party imports
import pandas # pylint: disable=wrong-import-position
import click # pylint: disable=wrong-import-position

# custom imports
import humanfirst # pylint: disable=wrong-import-position,import-error


@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File')
def main(filename: str):
    """Main Function"""
    process(filename)


def process(filename: str) -> None:
    """Converts unlabelled dataset to HF JSON format"""

    # read the input csv
    df = pandas.read_csv(filename, sep=",", encoding='utf8')

    # index by conversation and utterance
    df = df.set_index(['id'], drop=True)

    # build examples
    df = df.apply(build_examples, axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    filename_out = filename.replace('.csv', '_load.json')
    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"Dataset is saved at {filename_out}")


def build_examples(row: pandas.Series):
    '''Build the examples'''

    # build examples
    example = humanfirst.HFExample(
        text=row['response'].strip(),
        id=f'example-{row.name}',
        created_at="",
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata={"id": row.name,
                  "prompt": row["final_prompt"]},
        # this links the individual utterances into their conversation
        context=humanfirst.HFContext()
    )
    row['example'] = example
    return row


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
