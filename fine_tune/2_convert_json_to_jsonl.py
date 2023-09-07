#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ******************************************************************************************************************120
#
# python ./fine_tune/2_convert_json_to_jsonl.py
#
# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import click
import jsonlines

@click.command()
@click.option("-i", "--input_file", type=str, required=True, help="input file path")
def main(input_file: str) -> None:
    """Main Function"""

    with open(input_file, mode="r", encoding="utf8") as file_obj:
        data = json.load(file_obj)

    file_output = input_file.replace(".json",".jsonl")
    with jsonlines.open(file_output, 'w') as writer:
        for _, value in data.items():
            writer.write_all(value)

if __name__ == "__main__":
    main() # pylint: disable=no-value-for-parameter
