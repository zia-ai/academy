"""
 Code Language:   python
 Script:          hf_labelled_to_unlabelled.py
 Imports:         click, os
 Functions:       main(), process(), delete_intent_ids_from_examples()
 Description:     Converts labelled HF data to Unlabelled format

"""
# **********************************************************************************************************************

# standard imports
import json
from os.path import isfile

# 3rd party imports
import click


@click.command()
@click.option('-f', '--filepath', type=str, required=True, help='HF labelled json file path')
@click.option('-o', '--output_filepath', type=str, default='', help='HF unlabelled json file path')
def main(filepath: str, output_filepath: str) -> None:
    '''Main Function'''

    process(filepath, output_filepath)


def process(filepath: str, output_filepath: str) -> None:
    '''Convert labelled to unlabelled file'''

    if isfile(filepath):
        with open(filepath, mode="r", encoding="utf8") as f:
            data = json.load(f)
    else:
        raise FileNotFoundError(f"{filepath} does not exist")

    if "intents" in data.keys():
        del data["intents"]

    if "tags" in data.keys():
        del data["tags"]

    if "examples" in data.keys():
        data["examples"] = delete_intent_ids_from_examples(data["examples"])

    if output_filepath == '':
        output_filepath = filepath.split("/")
        output_filepath[-1] = "unlabelled.json"
        output_filepath = '/'.join(output_filepath)

    with open(output_filepath, mode="w", encoding="utf8") as f:
        json.dump(data, f, indent=3)


def delete_intent_ids_from_examples(examples: list) -> list:
    '''deletes intent ids from examples'''

    for i,_ in enumerate(examples):
        if "intents" in examples[i].keys():
            del examples[i]["intents"]

    return examples


if __name__ == "__main__":
    main() # pylint: disable=no-value-for-parameter
