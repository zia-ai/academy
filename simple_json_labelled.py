"""
python simple_json_labelled.py
basic hardcoded example

"""
# ******************************************************************************************************************120

# standard imports
import json
from datetime import datetime, timedelta
from dateutil import parser

# 3rd party imports
import click
import humanfirst


@click.command()
@click.option('-f', '--filename', type=str, required=False, default="./examples/json_model_example.json",
              help='Input File')
@click.option('-d', '--create_date', is_flag=True, default=False, help='Input File')
def main(filename: str, create_date: bool) -> None:
    """Main Function"""

    # read the json here
    input_json = json.loads(open(filename, 'r', encoding='utf8').read())

    process(input_json, filename, create_date)


def process(input_json: dict, filename: str, create_date: bool = False) -> None:
    """Process Function"""

    # declare a labelled workspace
    labelled = humanfirst.objects.HFWorkspace()

    # add a loop through the json file and for every example in it create the intent and example
    intent_names = list(input_json.keys())
    i = 0
    for name in intent_names:

        # get the group from the intent name or where-ever
        assert isinstance(name, str)
        group = name.split('_')[0]

        # create the intent name in the workspace
        # - this gives us the intent to associate with each example for this intent we are importing.
        intents = [labelled.intent(name_or_hier=[group, name])]

        # get the full input_intent for the language we are interested in
        input_intent = input_json[name]['EN']
        assert isinstance(input_intent, dict)
        set_at = parser.parse("2023-06-05T14:26:07Z")
        for example_id in list(input_intent.keys()):
            if not create_date:
                created_at = datetime.now()
            else:
                created_at = set_at + timedelta(seconds=i)
            # build example
            example = humanfirst.objects.HFExample(
                text=input_intent[example_id],
                id=example_id,
                created_at=created_at.isoformat(),
                intents=intents,
                tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
                metadata={},
            )

            # add example to workspace
            labelled.add_example(example)
            i = i+1

    # write to output
    file_out = open(filename.replace(".json", "_output.json"),
                    mode='w', encoding='utf8')
    labelled.write_json(file_out)
    file_out.close()


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
