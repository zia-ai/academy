# pylint: disable=invalid-name
"""
python ./json_to_csv.py
       -f <YOUR FILENAME>

"""
# *********************************************************************************************************************
# standard imports

# third party imports
import click
import pandas
import json
import humanfirst

# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
def main(input_filename: str):
    file = open(input_filename, mode = "r", encoding = "utf8")
    workspace_json = json.load(file)
    file.close()
    df = pandas.json_normalize(workspace_json["examples"])
    print(df)
    assert input_filename.endswith(".json")
    output_filename = input_filename.replace(".json", "_output.csv")
    assert input_filename != output_filename
    df.to_csv(output_filename,index=False, header=True)
    print(f'wrote to: {output_filename}')

    workspace = humanfirst.objects.HFWorkspace().from_json(workspace_json,delimiter="-")
    assert isinstance(workspace, humanfirst.objects.HFWorkspace)
    print(workspace.get_fully_qualified_intent_name("intent-B7UM4L5SPNBWPHIOMNIQMDN6"))

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter