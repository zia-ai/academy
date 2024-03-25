# pylint: disable=invalid-name
"""
python ./json_to_csv.py
       -f <YOUR FILENAME>

"""
# *********************************************************************************************************************
# standard imports
import json

# third party imports
import click
import pandas


# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-l', '--object_list_key', type=str, required=False,
              default="examples", help='Object list key to load from')
def main(input_filename: str, object_list_key: str):
    file = open(input_filename, mode = "r", encoding = "utf8")
    workspace_json = json.load(file)
    file.close()
    df = pandas.json_normalize(workspace_json[object_list_key])
    print(df)
    assert input_filename.endswith(".json")
    output_filename = input_filename.replace(".json", "_output.csv")
    assert input_filename != output_filename
    df.to_csv(output_filename,index=False, header=True)
    print(f'wrote to: {output_filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
