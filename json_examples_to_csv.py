"""
python.py json_examples_to_csv.py

Take a downloaded file from the Data pane GUI and turn it into a CSV

"""
# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import click
import pandas


@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str):
    """Main function"""

    # load json
    file = open(filename, mode="r", encoding="utf8")
    workspace_json = json.load(file)
    file.close()

    # normalise to DF
    df = pandas.json_normalize(workspace_json["examples"])

    # Output file
    output_filename = filename.replace(".json",".csv")
    assert output_filename != filename
    print(df)
    df.to_csv(output_filename,encoding="utf8",index=False)
    print(f'Wrote to: {output_filename}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
