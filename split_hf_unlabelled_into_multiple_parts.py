"""
python split_hf_unlabelled_into_multiple_parts.py

If the dataset size is larger than limit of 80000000 bytes, then the dataset would not be uploaded to the tool.
This script can be used to resolve this issue.
This script splits the HF JSON file into the number of parts that user wants.
Then the individual parts of the file can be uploaded separately.

"""
# *********************************************************************************************************************

# standard imports
import json

# third Party imports
import pandas
import click
import numpy

# custom imports
import back_to_hf_unlabelled

@click.command()
@click.option('-f', '--filepath', type=str, required=True, help='Directory containing utterances as HF json')
@click.option('-p', '--parts', type=int, default=2, help='')
def main(filepath: str, parts: int) -> None:
    """Main Function"""

    with open(filepath, mode="r", encoding="utf-8") as utterance_file:
        data = json.load(utterance_file)

    df = pandas.json_normalize(data["examples"], sep="-")
    print(df.columns)
    df.sort_values(["context-context_id","created_at"],inplace=True)
    df.set_index(["context-context_id","created_at"],inplace=True,drop=True)

    # divide large dataframe into multiple parts
    dfs = numpy.array_split(df, parts)

    # parse through every part of the divided dataframe
    for i,sub_df in enumerate(dfs):

        # make sure every sub dataframe is actually a dataframe
        assert isinstance(sub_df, pandas.DataFrame)

        # suffixing the output file path with the part_no
        output_path = filepath.replace(".json",f"_{i}.json")

        # this prevents overwriting the file
        assert output_path != filepath

        # bring back the indexed column into normal columns
        sub_df.reset_index(inplace=True)

        # converts the dataframe back to unlabelled HF JSON format
        # back_to_hf function accepts dataframe, output file path,
        # and a list of any newly created column which needs to be added as metadata
        back_to_hf_unlabelled.back_to_hf(sub_df,output_path,[])


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
