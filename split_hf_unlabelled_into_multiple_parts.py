"""
python split_hf_unlabelled_into_multiple_parts.py

"""
# *****************************************************************************

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
    df.sort_values(["context-context_id","metadata-idx"],inplace=True)
    df.set_index(["context-context_id","metadata-idx"],inplace=True,drop=True)
    dfs = numpy.array_split(df, parts)

    for i,sub_df in enumerate(dfs):
        assert isinstance(sub_df, pandas.DataFrame)
        output_path = filepath.replace(".json",f"_{i}.json")
        sub_df.reset_index(inplace=True)
        back_to_hf_unlabelled.back_to_hf(sub_df,output_path,[])


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
