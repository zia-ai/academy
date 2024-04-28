"""
trim.py
"""
# *****************************************************************************

# standard imports
import os
import json

# third party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-i','--input_location',type=str,required=True,help='a file or a directory')
@click.option('-h','--sample',type=int,required=False,default=0,help='Sample head')
@click.option('-s','--sep',type=str,required=False,default="-",help='Seperator')
def main(input_location: str,
         sample: int,
         sep: str):
    """Main Function"""

    # deal with file or directory
    work_list = []
    if os.path.isfile(input_location):
        work_list.append(input_location)
    elif os.path.isdir(input_location):
        candidate_list = os.listdir(input_location)
        for c in candidate_list:
            if c.endswith(".json"):
                work_list.append(os.path.join(input_location,c))
    print("Worklist of files is:")
    print(work_list)

    # main process loop
    for file_name in work_list:
        process(read_json_to_df(file_name, sample, sep))

def process(df: pandas.DataFrame):
    """Do the things"""
    df = strip_string(df,"WELCOME")
    # df = truncate_at_idx(df,3)
    print(df[["text","context-context_id","metadata-idx","metadata-first_client_utt","metadata-second_client_utt"]])

def truncate_at_idx(df: pandas.DataFrame, truncate_after: int) -> pandas.DataFrame:
    """Remove IDX after this"""
    df = df[df["metadata-idx"] <= truncate_after]
    return df

def strip_string(df: pandas.DataFrame, strip_this: str) -> pandas.DataFrame:
    """Strips any utterances matching exactly string"""
    print(f'Before stripping {strip_this}')
    print(df.shape)
    print(df.columns)
    df = df[~(df["text"]==strip_this)]
    print(f'After  stripping {strip_this}')
    print(df.shape)
    return df


def read_json_to_df(file_name: str, sample: int, sep: str) -> pandas.DataFrame:
    """Get a sampled data frame"""

    file_handle = open(file_name, mode="r", encoding="utf8")
    data = json.load(file_handle)
    data = data["examples"]
    file_handle.close()
    df = pandas.json_normalize(data=data, sep=sep)
    if sample > 0:
        context_ids = df["context-context_id"].unique()
        context_ids = context_ids[0:sample]
        df = df[df["context-context_id"].isin(context_ids)]
    df.sort_values(["context-context_id","metadata-idx"],inplace=True,ignore_index=True)
    return df

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
