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
import back_to_hf_unlabelled

@click.command()
@click.option('-i','--input_location',type=str,required=True,help='a file or a directory')
@click.option('-h','--sample',type=int,required=False,default=0,help='Sample head')
@click.option('-s','--sep',type=str,required=False,default="-",help='Seperator')
@click.option('-f','--file_num',type=int,required=False,default=0,help='Files to stop after')
def main(input_location: str,
         sample: int,
         sep: str,
         file_num: int):
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

    # Read all the files
    for i,file_name in enumerate(work_list):
        print(f'{i} Processing: {file_name}')
        df = pandas.DataFrame()
        df = pandas.concat([df,read_json_to_df(file_name, sep)])
        if file_num > 0 and (i + 1) == file_num:
            break
    print(f'All files read shape: {df.shape}')

    # sample the dataframe
    df = sample_dataframe(df,sample)

    # do the stuff
    df = process(df)

    # output location
    if os.path.isfile(input_location):
        output_location = input_location.replace(".json","_output.json")
    elif os.path.isdir(input_location):
        output_location = os.path.join(input_location,"output.json")
    assert input_location != output_location

    # turn it back into
    back_to_hf_unlabelled.back_to_hf(df,output_location)

def sample_dataframe(df: pandas.DataFrame, sample: int) -> pandas.DataFrame:
    if sample > 0:
        context_ids = df["context-context_id"].unique()
        context_ids = context_ids[0:sample]
        df = df[df["context-context_id"].isin(context_ids)].copy(deep=True)
    df["intidx"] = df["metadata-idx"].astype(int)
    df.sort_values(["context-context_id","intidx"],inplace=True,ignore_index=True)
    df.drop(columns=["intidx"],inplace=True)
    return df

def process(df: pandas.DataFrame):
    """Do the things"""
    # df = strip_string(df,"WELCOME")
    # df = truncate_at_idx(df,3)
    # df = df[df["context-context_id"]=="CA6687da219a88323bc6a93d6d4a161611"]

    # make it just the first or second customer utterances
    df = df[(df["metadata-first_client_utt"]=="True") | (df["metadata-second_client_utt"]=="True")]

    # blah
    print(df[["text","context-context_id","metadata-idx","metadata-first_client_utt","metadata-second_client_utt"]])

    # groupby summary
    print(df[["text","metadata-idx"]].groupby("text").count().sort_values("metadata-idx",ascending=False))

    # eliminate these
    for t in ["WELCOME","NO_RESPONSE","NO_RESPONSE_FINAL"]:
        df = strip_string(df,t)

    return df


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


def read_json_to_df(file_name: str, sep: str) -> pandas.DataFrame:
    """Just read data frame"""

    file_handle = open(file_name, mode="r", encoding="utf8")
    data = json.load(file_handle)
    data = data["examples"]
    file_handle.close()
    df = pandas.json_normalize(data=data, sep=sep)
    return df

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
