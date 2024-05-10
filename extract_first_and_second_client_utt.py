"""
extract_first_and_second_client_utt.py

Any long set of conversations (for instance ABCD) that you want to take just the first or second client utterances.

Notes: 
Input files should be in HF json format with first_client_utt,second_client_utt,idx metadata fields
These metadata are automatically available if the input data files are produced using csv_to_json_unlabelled.py script
"""
# *********************************************************************************************************************

# standard imports
import os
import json

# third party imports
import click
import pandas

# custom imports
import back_to_hf_unlabelled

@click.command()
@click.option('-i','--input_location',type=str,required=True,
              help='a file or a directory containing conversation in HF JSON format')
@click.option('-h','--sample',type=int,required=False,default=0,help='Sample head')
@click.option('-s','--sep',type=str,required=False,default="-",help='Seperator')
@click.option('-f','--file_num',type=int,required=False,default=0,help='Files to stop after')
@click.option('-r','--remove_list',type=str,required=False,default="WELCOME,NO_RESPONSE,NO_RESPONSE_FINAL",
        help='List of utterances to be excluded from the final dataset')
def main(input_location: str,
         sample: int,
         sep: str,
         file_num: int,
         remove_list: str):
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

    work_list.sort()
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
    df = process(df,remove_list)

    # output location
    if os.path.isfile(input_location):
        output_location = input_location.replace(".json","_output.json")
    elif os.path.isdir(input_location):
        output_location = os.path.join(input_location,"output.json")
    assert input_location != output_location

    # turn it back into
    back_to_hf_unlabelled.back_to_hf(df,output_location)

def sample_dataframe(df: pandas.DataFrame, sample: int) -> pandas.DataFrame:
    """Sample by conversation ids"""
    if sample > 0:
        context_ids = df["context-context_id"].unique()
        context_ids = context_ids[0:sample]
        df = df[df["context-context_id"].isin(context_ids)].copy(deep=True)
    df["intidx"] = df["metadata-idx"].astype(int)
    df.sort_values(["context-context_id","intidx"],inplace=True,ignore_index=True)
    df.drop(columns=["intidx"],inplace=True)
    return df

def process(df: pandas.DataFrame, remove_list: str):
    """Extracts first or second client utterances"""

    # make it just the first or second customer utterances
    df = df[(df["metadata-first_client_utt"]=="True") | (df["metadata-second_client_utt"]=="True")]

    print(df[["text","context-context_id","metadata-idx","metadata-first_client_utt","metadata-second_client_utt"]])

    # groupby text
    print(df[["text","metadata-idx"]].groupby("text").count().sort_values("metadata-idx",ascending=False))

    # eliminate the utterances from the remove list
    if remove_list != "":
        remove_list = remove_list.split(",")
        for t in remove_list:
            df = strip_string(df,t)

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
