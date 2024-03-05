# pylint: disable=invalid-name
"""
python ./ada_to_csv.py
       -f <YOUR FILENAME>

Parse an ADA produced conversation file into a csv for unlabelled.

"""
# *********************************************************************************************************************

# standard imports
import datetime
import re
import json

# third party imports
import click
import pandas
import tqdm
from dateutil import parser

# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-c', '--client', type=str, required=True, help='Client XxY')
@click.option('-s', '--sample', type=int, required=False, default=0, help='Whether to sample')
@click.option('-r', '--random_state', type=int, required=False, default=666, help='Make sampling reproducable')

def main(input_filename: str, sample: int, random_state: int,client: str):
    """main function"""

    # Open file
    file = open(input_filename, mode = "r", encoding = "utf8")
    workspace_json = json.load(file)
    file.close()
    tqdm.tqdm.pandas()
    df = pandas.json_normalize(workspace_json)

    if sample > 0:
        df = df.sample(sample,random_state=random_state)

    # deal with Metavariables
    metavariable_keys = [
        "browser",
        "browser_version",
        "device",
        "initialurl",
        "introshown",
        "ip_address",
        "language",
        "last_answer_id",
        "last_question_asked",
        "user_agent",
    ]
    df = df.progress_apply(read_dict_col,axis=1,args=["Metavariables",metavariable_keys])
    assert isinstance(df,pandas.DataFrame)

    # deal with variable_keys
    variable_keys = [
        # apikey - very long and not filterable
        f"{client}_status",
        f"{client}_systemstatus",
        "customer_type",
        "first_name",
        # These don't seem to often be there
        "inEffectSince",
        "inEffectSinceText",
        "inEffectSince_reformated"
    ]
    df = df.progress_apply(read_dict_col,axis=1,args=["Variables",variable_keys])
    assert isinstance(df,pandas.DataFrame)

    # deal with splitting text
    speakers = ["BOT","CHATTER"]
    re_speakers = re.compile(f'({"|".join(speakers)}):[ ]*')
    df["list_text_dicts"] = df.apply(split_ada_text,args=[speakers,re_speakers],axis=1)
    df = df.explode("list_text_dicts").reset_index(drop=True)
    df = pandas.concat([df,pandas.json_normalize(df["list_text_dicts"])],axis=1)
    assert isinstance(df,pandas.DataFrame)


    # split down to months
    df["yyyy-mm"] = df["Date"].astype(str).str[0:7]
    print("Available colums are:")
    print(df.columns.to_list())
    for yearmonth in ["2023-10","2023-11","2023-12"]:
        df_output = df[df["yyyy-mm"]==yearmonth]
        assert input_filename.endswith(".json")
        output_filename = input_filename.replace(".json", f"_{yearmonth}output.csv")
        assert input_filename != output_filename
        df_output.to_csv(output_filename,index=False, header=True)
        print(f'wrote to: {output_filename}')

def split_ada_text(row: pandas.Series, speakers: list, re_splitter: re) -> list:
    """Split up the custom ada text format and produce a dict"""

    # Split up the text including the speaker
    list_of_texts = re_splitter.split(row["Chat Transcript"])
    try:
        convo_date = parser.parse(row["Date"]) # will add on seconds based on added_text
    except:
        convo_date = parser.parse("1999-01-01")
    speaker = ""
    split_text = ""
    output_list = []

    # Loop to assemble
    speaker = ""
    split_text = ""
    added_texts = 0
    for t in list_of_texts:
        assert isinstance(t, str)

        # Remove unwanted chars
        t = t.strip(" ")
        t = t.strip(":")
        # Check if in speaker
        if t in speakers:
            # if it is and we've already got speaker save that data
            if added_texts > 0:
                estimated_created_at = convo_date + datetime.timedelta(seconds=added_texts)
                estimated_created_at = str(estimated_created_at.isoformat())
                output_list.append(
                    {
                        "speaker": speaker,
                        "split_text": split_text.strip("\n"),
                        "estimated_created_at": estimated_created_at
                    }
                )
                # clear variables
                speaker = ""
                split_text = ""
            # assign new speaker
            speaker = t
        elif t == "":
            continue
        else:
            # add text on
            split_text = split_text + t
            # increment counter
            added_texts = added_texts + 1
    return output_list

def read_dict_col(row: pandas.Series, col_to_read: str, extract_these_keys: list) -> pandas.Series:
    """Split the BOT CHATTER FORMAT"""
    input_dict = json.loads(row[col_to_read])
    for key in extract_these_keys:
        try:
            row[key] = input_dict[key]
        except KeyError:
            row[key] = ""
    return row


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
