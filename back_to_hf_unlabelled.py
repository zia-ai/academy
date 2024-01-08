"""
python back_to_hf.py

This script demonstrates how to convert a dataframe created from unlabelled HF JSON back to HF JSON with addition of
any newly created metadata.

The back_to_hf function can be used in other scripts in order to convert dataframe read from HF JSON back to HF JSON
back_to_hf accepts 3 args:
  df: pandas.DataFrame
  file_output: str
  metadata_col: list
If there isn't any newly created metadata, then the metadata column can be None or empty list

Following is an demonstration of what this script does.
- Reading unlabelled json as dataframe
- Addition of 3 new columns
- Providing 2 newly created columns as metadata to the back_to_hf function
- The back_to_hf function ignores the newly created column that is not available in the metadata list
  and produces the HF formatted json file
"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import pandas
import click

class IncorrectColumnNameException(Exception):
    """This happens when a metadata column provided does not belong to the dataframe"""

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input HF JSON File Path')
def main(filename: str) -> None:
    """Main Function"""

    # load input data
    with open(filename, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-")

    df["col1"] = "Test1"
    df["col2"] = "Test2"
    df["col3"] = "Test3"

    file_output = filename.replace(".json","_demonstration_of_back_to_hf.json")

    metadata_col_list = ["col2","col3"] # Should provide only the newly added columns

    back_to_hf(df=df,
               file_output=file_output,
               metadata_col=metadata_col_list)

def back_to_hf(df: pandas.DataFrame, file_output: str, metadata_col: list = None):
    """Converts the data back to HumanFirst format"""

    if metadata_col is None:
        metadata_col = []

    df["id"] = df["id"].astype(str)
    df["text"] = df["text"].fillna("")
    df["created_at"] = df["created_at"].astype(str)


    columns = ['id', 'text', 'created_at',
                  'intents', 'tags', 'context-context_id',
                  'context-type', 'context-role']

    df_cols = df.columns.to_list()

    for col in metadata_col:
        if col not in df_cols:
            raise IncorrectColumnNameException(f"Metadata column {col} does not exist in the dataframe")

    drop_list = []
    for col in df_cols:
        if (not col in columns) and (not col.find("metadata") >= 0) and (not col in metadata_col):
            drop_list.append(col)

    if len(drop_list) > 0:
        df.drop(columns=drop_list,inplace=True)

    rename_col = {}
    for col in metadata_col:
        df[col] = df[col].astype(str)
        rename_col[col] = f"metadata-{col}"

    df.rename(columns=rename_col, inplace=True)

    file_json = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json"
    }
    file_json["examples"] = df_to_formatted_json(df,sep="-")

    with open(file_output,mode="w",encoding="utf8") as f:
        json.dump(file_json,f,indent=2)

    print(f"HF formatted file is written at {file_output}")


def make_formatted_dict(my_dict, key_arr, val):
    """Set val at path in my_dict defined by the string (or serializable object) array key_arr"""

    current = my_dict
    for i,_ in enumerate(key_arr):
        key = key_arr[i]
        if key not in current:
            if i == len(key_arr)-1:
                current[key] = val
            else:
                current[key] = {}
        else:
            if not isinstance(current[key], dict):
                print("Given dictionary is not compatible with key structure requested")
                raise ValueError("Dictionary key already occupied")
        current = current[key]

    return my_dict


def df_to_formatted_json(df, sep=".") -> list:
    """Convert df to json"""

    result = []
    for _, row in df.iterrows():
        parsed_row = {}
        for idx, val in row.items():
            if idx.find("metadata")!=-1:
                val = str(val)
            if idx == "intents" or idx == "tags":
                val = []
            keys = idx.split(sep)
            parsed_row = make_formatted_dict(parsed_row, keys, val)
        result.append(parsed_row)

    return result


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
