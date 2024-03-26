"""
python ifbp_to_csv.py
"""
# ******************************************************************************************************************120

# standard imports
import json
import os
import glob

# 3rd party imports
import pandas
import click

@click.command()
@click.option('-f', '--folder_path', type=str, required=True, help='Input JSON folder Path')

def main(folder_path: str) -> None:
    """Main function"""

    folder_path = os.path.abspath(folder_path)

    # Check if the folder path exists
    if not os.path.exists(folder_path):
        print("Folder path does not exist.")
        quit()

    # Use glob to find JSON files in the folder
    json_files = find_json_files(folder_path)

    df_list = []
    for json_file in json_files:
        df_list.append(process(json_file))

    concatenated_df = pandas.concat(df_list, ignore_index=True)
    print(concatenated_df)
    print(concatenated_df.columns)
    print(concatenated_df.shape)

    output_csv = os.path.join(folder_path,"final_conversation.csv")
    concatenated_df.to_csv(output_csv, sep=",", encoding="utf", index=False)
    print(f"CSV is ready at {output_csv}")

def find_json_files(folder_path):
    """Returns list of json files in the folder"""

    json_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            assert isinstance(root,str)
            if root.find("__MACOSX") == -1:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
    return json_files

def process(filename: str) -> None:
    """Convert to CSV"""

    # load input data
    with open(filename, mode="r", encoding="utf8") as file:
        data = json.load(file)

    df = pandas.json_normalize(data=data["conversationExports"], sep="-")
    df.rename(columns={
        "id":"convo_id",
        "createdAt": "convo_createdAt",
        "topic":"convo_topic"}, inplace=True)
    df_exploded = df.explode("messages").reset_index(drop=True)
    df_exploded_dict = pandas.json_normalize(df_exploded["messages"])

    # Concatenate the exploded DataFrame with the original DataFrame
    df_result = pandas.concat([df_exploded[['convo_id',
                                            "convo_createdAt",
                                            "convo_topic"]], df_exploded_dict], axis=1)
    df_result.rename(columns={
        "id":"uttr_id",
        "createdAt": "uttr_createdAt"}, inplace=True)

    with open(filename,mode="w",encoding="utf8") as f:
        json.dump(data,f,indent=2)

    return df_result

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
