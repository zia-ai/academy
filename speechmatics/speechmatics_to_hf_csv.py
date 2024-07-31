"""
python speechmatics_to_hf_csv.py

Converts Speechmatics JSON to HF CSV
"""
# *********************************************************************************************************************

# standard imports
import json
import uuid
from datetime import datetime, timedelta
import os

# third Party imports
import pandas
import click

@click.command()
@click.option('-f', '--folder_path', type=str, required=True, help='Speechmatics json folder')
@click.option('-o', '--output_filename', type=str, required=True, help='FQN Where to save the output csv')
def main(folder_path: str, output_filename: str) -> None:
    """Main Function"""

    # List all files in the directory
    file_paths = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.json')]
    print(f'To process: {len(file_paths)}')
    df_list = []
    unprocessed_results = []
    for file_path in file_paths:
        with open(file_path, mode="r", encoding="utf-8") as utterance_file:
            data = json.load(utterance_file)
        if data["results"]:
            print(f"Processing: {file_path}")
            df_list.append(process(data))
        else:
            unprocessed_results.append(file_path)

    if df_list:
        final_df = pandas.concat(df_list,ignore_index=True)

        print(final_df)
        output_filename = os.path.join(output_filename)
        final_df.to_csv(output_filename,index=False, header=True)

    print(f"Number of processed transcriptions                  : {len(df_list)}")
    print(f"Number of unprocessed transcriptions (Empty results): {len(unprocessed_results)}")
    print(f"Unprocessed files: \n{unprocessed_results}")


def process(data: str) -> pandas.DataFrame:
    """Speechmatics Json to HF consumable format"""

    df = pandas.json_normalize(data["results"], sep="-")
    df["confidence"] = df["alternatives"].apply(lambda x: x[0]["confidence"])
    df["content"] = df["alternatives"].apply(lambda x: x[0]["content"])
    df["language"] = df["alternatives"].apply(lambda x: x[0]["language"])
    df = df.sort_values(by="start_time").reset_index(drop=True)

    merged_df = merge_info(df)
    merged_df["type"] = 'word'
    merged_df = merge_info(merged_df)

    # Use the current date and time as the base
    base_datetime = datetime.now()

    # Convert start_time to datetime
    merged_df['timestamp'] = merged_df['start_time'].apply(
        lambda x: f"{(base_datetime + timedelta(seconds=x)).isoformat()}Z")
    # merged_df["created_at"] = merged_df["created_at"].astype(str)

    # Add metadata
    merged_df["recording_file"] = data["job"]["data_name"]
    merged_df["diarization"] = data["metadata"]["transcription_config"]["diarization"]
    merged_df["accuracy_level"] = data["metadata"]["transcription_config"]["operating_point"]

    # generate conversation id
    merged_df["convo_guid"] = f"convo-{uuid.uuid4()}"

    # assign speakers
    merged_df["speaker"] = merged_df["channel"].apply(lambda x: "client" if x == "channel_1" else "expert")
    print(f'Rows for this conversation: {merged_df.shape[0]}')

    return merged_df


def merge_info(df:pandas.DataFrame) -> pandas.DataFrame:
    """merges contents, start time, and end time"""

    # Group by channel, but only group consecutive rows in the same channel
    df['group'] = (df['channel'] != df['channel'].shift(1)).cumsum()

    # Aggregate text, start_time, and end_time
    merged_df = df.groupby(['channel', 'group']).apply(lambda x: pandas.Series({
        'content': custom_join(x),
        'start_time': x['start_time'].min(),
        'end_time': x['end_time'].max(),
        'language': x["language"].iloc[0]
    }),include_groups=False).reset_index()

    merged_df = merged_df.sort_values(by="start_time").reset_index(drop=True)
    merged_df = merged_df.loc[merged_df["content"] != ""].reset_index(drop=True).drop(columns=["group"])

    return merged_df


def custom_join(series: pandas.Series) -> str:
    """Function to concatenate text correctly based on type"""

    texts = series['content'].tolist()
    types = series['type'].tolist()
    merged_text = ""
    for i,text in enumerate(texts):
        if types[i] == 'word':
            if i==0:
                merged_text += text
            else:
                merged_text += ' ' + text
        elif types[i] == 'punctuation':
            if i==0:
                continue
            merged_text += text
        elif types[i] == 'entity':
            if i==0:
                merged_text += text
            else:
                merged_text += ' ' + text
        else:
            raise RuntimeError("Invalid text type")
    return merged_text


def extract_content(alternatives: list) -> list:
    """Extract contents"""

    return alternatives[0]["confidence"],alternatives[0]["content"],alternatives[0]["language"]


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
