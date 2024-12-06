"""
python speechmatics_to_hf_csv.py

Converts Speechmatics JSON to HF CSV

Set up gcloud authentication.
Steps in ./google_storage_helpers.py

Run script and see which channel represents the client and which one expert.
Cause it varies for audios from different clients.
Client A audios might have channel_1:client and channel_2:expert
Client B audios might have channel_1:expert and channel_2:client

If using Speaker Diarization, these will be S1 and S2
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
from google_storage_helpers import GoogleStorageHelper # GCP helpers

BUCKET_BASE_URL = "https://storage.cloud.google.com/"
FOLDER_FILE_SPLIT_DELIMITER = "---"

@click.command()
@click.option('-f', '--folder_path', type=str, required=True, help='Speechmatics json folder')
@click.option('-b', '--bucket_name', type=str, default="",
              help='Name of the bucket containing the audio files to create audio url')
@click.option('-c', '--client_channel', type=click.Choice(['channel_1', 'channel_2', 'S1','S2']), default = "channel_2",
              help='Which channel has client utterances? channel_1 or channel_2? or S1 S2 in the case of speaker')
@click.option('-o', '--output_filename', type=str, required=True, help='FQN Where to save the output csv')
@click.option('-i', '--impersonate', is_flag=True, default=False, help='Impersonate service account or not')
@click.option('-s', '--impersonate_service_account', type=str, required=False, default="",
              help='Target service account to impersonate')
def main(folder_path: str,
         output_filename: str,
         client_channel: str,
         bucket_name: str,
         impersonate: str,
         impersonate_service_account: str) -> None:
    """Main Function"""

    bucket_exists = 0
    if bucket_name:
        gs_helper = GoogleStorageHelper(impersonate=impersonate,
                                        impersonate_service_account=impersonate_service_account)
        # gs_helper.storage_client.bucket(bucket_name)
        bucket_exists = gs_helper.is_bucket_exists(bucket_name=bucket_name)
        if bucket_exists:
            print(f"Bucket: {bucket_name} exists")
        else:
            print(f"Bucket: {bucket_name} doesn't exists. Check the given bucket name")


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
            df_list.append(process(data, client_channel, bucket_name, bucket_exists))
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

def process(data: str, client_channel: str, bucket_name: str, bucket_exists: int) -> pandas.DataFrame:
    """Speechmatics Json to HF consumable format"""

    df = pandas.json_normalize(data["results"], sep="-")
    print(df.columns.to_list())
    df["confidence"] = df["alternatives"].apply(lambda x: x[0]["confidence"])
    df["content"] = df["alternatives"].apply(lambda x: x[0]["content"])
    df["language"] = df["alternatives"].apply(lambda x: x[0]["language"])

    # speechmatics supports 2 types of diarization
    # if an audio file is speaker diarized, then have to extract the speaker information spearately
    if data["metadata"]["transcription_config"]["diarization"] == "speaker":
        df["channel"] = df["alternatives"].apply(lambda x: x[0]["speaker"])

    print(df.columns.to_list())

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
    merged_df["role"] = merged_df["channel"].apply(lambda x: "client" if x == client_channel else "expert")
    print(f'Rows for this conversation: {merged_df.shape[0]}')

    convo_id_col = "convo_guid"

    # index the speakers
    merged_df['idx'] = merged_df.groupby([convo_id_col]).cumcount()
    merged_df['idx_max'] = merged_df.groupby([convo_id_col])[
        'idx'].transform("max")

    # This info lets you filter for the first or last thing the client says
    # this is very useful in boot strapping bot design
    # 0s for expert
    merged_df['idx_client'] = merged_df.groupby(
        [convo_id_col, 'role']).cumcount().where(merged_df.role == 'client', 0)
    merged_df['idx_max_client'] = merged_df.groupby([convo_id_col])[
        'idx_client'].transform("max")
    merged_df['first_client_utt'] = merged_df.apply(decide_role_filter_values,
                                        args=['idx_client','client',0,"idx_max_client"],
                                        axis=1)
    merged_df['second_client_utt'] = merged_df.apply(decide_role_filter_values,
                                        args=['idx_client','client',1,"idx_max_client"],
                                        axis=1)
    merged_df['last_client_utt'] = merged_df.apply(decide_role_filter_values,
                                        args=['idx_client','client',-1,"idx_max_client"],
                                        axis=1)

    # same for expert
    merged_df['idx_expert'] = merged_df.groupby(
        [convo_id_col, 'role']).cumcount().where(merged_df.role == 'expert', 0)
    merged_df['idx_max_expert'] = merged_df.groupby([convo_id_col])[
        'idx_expert'].transform("max")
    merged_df['first_expert_utt'] = merged_df.apply(decide_role_filter_values,
                                        args=['idx_expert','expert',0,'idx_max_expert'],
                                        axis=1)
    merged_df['second_expert_utt'] = merged_df.apply(decide_role_filter_values,
                                        args=['idx_expert','expert',1,'idx_max_expert'],
                                        axis=1)
    merged_df['last_expert_utt'] = merged_df.apply(decide_role_filter_values,
                                        args=['idx_expert','expert',-1,'idx_max_expert'],
                                        axis=1)

    merged_df.drop(['idx',
                    'idx_max',
                    'idx_client',
                    'idx_max_client',
                    'idx_expert',
                    'idx_max_expert'], axis=1, inplace=True)

    # Generate utterance level audio bucket URL
    # Helps to listen to specific utterance in the audio
    if bucket_exists:
        merged_df["utterance_level_url"] = merged_df.apply(lambda x: os.path.join(
            BUCKET_BASE_URL,
            bucket_name,
            f"{x['recording_file']}#t={x['start_time']}".replace(FOLDER_FILE_SPLIT_DELIMITER,"/")),
            axis=1)

        # print(merged_df["url"])

    return merged_df


def decide_role_filter_values(row: pandas.Series,
                              column_name: str,
                              role_filter: str,
                              value_filter: str,
                              idx_max_col_name: str) -> bool:
    """Determine whether this is the 0,1,2 where the role is also somthing"""
    if value_filter >=0 and row[column_name] == value_filter and row["role"] == role_filter:
        return True
    elif value_filter < 0 and row[column_name] == row[
        idx_max_col_name] and row["role"] == role_filter:
        return True
    else:
        return False


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
