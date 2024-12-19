"""
python gong_to_hf_csv.py

Converts Gong transcription JSON to HF COnversation CSV

Accepts list of agent ids to perform role mappings to ensure conversion of transcripts to HF conversation format
"""
# *********************************************************************************************************************

# standard imports
import json
from datetime import datetime, timedelta
import os

# third Party imports
import pandas
import click


@click.command()
@click.option('-f', '--folder_path', type=str, required=True, help='Gong json folder')
@click.option('-e', '--expert_speaker_ids', type=str, required=True,
              help='Provide the agents speaker IDs as comma delimited string')
@click.option('-o', '--output_filename', type=str, required=True, help='FQN Where to save the output csv')
def main(folder_path: str,
         output_filename: str,
         expert_speaker_ids: str) -> None:
    """Main Function"""

    expert_speaker_ids = expert_speaker_ids.strip().split(",")
    expert_speaker_id_list = []
    for expert_id in expert_speaker_ids:
        expert_speaker_id_list.append(expert_id.strip())


    # List all files in the directory
    file_paths = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.json')]
    print(f'To process: {len(file_paths)}')
    df_list = []
    unprocessed_results = []
    for file_path in file_paths:
        with open(file_path, mode="r", encoding="utf-8") as utterance_file:
            data = json.load(utterance_file)
        if data["callTranscripts"]:
            print(f"Processing: {file_path}")
            df_list.append(process(data, expert_speaker_id_list))
        else:
            # If no transcripts are present in callTranscripts array
            unprocessed_results.append(file_path)

    if df_list:
        final_df = pandas.concat(df_list,ignore_index=True)

        print(final_df)
        output_filename = os.path.join(output_filename)
        final_df.to_csv(output_filename,index=False, header=True)

    print(f"Number of processed transcriptions                  : {len(df_list)}")
    print(f"Number of unprocessed transcriptions (Empty callTranscripts): {len(unprocessed_results)}")
    print(f"Unprocessed files: \n{unprocessed_results}")

def process(data: str, expert_speaker_id_list: str) -> pandas.DataFrame:
    """Gong Json to HF consumable format"""

    # TODO: Handle edge cases where there could be call transcript id with no transcriptions in it
    # Normalize with correct paths
    df = pandas.json_normalize(
        data,
        record_path=['callTranscripts', 'transcript', 'sentences'],
        meta=[
            ['callTranscripts','callId'],  # Access the callId at the top level
            ['callTranscripts', 'transcript', 'speakerId'],  # Access speakerId
            ['callTranscripts', 'transcript', 'topic']  # Access topic
        ]
    )

    # Rename columns for clarity
    df.columns = ['start', 'end', 'text', 'callId', 'speakerId', 'topic']

    # Define a base datetime for conversion (e.g., Epoch time)
    # The Unix epoch represents 1970-01-01 00:00:00 UTC
    # which is the starting point for time measurement in Unix-based systems.
    # for current date as starting point use - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    base_datetime = datetime(1970, 1, 1) 

    # Convert milliseconds to RFC3339
    df['timestamp'] = df['start'].apply(lambda ms: (base_datetime + timedelta(milliseconds=ms)).isoformat() + 'Z')
    # df['end_rfc3339'] = df['end'].apply(lambda ms: (base_datetime + timedelta(milliseconds=ms)).isoformat() + 'Z')

    # copy call transcript id to a new column so that one can be used as conversation id and other one as metadata
    df["convo_guid"] = df["callId"]

    # assign speakers
    df["role"] = df["speakerId"].apply(lambda x: "expert" if x in expert_speaker_id_list else "client")

    convo_id_col = "convo_guid"

    # index the speakers
    df['idx'] = df.groupby([convo_id_col]).cumcount()
    df['idx_max'] = df.groupby([convo_id_col])[
        'idx'].transform("max")

    # This info lets you filter for the first or last thing the client says
    # this is very useful in boot strapping bot design
    # 0s for expert
    df['idx_client'] = df.groupby(
        [convo_id_col, 'role']).cumcount().where(df.role == 'client', 0)
    df['idx_max_client'] = df.groupby([convo_id_col])[
        'idx_client'].transform("max")
    df['first_client_utt'] = df.apply(decide_role_filter_values,
                                        args=['idx_client','client',0,"idx_max_client"],
                                        axis=1)
    df['second_client_utt'] = df.apply(decide_role_filter_values,
                                        args=['idx_client','client',1,"idx_max_client"],
                                        axis=1)
    df['last_client_utt'] = df.apply(decide_role_filter_values,
                                        args=['idx_client','client',-1,"idx_max_client"],
                                        axis=1)

    # same for expert
    df['idx_expert'] = df.groupby(
        [convo_id_col, 'role']).cumcount().where(df.role == 'expert', 0)
    df['idx_max_expert'] = df.groupby([convo_id_col])[
        'idx_expert'].transform("max")
    df['first_expert_utt'] = df.apply(decide_role_filter_values,
                                        args=['idx_expert','expert',0,'idx_max_expert'],
                                        axis=1)
    df['second_expert_utt'] = df.apply(decide_role_filter_values,
                                        args=['idx_expert','expert',1,'idx_max_expert'],
                                        axis=1)
    df['last_expert_utt'] = df.apply(decide_role_filter_values,
                                        args=['idx_expert','expert',-1,'idx_max_expert'],
                                        axis=1)

    df.drop(['idx',
            'idx_max',
            'idx_client',
            'idx_max_client',
            'idx_expert',
            'idx_max_expert'], axis=1, inplace=True)

    print(df.columns)
    print(df)

    return df


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


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
