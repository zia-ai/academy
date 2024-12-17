"""
python gong_to_hf_utterance_csv.py

Converts Gong transcription JSON to HF Utterance format

Entire conversation is converted into a single text chunk with each utterance prefixed with speaker metadata.
"""
# *********************************************************************************************************************

# standard imports
import json
import os

# third Party imports
import pandas
import click


@click.command()
@click.option('-f', '--folder_path', type=str, required=True, help='Gong json folder')
@click.option('-o', '--output_filename', type=str, required=True, help='FQN Where to save the output csv')
def main(folder_path: str,
         output_filename: str) -> None:
    """Main Function"""

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
            df_list.append(process(data))
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

def process(data: str) -> pandas.DataFrame:
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

    # Create a mapping of unique speakerIds to S1, S2, etc.
    unique_speakers = df['speakerId'].unique()
    speaker_mapping = {speaker: f'S{i+1}' for i, speaker in enumerate(unique_speakers)}

    # Create reverse mapping for getting speaker IDs
    reverse_speaker_mapping = {f'S{i+1}': speaker for i, speaker in enumerate(unique_speakers)}

    # Add the speaker prefix to the text
    df['text'] = df.apply(lambda row: f"{speaker_mapping[row['speakerId']]}: {row['text']}", axis=1)

    # Group by callId and get required aggregations
    merged_df = df.groupby('callId').agg({
        'text': lambda x: '\n'.join(x), # pylint: disable=unnecessary-lambda
        'topic': 'first',  # Keep the first topic for each callId
        'speakerId': lambda x: ','.join(sorted(set(x))),  # Get unique speaker IDs as comma-separated string
        'start': 'min',    # Get the earliest start time
        'end': 'max'       # Get the latest end time
    }).reset_index()

    # Rename topic column to first_topic
    merged_df = merged_df.rename(columns={'topic': 'first_topic',
                                          'speakerId': 'speakerIds'})

    # Add S1 and S2 columns
    merged_df['S1'] = reverse_speaker_mapping.get('S1', '')
    merged_df['S2'] = reverse_speaker_mapping.get('S2', '')

    return merged_df


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
