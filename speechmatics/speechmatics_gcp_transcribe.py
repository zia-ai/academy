"""
python speechmatics_gcp_transcribe

This is a restartable transcriber wokring with google buckets

Make sure your ADC for google is setup (hint: ../google/get_blob_list.py)

Make sure your speechmatics is setup (hint: speechmatcis_audio_transcribe.py)

Takes a bucket name.
Looks for wavs.
Compares wavs that have been transcribed with ones that haven't been
Starts transcribing

"""

# *********************************************************************************************************************

# standard imports
import os
import json

# 3rd party imports
import click

# custom imports
import speechmatics_helpers # Humanfirst Speechmatics helps
from google_storage_helpers import GoogleStorageHelper # GCP helpers

@click.command()
@click.option('-a', '--api_key', type=str, required=True, help='Api key from speechmatics portal')
@click.option('-b', '--bucket_name', type=str, required=True, help='Bucket name to read/write to')
@click.option('-t', '--audio_type', type=str, required=True, help='.wav .mp3 to search for')
@click.option('-w', '--working_dir', type=str, required=True, help='Dir to download to')
@click.option('-v', '--vocab_file_path', type=str, required=False, default = "", help='Additional Vocabulary file path')
@click.option('-i', '--impersonate', is_flag=True, default=False, help='Impersonate service account or not')
@click.option('-s', '--impersonate_service_account', type=str, required=False, default="",
              help='Target service account to impersonate')
@click.option('-n', '--process_n', type=int, required=False,default=0,
              help='Maximum number to process')
@click.option('-u', '--write_back', is_flag=True, required=False, default=False,
              help='Whether to write back to GCP')
def main(
        api_key: str,
        bucket_name: str,
        audio_type: str,
        working_dir: str,
        process_n: int,
        write_back: bool,
        impersonate: bool,
        impersonate_service_account: str,
        vocab_file_path: str) -> None:
    """Main Function"""

    # setup speechmatics
    settings = speechmatics_helpers.get_connection_settings(api_key)
    transcription_configuration = speechmatics_helpers.get_transcription_configuration()
    # print(json.dumps(transcription_configuration,indent=2))

    if vocab_file_path:
        if os.path.exists(vocab_file_path):
            with open(vocab_file_path, mode="r", encoding="utf8") as file:
                additional_vocab = json.load(file)
                transcription_configuration[
                    "transcription_config"]["additional_vocab"] = additional_vocab["additional_vocab"]
        else:
            raise RuntimeError(f"{vocab_file_path} doesn't exist")

    gs_helper = GoogleStorageHelper(impersonate=impersonate,
                                    impersonate_service_account=impersonate_service_account)

    df_worklist = gs_helper.get_blob_df_worklist(bucket_name, audio_type, max_results=process_n)
    print(df_worklist)
    print(f'Total: {df_worklist.shape[0]}')
    print(f'To do: {df_worklist.loc[df_worklist["done"] == False].shape[0]}')
    print(f'Done:  {df_worklist.loc[df_worklist["done"] == True].shape[0]}')

    if process_n > 0:
        df_worklist = df_worklist.sample(process_n)


    for i,row in df_worklist.iterrows():

        # replace "/" with "---"
        input_file = row["source_name"].replace("/","---")

        # Download file
        gs_helper.download_blob_to_file(bucket_name, row["source_name"], os.path.join(working_dir, input_file))

        # TODO: auto detect language
        transcript = speechmatics_helpers.get_transcript(os.path.join(working_dir, input_file),
                                                        settings,
                                                        transcription_configuration)
        output_file = input_file.replace(audio_type,".json")

        # Dump transcript locally
        with open(os.path.join(working_dir,output_file),mode="w",encoding="utf8") as file_out:
            json.dump(transcript,file_out,indent=2)

        # upload the file again - don't have priviledges for test project - so makes everything irrelevant so far
        if write_back:
            gs_helper.upload_file_to_blob(bucket_name,output_file,os.path.join(working_dir,output_file))

        print(f'{i} Completed')


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
