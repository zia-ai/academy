"""
python speechmatics_gcp_transcribe.py

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
from datetime import datetime
import logging
import sys

# 3rd party imports
import click

# custom imports
import speechmatics_helpers # Humanfirst Speechmatics helps
from google_storage_helpers import GoogleStorageHelper # GCP helpers

FOLDER_FILE_SPLIT_DELIMITER = "---"

@click.command()
@click.option('-a', '--api_key', type=str, required=True, help='Api key from speechmatics portal')
@click.option('-b', '--bucket_name', type=str, required=True, help='Bucket name to read/write to')
@click.option('-t', '--audio_type', type=str, required=True, help='.wav .mp3 to search for')
@click.option('-w', '--working_dir', type=str, required=True, help='Dir to download to')
@click.option('-v', '--vocab_file_path', type=str, required=False, default = "", help='Additional Vocabulary file path')
@click.option('-g', '--log_folder_path', type=str, required=False, default = "./speechmatics/logs/", help='Log folder path')
@click.option('-k', '--log_level',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'critical']),
              default='info',
              help='Log levels')
@click.option('-i', '--impersonate', is_flag=True, default=False, help='Impersonate service account or not')
@click.option('-s', '--impersonate_service_account', type=str, required=False, default="",
              help='Target service account to impersonate')
@click.option('-n', '--process_n', type=int, required=False,default=0,
              help='Maximum number to process')
@click.option('-u', '--write_back', is_flag=True, required=False, default=False,
              help='Whether to write back to GCP')
@click.option('-p', '--transcribe', is_flag=True, required=False, default=False,
              help='Transcribes downloaded audio')
@click.option('-l', '--language', type=str, default="en",
              help='Audio language. Can this to "auto" for automatic language detection')
@click.option('-c', '--concurrency', type=int, default=5, help='Number of transcription jobs submitted at single time')
@click.option('-x', '--expected_languages', required = False, default="", help='Comma separated list of languages')
@click.option('-o','--low_confidence_action', type=click.Choice(['allow', 'use_default_language', ""]), default='',
              help="""Options - allow, use_default_language, ''. Triggered when auto detect fails
              https://docs.speechmatics.com/features-other/lang-id#low-confidence-action""")
@click.option('-h', '--default_language', required = False, default="en",
              help='Default language to use in case automatic laguagee detection couldn\'t able to decide')
@click.option('-y', '--speaker_sensitivity', type=float, default = 0.0, help='speaker sensitivity')
def main(
        api_key: str,
        bucket_name: str,
        audio_type: str,
        working_dir: str,
        process_n: int,
        write_back: bool,
        concurrency: int,
        impersonate: bool,
        impersonate_service_account: str,
        vocab_file_path: str,
        transcribe: bool,
        language: str,
        expected_languages: str,
        default_language: str,
        low_confidence_action: str,
        log_folder_path: str,
        log_level: str,
        speaker_sensitivity: float) -> None:
    """Main Function"""

    # set log level
    if log_level == "debug":
        log_level = logging.DEBUG
    elif log_level == "info":
        log_level = logging.INFO
    elif log_level == "warning":
        log_level = logging.WARNING
    elif log_level == "error":
        log_level = logging.ERROR
    elif log_level == "critical":
        log_level = logging.CRITICAL
    else:
        raise RuntimeError("Incorrect log level. Should be one of debug, info, warning, error, critical")

    # Configure the root logger
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_folder_path,f"{timestamp}.log")
    setup_logging(log_file_path, log_level)

    # Redirect stdout and stderr
    redirect_output_to_log(log_file_path)

    # Create a logger object
    logger = logging.getLogger(__name__) # pylint: disable=unused-variable

    # setup speechmatics
    settings = speechmatics_helpers.get_connection_settings(api_key)
    transcription_configuration = speechmatics_helpers.get_transcription_configuration(
        language=language,
        expected_languages=expected_languages,
        low_confidence_action=low_confidence_action,
        default_language=default_language,
        speaker_sensitivity=speaker_sensitivity
    )

    if vocab_file_path:
        if os.path.exists(vocab_file_path):
            with open(vocab_file_path, mode="r", encoding="utf8") as file:
                additional_vocab = json.load(file)
                transcription_configuration[
                    "transcription_config"]["additional_vocab"] = additional_vocab["additional_vocab"]
        else:
            raise RuntimeError(f"{vocab_file_path} doesn't exist")

    print("Transcription Configuration")
    print(json.dumps(transcription_configuration,indent=2))

    gs_helper = GoogleStorageHelper(impersonate=impersonate,
                                    impersonate_service_account=impersonate_service_account)

    df_worklist = gs_helper.get_blob_df_worklist(bucket_name, audio_type, max_results=process_n)

    # Creating a list of all .wav files in the folder
    audio_files = [f for f in os.listdir(working_dir) if f.endswith(audio_type)]

    # Creating a list of all .json files in the folder
    transcription_files = [f for f in os.listdir(working_dir) if f.endswith('.json')]

    # Concert source name to audio file name format
    df_worklist['filename'] = df_worklist['source_name'].apply(lambda x: x.replace("/",FOLDER_FILE_SPLIT_DELIMITER))

    # Check if each file in the DataFrame is in the audio_files list from the local storage
    df_worklist['downloaded_audio_locally'] = df_worklist['filename'].apply(lambda x: x in audio_files)

    # Check if each file in the DataFrame is in the transcribed list from the local storage
    df_worklist['completed_locally'] = df_worklist['filename'].apply(
        lambda x: x.replace(".wav",".json") in transcription_files)

    print(df_worklist)
    print(f'Total: {df_worklist.shape[0]}')

    # Audio file downloaded locally or not
    print("Audio file downloaded locally")
    print(f'To do: {df_worklist.loc[df_worklist["downloaded_audio_locally"] == False].shape[0]}') # pylint: disable=singleton-comparison
    print(f'Done:  {df_worklist.loc[df_worklist["downloaded_audio_locally"] == True].shape[0]}') # pylint: disable=singleton-comparison

    # Transcription available locally or not
    print("Transcription available locally")
    print(f'To do: {df_worklist.loc[df_worklist["completed_locally"] == False].shape[0]}') # pylint: disable=singleton-comparison
    print(f'Done:  {df_worklist.loc[df_worklist["completed_locally"] == True].shape[0]}') # pylint: disable=singleton-comparison

    # Transcription available in GCP bucket or not
    print("Transcription available in GCP bucket")
    print(f'To do: {df_worklist.loc[df_worklist["done"] == False].shape[0]}') # pylint: disable=singleton-comparison
    print(f'Done:  {df_worklist.loc[df_worklist["done"] == True].shape[0]}') # pylint: disable=singleton-comparison

    # if process_n > 0:
    #     df_worklist = df_worklist.sample(process_n)

    for i,row in df_worklist.iterrows():

        # replace "/" with "---"
        input_file = row["filename"]

        # Download file
        if not row["downloaded_audio_locally"]:
            gs_helper.download_blob_to_file(bucket_name, row["source_name"], os.path.join(working_dir, input_file))

            # Set 'downloaded_audio_locally' to True after successful download
            df_worklist.at[i, 'downloaded_audio_locally'] = True

            print(f'{row["source_name"]} Downloaded')

        if row["done"] and not row["completed_locally"]:
            output_file = input_file.replace(audio_type,".json")
            gs_helper.download_blob_to_file(bucket_name, row["target_name"], os.path.join(working_dir, output_file))

            # Set 'completed_locally' to True after successful download
            df_worklist.at[i, 'completed_locally'] = True

            print(f'{row["target_name"]} Downloaded')

    if transcribe:
        # Filtering to find filenames where audio is downloaded but transcription is not completed locally
        df_audio_downloaded_but_not_transcribed = df_worklist.loc[
            (df_worklist["downloaded_audio_locally"] == True) & # pylint: disable=singleton-comparison
            (df_worklist["completed_locally"] == False) & # pylint: disable=singleton-comparison
            (df_worklist["done"] == False), # pylint: disable=singleton-comparison
            "filename"
        ]

        audio_downloaded_but_not_transcribed = [
            os.path.join(working_dir,file) for file in df_audio_downloaded_but_not_transcribed.to_list()]
        index_audio_downloaded_but_not_transcribed = df_audio_downloaded_but_not_transcribed.index

        if audio_downloaded_but_not_transcribed:
            print(f"Transcribing: {len(audio_downloaded_but_not_transcribed)} audio files")
            # print(f"{audio_downloaded_but_not_transcribed}")
            start_time = datetime.now()
            transcripts, rejected_transcriptions = speechmatics_helpers.batch_transcribe(
                audio_downloaded_but_not_transcribed,
                settings,
                transcription_configuration,
                concurrency
            )

            print(f"Number of Transcriptions successfully completed: {len(transcripts)}")
            print(f"Number of Transcriptions rejected: {len(rejected_transcriptions)}")
            end_time = datetime.now()
            print("Execution time for transcribing:", end_time - start_time)

            if rejected_transcriptions:
                print("List of rejected transcriptions")
                for audio, err_msg in rejected_transcriptions.items():
                    print(f"{audio} - {err_msg}")

            for file_path, transcript in transcripts.items():
                assert isinstance(file_path,str)
                file_output = file_path.replace(audio_type,".json")
                with open(file_output,mode="w",encoding="utf8") as f:
                    json.dump(transcript,f,indent=2)
                    print(f"{file_output} saved locally")
                index_df = index_audio_downloaded_but_not_transcribed[
                    audio_downloaded_but_not_transcribed.index(file_path)]
                # Set 'completed_locally' to True after successful download
                df_worklist.at[index_df, 'completed_locally'] = True

    # upload the file again - don't have priviledges for test project - so makes everything irrelevant so far
    if write_back:
        # Filtering to find transcribed filenames
        local_transcription_files = df_worklist.loc[
            (df_worklist["downloaded_audio_locally"] == True) & # pylint: disable=singleton-comparison
            (df_worklist["completed_locally"] == True) & # pylint: disable=singleton-comparison
            (df_worklist["done"] == False), # pylint: disable=singleton-comparison
            "filename"
        ].to_list()
        print("Uploading transcriptions to GCP")
        for file in local_transcription_files:
            assert isinstance(file, str)
            output_file = file.replace(audio_type,".json")
            gs_helper.upload_file_to_blob(bucket_name,output_file,os.path.join(working_dir,output_file))
            print(f"{output_file} is uploaded to GCP bucket")


def setup_logging(log_file_path: str, log_level: logging):
    """Set Up logging"""
    # Remove all existing handlers
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='a'),
            # Remove StreamHandler to prevent double logging
            # logging.StreamHandler()  # You can comment this out if you don't want logs in the terminal
        ]
    )

    return log_file_path

def redirect_output_to_log(log_file_path):
    """Redirect output to log"""
    # Redirect stdout and stderr to the log file
    log_file = open(log_file_path, 'a', encoding="utf8")
    os.dup2(log_file.fileno(), sys.stdout.fileno())
    os.dup2(log_file.fileno(), sys.stderr.fileno())


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
