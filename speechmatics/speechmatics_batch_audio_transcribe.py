"""
python speechmatics_batch_audio_transcribe.py

Transcribes batch of audios .wav or .mp3 and writes it to the same path but as .json
"""

# *********************************************************************************************************************

# standard imports
import json
import os
from datetime import datetime
import logging
import sys

# 3rd party imports
import click

# custom imports
import speechmatics_helpers

@click.command()
# mandatory
@click.option('-f', '--audio_folder_path', type=str, required=True, help='Speechmatics json folder')
@click.option('-a', '--api_key', type=str, required=True, help='Speechmatics API token')
@click.option('-t', '--audio_type', type=str, required=True, help='.wav .mp3 to search for')
# optional
@click.option('-c', '--concurrency', type=int, default=5, help='Number of transcription jobs submitted at single time')
@click.option('-l', '--language', type=str, default="en",
              help='Audio language. Can this to "auto" for automatic language detection')
@click.option('-v', '--vocab_file_path', type=str, required=False, default = "", help='Additional Vocabulary file path')
@click.option('-d', '--diarization', type=str, default="channel",\
              help='Speechmatics diarization options - channel/speaker')
@click.option('-e', '--entities', is_flag=True, default=False, help='Detects entities')
@click.option('-x', '--expected_languages', required = False, default="", help='Comma separated list of languages')
@click.option('-i','--low_confidence_action', type=click.Choice(['allow', 'use_default_language', ""]), default='',
              help="""Options - allow, use_default_language, ''. Triggered when auto detect fails
              https://docs.speechmatics.com/features-other/lang-id#low-confidence-action""")
@click.option('-h', '--default_language', required = False, default="en",
              help='Default language to use in case automatic laguagee detection couldn\'t able to decide')
@click.option('-s', '--operation', type=str, default = "standard", help='Speechmatics operation - standard or enhanced')
@click.option('-p', '--punctuation_sensitivity', type=float, default = 0.5, help='punctuation sensitivity')
@click.option('-n', '--process_n', type=int, required=False, default=0,
              help='Maximum number of audio to process')
@click.option('-g', '--log_folder_path', type=str, required=False, default = "./speechmatics/logs/", help='Log folder path')
@click.option('-k', '--log_level',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'critical']),
              default='info',
              help='Log levels')
@click.option('-y', '--speaker_sensitivity', type=float, default = 0.0, help='speaker sensitivity')
def main(audio_folder_path: str,
         api_key: str,
         audio_type: str,
         language: str,
         diarization: str,
         entities: bool,
         operation: str,
         punctuation_sensitivity: float,
         vocab_file_path: str,
         concurrency: int,
         process_n: int,
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

    start_time = datetime.now()

    settings = speechmatics_helpers.get_connection_settings(api_key)

    # Define transcription parameters
    transcription_config = speechmatics_helpers.get_transcription_configuration(
        language=language,
        diarization=diarization,
        entities=entities,
        operation=operation,
        punctuation_sensitivity=punctuation_sensitivity,
        expected_languages=expected_languages,
        low_confidence_action=low_confidence_action,
        default_language=default_language,
        speaker_sensitivity=speaker_sensitivity
    )

    if vocab_file_path:
        if os.path.exists(vocab_file_path):
            with open(vocab_file_path, mode="r", encoding="utf8") as file:
                additional_vocab = json.load(file)
                transcription_config[
                    "transcription_config"]["additional_vocab"] = additional_vocab["additional_vocab"]
        else:
            raise RuntimeError(f"{vocab_file_path} doesn't exist")
    print("Transcription Configuration")
    print(json.dumps(transcription_config,indent=2))

    audio_file_paths = [os.path.join(audio_folder_path, file)
                  for file in os.listdir(audio_folder_path)
                  if file.endswith(audio_type)]

    # Choose only untranscribed audios
    untranscribed_audio_file_paths = []
    for file in audio_file_paths:
        if not os.path.exists(file.replace(audio_type,".json")):
            untranscribed_audio_file_paths.append(file)

    print(f"Total untranscribed audios: {len(untranscribed_audio_file_paths)}")

    if process_n > 0:
        untranscribed_audio_file_paths = untranscribed_audio_file_paths[:process_n]

    if untranscribed_audio_file_paths:
        transcripts, rejected_transcriptions = speechmatics_helpers.batch_transcribe(untranscribed_audio_file_paths,
                                                            settings,
                                                            transcription_config,
                                                            concurrency)

        print(f"Number of Transcriptions successfully completed: {len(transcripts)}")
        print(f"Number of Transcriptions rejected: {len(rejected_transcriptions)}")
        end_time = datetime.now()
        print("Execution time for transcribing:", end_time - start_time)

        if rejected_transcriptions:
            print("List of rejected transcriptions")
            for audio, err_msg in rejected_transcriptions.items():
                print(f"{audio} - {err_msg}")

        start_time = datetime.now()
        for file_path, transcript in transcripts.items():
            assert isinstance(file_path,str)
            file_output = file_path.replace(audio_type,".json")
            with open(file_output,mode="w",encoding="utf8") as f:
                json.dump(transcript,f,indent=2)

        end_time = datetime.now()
        print("Execution time for writing all the transcripts:", end_time - start_time)


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
