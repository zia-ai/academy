"""
python speechmatics_batch_audio_transcribe.py

Transcribes batch of audios .wav or .mp3 and writes it to the same path but as .json
"""

# *********************************************************************************************************************

# standard imports
import json
import os
from datetime import datetime

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
@click.option('-l', '--language', type=str, default="en", help='Audio language')
@click.option('-v', '--vocab_file_path', type=str, required=False, default = "", help='Additional Vocabulary file path')
@click.option('-d', '--diarization', type=str, default="channel",\
              help='Speechmatics diarization options - channel/speaker')
@click.option('-e', '--entities', is_flag=True, default=False, help='Detects entities')
@click.option('-s', '--operation', type=str, default = "standard", help='Speechmatics operation - standard or enhanced')
@click.option('-p', '--punctuation_sensitivity', type=float, default = 0.5, help='punctuation sensitivity')
def main(audio_folder_path: str,
         api_key: str,
         audio_type: str,
         language: str,
         diarization: str,
         entities: bool,
         operation: str,
         punctuation_sensitivity: float,
         vocab_file_path: str,
         concurrency: int) -> None:
    """Main Function"""

    start_time = datetime.now()

    settings = speechmatics_helpers.get_connection_settings(api_key)

    # Define transcription parameters
    transcription_config = speechmatics_helpers.get_transcription_configuration(
        language=language,
        diarization=diarization,
        entities=entities,
        operation=operation,
        punctuation_sensitivity=punctuation_sensitivity
    )

    if vocab_file_path:
        if os.path.exists(vocab_file_path):
            with open(vocab_file_path, mode="r", encoding="utf8") as file:
                additional_vocab = json.load(file)
                transcription_config[
                    "transcription_config"]["additional_vocab"] = additional_vocab["additional_vocab"]
        else:
            raise RuntimeError(f"{vocab_file_path} doesn't exist")

    file_paths = [os.path.join(audio_folder_path, file)
                  for file in os.listdir(audio_folder_path)
                  if file.endswith(audio_type)]

    # Open the client using a context manager
    print(f"Total number of audio to transcribe {len(file_paths)}")
    transcripts = speechmatics_helpers.batch_transcribe(file_paths,settings,transcription_config,concurrency)

    print(f"Number of Transcriptions successfully completed: {len(transcripts)}")
    end_time = datetime.now()
    print("Execution time for transcribing:", end_time - start_time)

    start_time = datetime.now()
    for file_path, transcript in transcripts.items():
        assert isinstance(file_path,str)
        file_output = file_path.replace(audio_type,".json")
        with open(file_output,mode="w",encoding="utf8") as f:
            json.dump(transcript,f,indent=2)

    end_time = datetime.now()
    print("Execution time for writing all the transcripts:", end_time - start_time)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
