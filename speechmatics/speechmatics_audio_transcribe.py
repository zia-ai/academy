"""
python speechmatics_to_hf_csv.py

Transcribes a single audio .wav or .mp3 and writes it to the same path but as .json

"""

# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import click

# custom imports
import speechmatics_helpers

@click.command()
# mandatory
@click.option('-f', '--audio_file_path', type=str, required=True, help='Speechmatics json folder')
@click.option('-a', '--api_key', type=str, required=True, help='Speechmatics API token')
# optional
@click.option('-l', '--language', type=str, default="en", help='Audio language')
@click.option('-d', '--diarization', type=str, default="channel",\
              help='Speechmatics diarization options - channel/speaker')
@click.option('-e', '--entities', is_flag=True, default=False, help='Detects entities')
@click.option('-s', '--operation', type=str, default = "standard", help='Speechmatics operation - standard or enhanced')
@click.option('-p', '--punctuation_sensitivity', type=float, default = 0.5, help='punctuation sensitivity')
def main(audio_file_path: str,
         api_key: str,
         language: str,
         diarization: str,
         entities: bool,
         operation: str,
         punctuation_sensitivity: float) -> None:
    """Main Function"""

    settings = speechmatics_helpers.get_connection_settings(api_key)

    # Define transcription parameters
    transcription_config = speechmatics_helpers.get_transcription_configuration(
        language=language,
        diarization=diarization,
        entities=entities,
        operation=operation,
        punctuation_sensitivity=punctuation_sensitivity
    )

    # Open the client using a context manager
    transcript = speechmatics_helpers.get_transcript(audio_file_path,settings,transcription_config)

    # write output
    if audio_file_path.endswith(".mp3"):
        file_output= audio_file_path.replace(".mp3",".json")
    elif audio_file_path.endswith(".wav"):
        file_output= audio_file_path.replace(".wav",".json")
    else:
        raise RuntimeError("Unrecognised autio type")
    assert(file_output != audio_file_path)
    with open(file_output,mode="w",encoding="utf8") as f:
        json.dump(transcript,f,indent=2)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
