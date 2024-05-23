"""
python speechmatics_to_hf_csv.py

Transcribes an audio ussing speechmatics

Speechmatics python SDK doesn't include lot of functionalities which they have in their github
So install speechmatics directly from the source instead of pip

Follow the below steps to install speechmatics into desired environment
Git clone speechmatics - https://github.com/speechmatics/speechmatics-python/tree/master
Installation steps are avaliable in the README.md of the speechmatics github
    Ensure to have the academy venv open
    Then cd speechmatics and run "python setup.py install"
"""
# TODO: Accept folder with mp3 files and produce transcripts to an output folder
#       If a transcript exists in output folder, then do not run a transcription job for the corresponding mp3 file
# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import click
from speechmatics.models import ConnectionSettings
from speechmatics.batch_client import BatchClient
from httpx import HTTPStatusError


@click.command()
@click.option('-f', '--file_path', type=str, required=True, help='Speechmatics json folder')
@click.option('-l', '--language', type=str, default="en", help='Audio language')
@click.option('-a', '--auth_token', type=str, required=True, help='Speechmatics API token')
@click.option('-d', '--diarization', type=str, default="channel",\
              help='Speechmatics diarization options - channel/speaker')
@click.option('-e', '--entities', is_flag=True, default=False, help='Detects entities')
@click.option('-s', '--operation', type=str, default = "standard", help='Speechmatics operation - standard or enhanced')
@click.option('-p', '--punctuation_sensitivity', type=float, default = 0.5, help='punctuation sensitivity')
def main(file_path: str,
         language: str,
         auth_token: str,
         diarization: str,
         entities: bool,
         operation: str,
         punctuation_sensitivity: float) -> None:
    """Main Function"""

    settings = ConnectionSettings(
        url="https://asr.api.speechmatics.com/v2",
        auth_token=auth_token,
    )

    # Define transcription parameters
    conf = {
        "type": "transcription",
        "transcription_config": {
            "diarization": diarization,
            "enable_entities": entities,
            "language": language,
            "operating_point": operation,
            "output_locale": "en-US",
            "punctuation_overrides": {
                "permitted_marks": [
                    ",",
                    ".",
                    "?",
                    "!"
                ],
                "sensitivity": punctuation_sensitivity
            }
        }
    }

    # Open the client using a context manager
    with BatchClient(settings) as client:
        # list all the jobs
        # list_of_jobs = client.list_jobs()
        try:
            job_id = client.submit_job(
                audio=file_path,
                transcription_config=conf,
            )
            print(f"job {job_id} submitted successfully, waiting for transcript")

            # Note that in production, you should set up notifications instead of polling.
            # Notifications are described here: https://docs.speechmatics.com/features-other/notifications
            transcript = client.wait_for_completion(job_id, transcription_format="json-v2")
            file_output= file_path.replace(".mp3",".json")
            with open(file_output,mode="w",encoding="utf8") as f:
                json.dump(transcript,f,indent=2)
        except HTTPStatusError:
            print("Invalid API key - Check your auth_token at the top of the code!")


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
