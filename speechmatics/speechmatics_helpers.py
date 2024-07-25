"""
python speechmatics_helpers.py

Speechmatcis API conflicted with academy default env settings
Recommend setting up dedicated venv for it - then this all just works including in visual studio code

python -m venv sm
source sm/bin/activate
python -m pip install --upgrade pip
pip install -r ./speechmatics/speechmatics_requirements.txt

"""

# *********************************************************************************************************************

# standard imports

# 3rd party imports
from httpx import HTTPStatusError
from speechmatics.models import ConnectionSettings
from speechmatics.batch_client import BatchClient

SPEECHMATICS_URL = "https://asr.api.speechmatics.com/v2"

def get_connection_settings(api_key: str) -> ConnectionSettings:
    """Get connection settings for api
    api_key: the key from the speechmatics https://portal.speechmatics.com/"""
    settings = ConnectionSettings(
        url=SPEECHMATICS_URL,
        auth_token=api_key,
    )
    return settings

def get_transcription_configuration(
        language: str = "en",
        diarization: str = "channel",
        entities: bool = False,
        operation: str = "standard",
        punctuation_sensitivity: float = 0.5
        # expected_languages: str = "",
        # default_language: str = "en"
    ) -> dict:
    """"
    language: "en", "fr" etc. default "en"
        BatchClient SDK wrapper doesn't support native speech detection - can't default to "en"
        https://docs.speechmatics.com/introduction/supported-languages
    diarization: "speaker"|"channel" default "channel"
    entities: True:False default False
    operation: "standard"|"enhanced" default "standard"
    punctionation_sensitivity: default = 0.5
    expected_languages: comma delimited list of en,fr, etc for use with language = auto
    """

    # Define transcription parameters
    conf = {
        "type": "transcription",
        "transcription_config": {
            "diarization": diarization,
            "enable_entities": entities,
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

    # setup language or auto detection
    if language == "auto":
        raise RuntimeError("BatchClient doesn't accept auto right now")
        # Needs to have transcription config and language identification config
    conf["transcription_config"]["language"] = language

    return conf

def get_transcript(audio_file_path: str, settings: dict, transcription_config: dict) -> dict:
    """
    Get transcript for an audio file
    """

    # Open the client using a context manager
    with BatchClient(settings) as client:
        # list all the jobs
        # list_of_jobs = client.list_jobs()
        try:
            job_id = client.submit_job(
                audio=audio_file_path,
                transcription_config=transcription_config,
            )
            print(f"job {job_id} submitted successfully, waiting for transcript")

            # Note that in production, you should set up notifications instead of polling.
            # Notifications are described here: https://docs.speechmatics.com/features-other/notifications
            transcript = client.wait_for_completion(job_id, transcription_format="json-v2")
            return transcript
        except HTTPStatusError as e:
            print(f"Speechmatics API returned something bad - {e}")
