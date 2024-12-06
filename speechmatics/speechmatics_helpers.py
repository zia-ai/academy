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

class TranscriptionError(Exception):
    """
    Indicates an error in transcription.
    """

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
        punctuation_sensitivity: float = 0.5,
        low_confidence_action: str = "",
        expected_languages: str = "",
        default_language: str = "en",
        speaker_sensitivity: float = 0.0
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
            "punctuation_overrides": {
                "permitted_marks": [
                    ",",
                    ".",
                    "?",
                    "!"
                ],
                "sensitivity": punctuation_sensitivity
            },

        }
    }

    if language == "en":
        conf["transcription_config"]["output_locale"] = "en-US"
    # Supported for English and Mandarin
    # The three locales in English that are available are:

    # British English (en-GB)
    # US English (en-US)
    # Australian English (en-AU)
    # When transcribing in English, it is recommended to specify the locale.
    # If no locale is specified then the spelling may be inconsistent within a transcript.

    # The following locales are supported for Chinese Mandarin:

    # Simplified Mandarin (cmn-Hans)
    # Traditional Mandarin (cmn-Hant)


    # setup language or auto detection
    if language == "auto":
        if low_confidence_action != "":
            conf["language_identification_config"] = {
                "low_confidence_action": low_confidence_action
            }

            if low_confidence_action == "use_default_language":
                conf["language_identification_config"]["default_language"] = default_language

        if expected_languages != "":
            expected_languages = expected_languages.split(",")
            if "language_identification_config" not in conf:
                conf["language_identification_config"] = {}
            conf["language_identification_config"]["expected_languages"] = expected_languages
    conf["transcription_config"]["language"] = language

    if diarization == "speaker":
        conf["transcription_config"]["speaker_diarization_config"] = {
            "speaker_sensitivity": speaker_sensitivity
        }

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

def batch_transcribe(audio_file_paths: list, settings: dict, transcription_config: dict, concurrency: int) -> dict:
    """
    Get transcript for an audio file
    """

    transcript = {}
    rejected_transcriptions = {}
    # Open the client using a context manager
    with BatchClient(settings) as client:
        # list all the jobs
        # list_of_jobs = client.list_jobs()

        try:
            for result in client.submit_jobs(audio_paths=audio_file_paths,
                                             transcription_config=transcription_config,
                                             concurrency=concurrency):
                try:
                    transcript[result[0]] = client.wait_for_completion(result[1], transcription_format="json-v2")
                except Exception as e:
                    rejected_transcriptions[result[0]] = e

        #     # Note that in production, you should set up notifications instead of polling.
        #     # Notifications are described here: https://docs.speechmatics.com/features-other/notifications

            return transcript, rejected_transcriptions
        except HTTPStatusError as e:
            print(f"Speechmatics API returned something bad - {e}")