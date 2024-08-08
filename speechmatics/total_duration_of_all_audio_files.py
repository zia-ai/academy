"""
python total_duration_of_all_audio_files

Parse through folder containing audio files and calculates the total duration of all audio files.
Gives the results in Hrs

Make sure to install ffmpeg before running the script
"""

# *********************************************************************************************************************

# standard imports
import os
import click

# 3rd party imports
from pydub import AudioSegment

@click.command()
@click.option('-f', '--audio_folder_path', type=str, required=True, help='Folder containing auido .wav files')
def main(audio_folder_path: str):
    """Get total duration"""

    total_duration_ms = 0
    for filename in os.listdir(audio_folder_path):
        if filename.endswith('.wav'):
            file_path = os.path.join(audio_folder_path, filename)
            print(file_path)
            audio = AudioSegment.from_wav(file_path)
            total_duration_ms += len(audio)  # duration in milliseconds

    # Convert milliseconds to hours
    total_duration_hours = total_duration_ms / (1000 * 60 * 60)
    print(f"Total duration of all .wav files: {total_duration_hours:.2f} hours")

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
