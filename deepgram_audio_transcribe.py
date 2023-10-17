#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *********************************************************************************************************************
#
# python deepgram_audio_transcribe.py
#
# *********************************************************************************************************************

# standard imports
import os
import json

# 3rd party imports
import asyncclick as click
from deepgram import Deepgram

@click.command()
@click.option('-d', '--directory', type=str, required=True,
              help='Directory with input files *.wav')
@click.option('-k', '--deepgramkey', type=str, required=True,
              help='Deepgram API Key')
@click.option('-o', '--outputdir', type=str, required=True,
              help='Output directory for Deepgram Json format')
@click.option('-r', '--rebuild', is_flag=True, default=False,
              help='Whether to rebuild all transcriptions or look for delta')
@click.option('-s', '--sample',  type=int, required=False, default=0,
              help='Number of transcriptions to do (on top of skipped)')
async def main(directory: str, deepgramkey: str, outputdir: str, rebuild: bool, sample: int) -> None:
    """Main Function"""

    # Check ends with /
    if not directory.endswith('/'):
        directory = directory + '/'
    if not outputdir.endswith('/'):
        outputdir = outputdir + '/'

    # work out how many files
    file_list = os.listdir(directory)
    wav_files = []
    for file in file_list:
        if file.endswith(".wav"):
            wav_files.append(f'{directory}{file}')
    print(f'Wav files to be processed: {len(wav_files)}')

    # get deepgram client
    dg_client = Deepgram(deepgramkey)

    # transcription options
    opts = {
        'tier': 'nova',
        'model': 'general',
        'diarize': True,
        'punctuate': True,
        'smart_format': True
    }

    count_samples = 0
    for wav in wav_files:
        with open(wav, 'rb') as wav_rb:
            source = {'buffer': wav_rb, 'mimetype': 'audio/x-wav'}

            output_file_name = get_output_name(wav, outputdir)

            if not rebuild and os.path.isfile(output_file_name):
                print(f'Skipping file {wav} as ouptut exists')
            else:
                response = await dg_client.transcription.prerecorded(source, opts)
                print(f'Writing to {output_file_name}')
                with open(output_file_name,"w",encoding='utf8') as output_file:
                    output_file.write(json.dumps(response,indent=2))
                count_samples = count_samples + 1
        if sample > 0 and count_samples >= sample:
            break

def get_output_name(input_name: str, outputdir: str) -> str:
    """Transform input name to output name"""
    assert isinstance(input_name,str)
    output_file_name = input_name.replace(".wav",".json")
    output_file_name = outputdir + output_file_name.split("/")[-1]
    return output_file_name

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
