#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
#  pyhthon translate.py -t "Text to translate"
#
# *****************************************************************************

# standard imports
import os
import typing

# third party imports
import click

# this uses basic model - NMT google models.  Not AutoML fine tuned on customer data models
# this is good enough in most instances.
from google.cloud import translate_v2 as translate

@click.command()
@click.option('-t','--text',type=str,required=True,help='What to translate')
def main(text: str):
    # store a set of service user credentials in this hidden file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.google-credentials.json'
    translate_client = translate.Client()
    print(translate_text('es', list([str(text),'other string','this is my third string']), translate_client,'en'))
    
def translate_text(target: str, text: typing.Union[str,list], translate_client: translate.Client, source: str = None) -> list:
    """Translates text into the target language.
    target and source must be an ISO 639-1 language code.
    https://cloud.google.com/translate/docs/languages
    
    Accepts a string or a list of strings to translate

    Will default to auto detection if source language not passed
    """    
    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.
    return translate_client.translate(text, source_language=source, target_language=target)

if __name__ == '__main__':
    main()

