#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python nl_model.py
# 
# example of annoymisation in Dutch using Spacy
#
# *****************************************************************************

# standard imports
import os
import random
import logging

# third party imports
import click
import presidio_analyzer
import presidio_anonymizer
# Stanza doesn't load the model in the NLP config needs more work.
# import stanza
# stanza.download("nl")

@click.command()
def main():

    logging.basicConfig(level='ERROR')

    # real utterance replaced wtih historical name, university library address, fake date of birth and customer number
    utterance='''
    Hai.  Ik heb een vraag. Wil graag een betalingsregeling treffen over de eindafrekening.
    Mijn klantnummer is 242457844
    Adres van Heidelberglaan 3, 3584 CS Utrecht, Netherlands
    Geb datum 28-09-1963
    Johan Huizinga
    '''
    print(utterance)

    LANGUAGES_CONFIG_FILE = "./languages-config.yaml"
    print(f'File is: {os.path.isfile(LANGUAGES_CONFIG_FILE)}')

    # Create NLP engine based on configuration file
    provider = presidio_analyzer.nlp_engine.NlpEngineProvider(conf_file=LANGUAGES_CONFIG_FILE)
    nlp_engine_nl = provider.create_engine()

    # Pass created NLP engine and supported_languages to the AnalyzerEngine
    analyzer = presidio_analyzer.AnalyzerEngine(
        nlp_engine=nlp_engine_nl, 
        supported_languages=["en", "nl"]
    )
    anonymizer = presidio_anonymizer.AnonymizerEngine()
    utterance = presidio_anonymize(utterance, analyzer, anonymizer)
    print(utterance)

def presidio_anonymize(text: str, analyzer: presidio_analyzer.AnalyzerEngine, anonymizer: presidio_anonymizer.AnonymizerEngine) -> str:
    '''Example anonymization using presidio'''
    # https://microsoft.github.io/presidio/supported_entities/
    results = analyzer.analyze(text=text,
                           entities=["PHONE_NUMBER","PERSON","DATE_TIME","EMAIL_ADDRESS","LOCATION"],
                           language='nl')

    print('*****')
    print(results)
    print('*****')
    
    if len(results) > 0:
        # Define anonymization operators - will mask with a random digit
        # 07764 988712 becomes 07764 983333 rather than 07764 98****
        # this is also going to do order numbers and reference numbers in the ABCD set
        # for more advanced jumbling a custom operator in presidio can be implemented.
        operators = {
            "PHONE_NUMBER": presidio_anonymizer.anonymizer_engine.OperatorConfig(
                "mask",
                {
                    "type": "mask",
                    "masking_char": str(random.randint(0,9)), # low randomness but fine for this purpose.
                    "chars_to_mask": 4,
                    "from_end": True,
                },
            ),
            "PERSON": presidio_anonymizer.anonymizer_engine.OperatorConfig("replace", {"new_value":"PERSON"}),
            "DATE_TIME": presidio_anonymizer.anonymizer_engine.OperatorConfig("replace", {"new_value":"DATE_TIME"}),
            "EMAIL_ADDRESS": presidio_anonymizer.anonymizer_engine.OperatorConfig("replace", {"new_value":" EMAIL_ADDRESS"}),
            "LOCATION": presidio_anonymizer.anonymizer_engine.OperatorConfig("replace", {"new_value":" LOCATION"})
        }
        anonymized_text = anonymizer.anonymize(text=text,analyzer_results=results,operators=operators).text
    else:
        anonymized_text = text
    return anonymized_text

if __name__ == '__main__':
    main()