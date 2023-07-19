#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python abcd_unlabelled.py
#
# create an unlabelled json set to upload to HF
#
# Works on the dataset from this paper:
# Chen, D., Chen, H., Yang, Y., Lin, A. and Yu, Z., 2021.
# Action-based conversations dataset: A corpus for building more in-depth
# task-oriented dialogue systems. arXiv preprint arXiv:2104.00783.
#
# options
#
# --sample <int>
#   randomly sample on a portion of complete conversations from the dataset
#   useful if making changes to script or trying to keep under a datapoint limit
#
# --anonymize
#   if present presidio will be used to replace mentions of PERSON and telephone
#   or account-a-like numbers in the abcd dataset
#
# --translate <lc>
#      translate using google translate to https://cloud.google.com/translate/docs/languages
#      you will need to have created json service credentials at .google-credentials.json
#      you will incur costs for using this over 500,000 chars in a month
#
#
# Produces two files
# ./data/abcd_unlabelled05.json - example month of May unlabelled file to upload as a datasource
# ./data/abcd_unlabelled06.json - same thing for June
#
# *****************************************************************************

# standard imports
import datetime
import json
import os
import random
import math
from datetime import datetime, timedelta
from time import perf_counter

# third party imports
import numpy
import pandas
import click
import presidio_analyzer
import presidio_anonymizer
import tqdm
from google.cloud import translate_v2 as translate

# custom imports
import humanfirst

# role mapping abcd roles to HF roles
role_mapping = {
    'customer': 'client',
    'agent':    'expert',
    'action':   'expert',
}

performance_log = []


@click.command()
@click.option('-i', '--input_file', type=str, default='./data/abcd_v1.1.json', help='Input File')
@click.option('-u', '--unlabelled', type=str, default='abcd_unlabelled', help='Unlabelled Output File ')
@click.option('-s', '--sample', type=int, default=0, help='n conversations to sample from dataset')
@click.option('-a', '--anonymize', is_flag=True, default=False, help='Run presidio based anonymisation or not')
@click.option('-t', '--translation', default='', type=str,
              help='Translate into this language code using google translate')
@click.option('-o', '--source', default='en', type=str, help='Source language of input file')
@click.option('-d', '--abcd_id', default=0, type=int, required=False,
              help='Filter for just this abcd_id')
@click.option('-l', '--include_actions', is_flag=True, default=False, type=bool, required=False,
              help='Include system actions')
def main(input_file: str, unlabelled: str, sample: int, anonymize: bool, translation: str, source: str, abcd_id: int, include_actions: bool):
    '''Main function'''
    process(input_file, unlabelled, sample, anonymize, translation, source, abcd_id, include_actions)

def process(input_file: str, unlabelled: str, sample: int, anonymize: bool,
            translation: str, source: str, abcd_id: int, include_actions: bool):
    '''Process the file'''
    perf_log('Begin')

    # load data
    tqdm.tqdm.pandas()
    df = load_data_file(input_file, abcd_id)

    # allow sampling for a smaller subset of conversations
    df = df if sample == 0 else df.sample(sample)

    # explode the original abcd column to abcd_role and utterance
    df = df.explode(['original']).reset_index(drop=True)
    
    # at this point should all b in order
    print(df)
    print(df.loc[0,:])
    
    df = pandas.concat([df, pandas.DataFrame(
        df['original'].tolist(), columns=['abcd_role', 'utterance'])], axis=1)
    print(df)
    df['idx'] = df.groupby('abcd_id').cumcount()
    print(df)
    perf_log('Exploded original rows')
    if not include_actions:
        perf_log(f'Before removing actions: {df.shape}')
        df = df[df["abcd_role"]!="action"]
        perf_log(f'After removing actions: {df.shape}')

    # index the speakers
    df['idx_max'] = df.groupby(["abcd_id"])['idx'].transform(numpy.max)
    df['idx_customer'] = df.groupby(['abcd_id', 'abcd_role']).cumcount().where(
        df.abcd_role == 'customer', 0)
    df['idx_customer_max'] = df.groupby(
        ["abcd_id"])['idx_customer'].transform(numpy.max)
    df['idx_agent'] = df.groupby(['abcd_id', 'abcd_role']).cumcount().where(
        df.abcd_role == 'agent', 0)
    df['idx_action'] = df.groupby(['abcd_id', 'abcd_role']).cumcount().where(
        df.abcd_role == 'action', 0)
    perf_log('Finished Indexing Speakers')

    # create metadata object in DF
    keys_to_extract = ['scenario_personal_member_level', 'scenario_order_city',
                       'scenario_flow', 'scenario_subflow', 'scenario_product_names', 'abcd_role']
    perf_log('Commencing metadata create')
    df = df.progress_apply(create_metadata, args=[keys_to_extract], axis=1)
    perf_log('Created metadata')

    # calculate timestamps
    start_date = datetime(2022, 5, 1, 0, 0, 0)
    period = 59  # days
    max_abcd_id = df['abcd_id'].max()
    seconds_between_utterances = 17
    perf_log('Commencing adding datatimes')
    df = df.progress_apply(add_datetimes, axis=1, args=[
                           start_date, period, max_abcd_id, seconds_between_utterances])
    perf_log('Created Timestamps')

    # abcd to hf roles
    df['hf_role'] = df['abcd_role'].apply(abcd_to_hf_roles)
    perf_log('Translated abcd roles to hf roles')

    # translate and anonymize check
    if translation != '' and anonymize:
        raise humanfirst.HFIncompatibleOptionException('Cannot both translate and anonymize')

    # translate
    if translation != '':
        # store a set of service user credentials in this hidden file
        creds_file = '.google-credentials.json'
        if not os.path.isfile(creds_file):
            raise humanfirst.HFMissingCredentialsException(
                f'Could not locate google credentials at: {creds_file}')
        perf_log(f'Translating from {source} to {translation} is on')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_file
        translate_client = translate.Client()
        df['original_utterance'] = df['utterance']
        df['utterance'] = translate_text(
            translation, df['utterance'].to_list(), translate_client, source=source)
    else:
        perf_log('Translation off')
        translate_client = None

    # anonymize
    if anonymize:
        analyzer = presidio_analyzer.AnalyzerEngine()
        anonymizer = presidio_anonymizer.AnonymizerEngine()
        perf_log(
            "WARNING: anonymization is on - script will take several minutes to complete")
        df['original_utterance'] = df['utterance']
        df['utterance'] = df['utterance'].apply(
            presidio_anonymize, args=[analyzer, anonymizer, source])
    else:
        perf_log('Anonymization off')
        analyzer = None
        anonymizer = None

    # set index
    df = df.set_index(['abcd_id', 'idx'], drop=True)
    perf_log('Set abcd_id,idx as index')

    # build examples

    perf_log('Commencing example build')
    df = df.progress_apply(build_example, axis=1)
    perf_log('Built all examples')

    # make a workspace for each month and write to file
    for m in list(df['month'].unique()):  # pylint: disable=invalid-name
        unlabelled_workspace = humanfirst.HFWorkspace()
        df_month = df[df['month'] == m]['example']
        for example in df_month:
            unlabelled_workspace.add_example(example)
        file_name = f'./data/{unlabelled}{m}{translation}.json'
        file_out = open(file_name, 'w', encoding='utf8')
        perf_log(f'Starting write out: {file_name}')
        unlabelled_workspace.write_json(file_out)
        perf_log(f'Finished write out: {file_name}')
        file_out.close()

    print(df[['utterance', 'abcd_role', 'created_at']])
    print(df[['month', 'utterance']].groupby(['month']).count())


def create_metadata(row: pandas.Series, keys_to_extract: list) -> pandas.Series:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    # HFMetadata values must be strings - here we pad to allow sorting up to 999 convo turns
    convo_metadata_dict = {
        'abcd_id': str(row.abcd_id),
        'conversation_turn': f'{row.idx:03}',
        'first_customer_utt': 'FALSE',
        'second_customer_utt': 'FALSE',
        'last_customer_utt': 'FALSE'
    }

    if row.abcd_role == 'customer':
        if row.idx_customer == 0:
            convo_metadata_dict['first_customer_utt'] = 'TRUE'
        if row.idx_customer == 1:
            convo_metadata_dict['second_customer_utt'] = 'TRUE'
        if row.idx_customer == row.idx_customer_max:
            convo_metadata_dict['last_customer_utt'] = 'TRUE'

    for key in keys_to_extract:
        if isinstance(row[key], list):
            convo_metadata_dict[key] = ','.join(row[key])
        elif isinstance(row[key], str):
            convo_metadata_dict[key] = row[key]
        else:
            raise humanfirst.HFMapperException('Value is not string or list')

    row['metadata'] = convo_metadata_dict
    return row


def load_data_file(input_file: str, abcd_id: int) -> pandas.DataFrame:
    '''Read abcd input file and return a data frame'''
    # load abcd file to memory
    file_in = open(input_file, 'r', encoding='utf8')
    abcddict = json.load(file_in)
    file_in.close()

    # merge abcd train test and dev set
    allset = abcddict['train'] + abcddict['test'] + abcddict['dev']
    perf_log("Total number of convos is: " + str(len(allset)))

    # removed delexed objects which are output from paper model rather than inputs.
    without_delexed = 0
    filtered_allset = []
    for i in range(len(allset)):  # pylint: disable=consider-using-enumerate
        try:
            del allset[i]['delexed']
        except KeyError:
            without_delexed = 0 + without_delexed
            
        # if we need to trin to just one record
        if abcd_id > 0 and abcd_id == allset[i]["convo_id"]:
            print(abcd_id)
            filtered_allset.append(allset[i])
            print(json.dumps(allset[i],indent=2))
    if len(filtered_allset) == 1:
        allset = filtered_allset
            
                
    perf_log("Records that couldn't have delexed removed: " +
             str(without_delexed))

    # json_normalise to pandas
    df = pandas.json_normalize(allset, sep='_')
    assert isinstance(df, pandas.DataFrame)
    df.rename(columns={'convo_id': 'abcd_id'}, inplace=True)
    # df = df.set_index('abcd_id')
    perf_log("Loaded data frame")
    return df


def add_datetimes(row: pandas.DataFrame, start_date: datetime, period: int,
                  max_abcd_id: int, time_per_utterance: int) -> pandas.Series:
    '''Adds a repeatable datetime based on the abcd_id'''
    created_at = start_date + \
        timedelta(seconds=(row.idx*time_per_utterance +
                  int(period*24*60*60*int(row.abcd_id)/max_abcd_id)))
    row['created_at'] = created_at
    row['month'] = f'{created_at.month:02}'
    return row


def abcd_to_hf_roles(role: str) -> str:
    '''Translates abcd to hf role mapping'''
    try:
        return role_mapping[role]
    except KeyError as exc:
        raise humanfirst.HFMapperException(f'Couldn\'t locate role: "{role}" in role mapping. KeyError: {exc}')


def build_example(row: pandas.Series) -> pandas.Series:
    '''Creates a HumanFirst unlabelled utterance example linking it to it's conversation and adding metadata'''
    example = humanfirst.HFExample(
        text=row['utterance'],
        id=f'example-{row.name[0]}-{row.name[1]}',
        created_at=row['created_at'],
        intents=[],  # no intents as unlabelled
        tags=[],  # no tags only metadata on unlabelled
        metadata=row['metadata'],
        # abcd_id, conversation, hf_role
        context=humanfirst.HFContext(
            str(row.name[0]), 'conversation', row['hf_role'])
    )
    row['example'] = example
    return row


def presidio_anonymize(text: str, analyzer: presidio_analyzer.AnalyzerEngine,
                       anonymizer: presidio_anonymizer.AnonymizerEngine, source: str) -> str:
    '''Example anonymization using presidio'''
    # https://microsoft.github.io/presidio/supported_entities/
    results = analyzer.analyze(text=text,
                               entities=["PHONE_NUMBER", "PERSON"],
                               language=source)
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
                    # low randomness but fine for this purpose.
                    "masking_char": str(random.randint(0, 9)),
                    "chars_to_mask": 4,
                    "from_end": True,
                },
            ),
            "PERSON": presidio_anonymizer.anonymizer_engine.OperatorConfig("replace", {"new_value": "PERSON"}),
        }
        anonymized_text = anonymizer.anonymize(
            text=text, analyzer_results=results, operators=operators).text
    else:
        anonymized_text = text
    return anonymized_text


def translate_text(target: str, utterances: list, translate_client: translate.Client, source: str = None) -> str:
    """Translates text into the target language.
    target and source must be an ISO 639-1 language code.
    https://cloud.google.com/translate/docs/languages

    Will default to auto detection if source language not passed
    """
    total_chars = len(''.join(utterances))
    if total_chars > 2:
        perf_log(
            f'WARNING WARNING translation exceeds google free limit: {total_chars}')

    # protect against conversations to translate longer than basic translate limit
    if len(utterances) <= 128:
        chunks = [utterances]
    else:
        chunks_needed = math.ceil(len(utterances)/128)
        chunks = numpy.array_split(utterances, chunks_needed)

    output_translation = []
    for chunk in chunks:
        results = translate_client.translate(
            list(chunk), source_language=source, target_language=target)
        for result in results:
            output_translation.append(result['translatedText'])
    return output_translation


def perf_log(label: str) -> list:
    "Performance logging helper"
    now = perf_counter()
    if len(performance_log) == 0:
        then = now
        start = now
    else:
        then = performance_log[-1]['timestamp']
        start = performance_log[0]['timestamp']
    log = {
        'label': label,
        'timestamp': now,
        'duration': now - then,
        'elapsed': now - start
    }
    performance_log.append(log)
    print(f'{log["duration"]:.3f} {label}')
    return perf_log


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
