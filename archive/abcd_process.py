#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python abcd_process.py
#
# Works on the dataset from this paper:
# Chen, D., Chen, H., Yang, Y., Lin, A. and Yu, Z., 2021. 
# Action-based conversations dataset: A corpus for building more in-depth task-oriented dialogue systems. arXiv preprint arXiv:2104.00783.
#
# options
#
# --workspace abcd -version <int>
#   load a particular labelled workspace for an Academy exercise
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
# Produces three files
# ./data/abcd_unlabelled05.json - example month of May unlabelled file to upload as a datasource
# ./data/abcd_unlabelled06.json - same thing for June
# ./data/abcd_labelled.json     - use to create a workspace and link to your datasource
#
# *****************************************************************************

# standard imports
import datetime
import json
import math
import os
import copy
import random
import math

# third party imports
import numpy
import pandas
import click
import presidio_analyzer
import presidio_anonymizer
from google.cloud import translate_v2 as translate

# custom imports
import common

# role mapping abcd roles to HF roles
role_mapping = {
    'customer': 'client',
    'agent':    'expert',
    'action':   'expert',
}

@click.command()
@click.option('-i','--input',type=str,default='./data/abcd_v1.1.json',help='Input File')
@click.option('-u','--unlabelled',type=str,default='abcd_unlabelled',help='Unlabelled Output File ')
@click.option('-l','--labelled',type=str,default='abcd_labelled',help='Labelled Output File ')
@click.option('-s','--sample',type=int,default=0,help='n conversations to sample from dataset')
@click.option('-w','--workspace',type=str,help='The name of any labelled data directory under worspaces, i.e "abcd"')
@click.option('-v','--version',type=int,help='The version or exercise number of the workspace you want to load i.e "1", or "2"')
@click.option('-a','--anonymize',is_flag=True,default=False,help='Run presidio based anonymisation or not')
@click.option('-t','--translation',default='',type=str,help='Translate into this language code using google translate')
@click.option('-o','--source',default='en',type=str,help='Source language of input file')
def main(input: str, unlabelled: str, labelled: str, sample: int, workspace: str, version: int, anonymize: bool, translation: str, source: str):

    # load file to memory    
    file_in = open(input, 'r', encoding='utf8')
    abcddict = json.load(file_in)
    file_in.close()

    # merge abcd train test and dev set
    allset = abcddict['train'] + abcddict['test'] + abcddict['dev']
    print("Total number of convos is: " + str(len(allset)))

    # removed delexed objects which are output from paper model rather than inputs.
    without_delexed = 0
    for i in range(len(allset)):
        try:
            del allset[i]['delexed']
        except KeyError:
            without_delexed = 0 + without_delexed
            pass
    print("Records that couldn't have delexed removed: " + str(without_delexed))

    # json_normalise to pandas
    df = pandas.json_normalize(allset,sep='_')
    assert isinstance(df,pandas.DataFrame)
    df.rename(columns = {'convo_id' : 'abcd_id'}, inplace=True)
    df = df.set_index('abcd_id')
    print("Loaded data frame")

    # allow sampling for a smaller subset 
    if sample > 0:
        df = df.sample(sample)
        print(f'Sampled down to {sample} conversations')

    # work out total char count 
    df = df.apply(count_total_translate_chars,axis=1)
    total_char_count = df["char_count"].sum()
    print(f'Total char count is {total_char_count}')
    print(f'Average char count per convo is {df["char_count"].mean()}')
    if translation and total_char_count > 500000:
        print("WARNING WARNING: exceeds monthly free translation limit")
        print("Google costs are $20 per 1M chars")

    # ABCD set has no dateimtes on the data
    # Generate repeatable datetimes for each utterance
    start_date = datetime.datetime(2022,5,1,0,0,0)
    period = 60 # days
    df = df.apply(add_ids_and_datetimes,axis=1,args=[start_date,period])
    print("Added date times and ids - lots of hashing of ids generated here")

    
    # exists across all workspaces
    labelled_workspace = common.HFWorkspace()

    # Set up the translate engine if we need it.
    if translation != '':
        # store a set of service user credentials in this hidden file
        creds_file = '.google-credentials.json'
        if not os.path.isfile(creds_file):
            raise Exception(f'Could not locate google credentials at: {creds_file}')
        print(f'Translating from en to {translation} is on')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_file
        translate_client = translate.Client()
    else:
        translate_client = None

    # if we have already created intents reload them from source control
    # we are reading from CSV to represent a cleansed source control dataset without tags etc.
    # i.e like a user might have before starting to use humanfirst
    if workspace:
        df_intents = pandas.read_csv(f'./workspaces/{workspace}/{workspace}{version}-intents.csv',delimiter=',',names=['utterance','slash_sep_hier'])
        assert isinstance(df_intents,pandas.DataFrame)
        if translation != '':
            print(f"Translating labelled utterances to {translation}")
            df_intents['utterance'] = translate_text(translation,list(df_intents['utterance']),translate_client,source)
            print(f"Translating label names to {translation}")
            df_intents['slash_sep_hier'] = translate_text(translation,list(df_intents['slash_sep_hier']),translate_client,source)
        df_intents.apply(build_intents_from_file, args=[labelled_workspace],axis=1)

    # now translate the scenarios information for metadata
    # this will end up translating a lot of things many times, could translate distinct and do lookup i.e gold/silve/bronze
    if translation != '':
        df['scenario_personal_member_level'] = translate_text(translation,df['scenario_personal_member_level'],translate_client,source)
        df['scenario_flow'] = translate_text(translation,df['scenario_flow'],translate_client,source)
        df['scenario_subflow'] = translate_text(translation,df['scenario_subflow'],translate_client,source)

    # split the df by month 
    months = df["start_month_convo"].unique()
    print(f'Unique months: {months}')
    for m in months:
        dftemp = df[df["start_month_convo"]==m].copy(deep=True)

        # so unlabelled data basically follows the same rules for labelled, it's just not associated with conversations intents
        # https://docs.humanfirst.ai/docs/advanced/humanfirst-json/

        # context provides the conversational link
        
        # storing the generated examples in the df though also added to unlabelled_workspace

        unlabelled_workspace = common.HFWorkspace()
        print('Created new workspace: ' + m)
          
        # Set up the anonymizer engine if we need it, loads the NLP module (spaCy model by default) 
        # and other PII recognizers
        if anonymize:
            analyzer = presidio_analyzer.AnalyzerEngine()
            anonymizer = presidio_anonymizer.AnonymizerEngine()
            print("WARNING: anonymization is on - script will take several minutes to complete")
        else:
            analyzer = None
            anonymizer = None
        
        dftemp['examples'] = dftemp.apply(parse_convo,axis=1,args=[unlabelled_workspace,labelled_workspace,sample,anonymize,analyzer,anonymizer,translation,translate_client,source])

        # create month labelled file
        print(f'Start write output on {m}')
        with open(f'./data/{unlabelled}{m}{translation}.json', 'w', encoding='utf8') as file_out:
            unlabelled_workspace.write_json(file_out)
        print(f'Finish write output on {m}{translation}')

    with open(f'./data/{labelled}{translation}.json', 'w', encoding='utf8') as file_out:
        labelled_workspace.write_json(file_out)
        # gzipping done separately

    # temp output csv
    dir_path = os.path.dirname(os.path.realpath(__file__))
    df.to_csv(f'{dir_path}/data/output.csv',encoding='utf8')

def add_ids_and_datetimes(row: pandas.Series, start_date: datetime.datetime, period: int) -> pandas.Series:
    '''Adds a converation id, uterance datetimes object, start date of conversation and start month of conversation'''
    # internal longer conversation id to show how we can match to original id
    row['conversation_id'] = common.hash_string(json.dumps(row.original),'convo')
    # convo start time (and time of first utterance)
    row['utterance_datetimes'] = common.get_list_utterance_datetimes(start_date, period, 30, common.hash_string(json.dumps(row.original),None),size=len(row.original))
    # work out the startime of the conversation from first utterance
    row['start_date_convo'] = row['utterance_datetimes'][0]
    # work out the start month for future splitting
    row['start_month_convo'] = f'{row["start_date_convo"].month:02}'
    return row

def abcd_to_hf_roles(role:str) -> str:
    '''Translates abcd to hf role mapping'''
    try:
        return role_mapping[role]
    except KeyError:
        raise Exception(f'Couldn\'t locate role: "{role}" in role mapping')

def build_intents_from_file(row: pandas.Series, labelled_workspace: common.HFWorkspace):
    '''Creates the labelled examples without tags from the saved csv hf export format'''
    hierarchy = str(row['slash_sep_hier']).split('/')
    labelled_workspace.example(row['utterance'],intents=[labelled_workspace.intent(hierarchy)])

def parse_convo(row: pandas.Series, unlabelled_workspace: common.HFWorkspace, labelled_workspace: common.HFWorkspace, sample: int, anonymize: bool, analyser: presidio_analyzer.AnalyzerEngine, anonymizer: presidio_anonymizer.AnonymizerEngine, translation:str, translate_client: translate.Client, source: str) -> list:
    '''parse a single conversation to a set of examples'''
    if sample == 0 and int(row.name) % 1000 == 0:
        print(f'Not in order, but found id number/1000: {row.name}')
    output_examples = []

    # HFMetadata just dict[str,all]
    # extract these keys into the metadata dict
    keys_to_extract = ['scenario_personal_member_level', 'scenario_order_city','scenario_flow','scenario_subflow','scenario_product_names']
    # HFMetadata values must be strings - here we pad to allow sorting up to 999 convo turns
    convo_metadata_dict = {
        'abcd_id': str(row.name),
        'conversation_turn':f'{0:03}'
    }
    for key in keys_to_extract:
        if isinstance(row[key],list):
            convo_metadata_dict[key] = ','.join(row[key])
        elif isinstance(row[key],str):
            convo_metadata_dict[key] = row[key]
        else:
            raise Exception(f'value is not string or list')

    # want to translate full conversation rather than doing piece meal
    # original is a list of list pairs 'customer','agent','action'
    
    # translate if necessary - don't support in combination with anonymization
    if translation != '':
        utterances = []
        for origin in row.original:
            utterances.append(origin[1])
        utterances = translate_text(translation, utterances, translate_client,source)
        for i, u in enumerate(utterances):
            row.original[i][1]=u
   
    for i, origin in enumerate(row.original):

        # abcd set has no timestamps
        # add a random number of seconds for every utterance after the first
        datetime_utterance = row.utterance_datetimes[i]
        assert isinstance(datetime_utterance,datetime.datetime)

        # record in meta data the conversation turn
        convo_metadata_dict['conversation_turn'] = str(f'{i:03}')

        # Put the speaker into metadata
        convo_metadata_dict['abcd_role'] = origin[0]
        
        # Add timestamp of utterance
        convo_metadata_dict['datetime_utterance'] = datetime_utterance.isoformat()

        # Tags 
        tags=[]
        tags_to_label = ['scenario_flow'] # 'scenario_subflow' - removed as too noisy with speaker role and seq
        for tag in tags_to_label:
            tag_value = row[tag]
            tags.append(unlabelled_workspace.tag(tag_value))
            labelled_workspace.tag(tag_value)
        # we don't tag the abcd_role/speaker because only customer data is searchable
        tags.append(unlabelled_workspace.tag(f'{i:03}'))
        labelled_workspace.tag(f'{i:03}')

        # Every example is linked to the conversation by a context record
        # context_id = conversation_id, type = 'conversation', role = client|expert
        context = common.HFContext(row['conversation_id'],'conversation',abcd_to_hf_roles(origin[0]))

        # extract utterance and anonymize if necessary
        utterance = origin[1]
        if anonymize:
            if source != 'en' or translation != '':
                raise Exception('Script currently only configured to anonymize English check archive for Dutch example')
            else:
                utterance = presidio_anonymize(utterance, analyser, anonymizer, source)

        # Create the example
        example = common.HFExample(utterance,common.hash_string(utterance,'example'),created_at=datetime_utterance,intents=[],tags=tags,metadata=copy.deepcopy(convo_metadata_dict),context=context)

        # add to the unlabelled_workspace
        unlabelled_workspace.add_example(example)

        # add to list
        output_examples.append(example)
    return output_examples

def presidio_anonymize(text: str, analyzer: presidio_analyzer.AnalyzerEngine, anonymizer: presidio_anonymizer.AnonymizerEngine, source: str) -> str:
    '''Example anonymization using presidio'''
    # https://microsoft.github.io/presidio/supported_entities/
    results = analyzer.analyze(text=text,
                           entities=["PHONE_NUMBER","PERSON"],
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
                    "masking_char": str(random.randint(0,9)), # low randomness but fine for this purpose.
                    "chars_to_mask": 4,
                    "from_end": True,
                },
            ),
            "PERSON": presidio_anonymizer.anonymizer_engine.OperatorConfig("replace", {"new_value":"PERSON"}),
        }
        anonymized_text = anonymizer.anonymize(text=text,analyzer_results=results,operators=operators).text
    else:
        anonymized_text = text
    return anonymized_text

def translate_text(target: str, utterances: list, translate_client: translate.Client, source: str = None) -> str:
    """Translates text into the target language.
    target and source must be an ISO 639-1 language code.
    https://cloud.google.com/translate/docs/languages
    
    Will default to auto detection if source language not passed
    """    
    # protect against conversations to translate longer than basic translate limit
    if len(utterances) <= 128:
        chunks = [utterances]
    else:
        chunks_needed = math.ceil(len(utterances)/128)
        chunks = numpy.array_split(utterances, chunks_needed)

    output_translation = []
    for chunk in chunks:
        results = translate_client.translate(list(chunk), source_language=source, target_language=target)       
        for result in results:
            output_translation.append(result['translatedText'])
    return output_translation

def count_total_translate_chars(row: pandas.Series) -> pandas.Series:
    '''Enricher to understand total translation size for pricing estimating'''
    char_count = 0
    for origin in row.original:
        char_count = char_count + len(origin[1])
    row['char_count'] = char_count
    return row

if __name__ == '__main__':
    main()