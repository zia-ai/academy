#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python nuance-mix-convert.py -d './data' -i 41655_08Nov2022_Stephen-Demo.trsx
# python nuance-mix-convert.py -d './data' -o 
# 
# *****************************************************************************

# standard imports
import os
from io import TextIOWrapper
import json
import re

# third party imports
import pandas
import click
import xml.etree.ElementTree as ET
import xml
import xml.dom.minidom
import datetime

# custom imports
import common


@click.command()
@click.option('-d', '--directory', type=str, default='./data', help='Directory for input and output')
@click.option('-m', '--mode', type=click.Choice(['import-tsrx','export-csv','export-json','infer',''], case_sensitive=False), default='', required=False, help='Mode to export or import or let it infer from filetype')
@click.option('-f', '--file',type=str,required=True,help='input filename in directory')
@click.option('-s', '--sample',type=int,default=0,help='Only do n records' )
@click.option('-k', '--skip',is_flag=True,default=False,help='Skip and log exceptions' )
@click.option('-c', '--countrycode',type=str,required=True,help='Nuance Mix Language Code https://docs.mix.nuance.com/languages/#languages eng-GBR eng-USA jpn-JPN')
@click.option('-e', '--empty',is_flag=True,default=False,help='Include intents even if they have no training')
@click.option('-d', '--delim',type=str,default='-',help='What delimiter to use for parent intents HF default / wont work in mix recommend -' )
@click.option('-v', '--verbose',is_flag=True,default=False,help='Turn on verbose mode' )
def main(directory: str, mode: str, file: str, sample: int, skip: bool, countrycode: str, empty: bool, delim: str, verbose: bool):
    directory, mode, file = validate_args(directory, mode, file)
    if mode == 'import-trsx':
        parse_nuance_mix_xml(ET.fromstring(read_file(directory, mode, file)), directory, file, skip, delim, verbose)
    elif mode == 'export-json':
        parse_humanfirst_json(directory, file, sample, skip, countrycode, empty, delim)
    elif mode == 'export-csv':
        raise Exception ('Decommissioned: Decided to keep to json to preserve metadata in and out')
    else:
        raise Exception ('Unrecognised mode')

def validate_args(directory: str, mode: str, file: str):
    if mode == '' or mode == 'infer':
        if file.endswith('.trsx'):
            print('Infered import to HumanFirst from Nuance Mix trsx file')
            mode = 'import-trsx'
        elif file.endswith('.csv'):
            print('Infer export to Nuance Mix from HumanFirst csv file')
            mode = 'export-csv'
        elif file.endswith('.json'):
            print('Infer export to Nuance Mix from HumanFirst json file')
            mode = 'export-json'
        else:
            raise Exception('File is not a supported type')
    if not directory.endswith('/'):
        directory = directory + '/'
    return (directory, mode, file)

def read_file(directory: str, mode: str, file: str) -> pandas.DataFrame:
    file_uri = f'{directory}{file}'
    assert(os.path.isfile(file_uri))
    file = open(file_uri,encoding='utf8',mode='r')
    assert(isinstance(file,TextIOWrapper))
    contents = file.read()
    file.close()
    return contents

def get_nuance_skeleton(countrycode: str):
    xml_string = f'''<project xmlns:nuance="https://developer.nuance.com/mix/nlu/trsx" xml:lang="{countrycode}" nuance:version="2.6" nuance:enginePackVersion="hosted">
        <sources>
            <source name="nuance_custom_data" displayName="nuance_custom_data" version="1.0" type="CUSTOM" useForOOV="true"/>
        </sources>
        <ontology base="http://localhost:8080/resources/ontology-1.0.xml">
            <intents>
            </intents>
        </ontology>
        <samples>
        </samples>
    </project>'''
    root = ET.fromstring(xml_string)
    return root

def parse_humanfirst_json(directory: str, file: str, sample: int, skip: bool, countrycode: str, empty: bool, delim: str):

    # get workspace
    labelled = common.HFWorkspace()
    file_in = open(f'{directory}{file}',encoding='utf8',mode='r')
    # TODO: from_json doesn't like something to do with tags color field
    # labelled.from_json(file_in)
    json_obj = json.load(file_in)
    file_in.close()


    # get a lookup dict to check full paths
    intents = extract_intents_dict(json_obj,delim) 

    #  build samples
    attribs = []
    tags = []
    texts = []
    intentrefs = []

    examples = json_obj['examples']
    for i, example in enumerate(examples):
        if sample > 0 and i >= sample:
            break
        try:
            assert(isinstance(example,dict))
            # assumption is that this is uploaded and downloaded from Nuance
            # attributes are stored on metadata in HumanFirst
            intent_id = example['intents'][0]['intent_id']
            fullpath = intents[intent_id]['fullpath']

            if 'metadata' in example.keys():
                attrib = example['metadata']
                assert(isinstance(attrib,dict))
                if 'intentref' in attrib.keys():
                    if attrib['intentref'] != fullpath:
                        raise Exception (f'Recorded intentref {attrib["intentref"]} does not match fullpath {fullpath}')
            # if not the attributes get these defaults
            else:
                attrib = {
                    'intentref': fullpath,
                    'count': "1",
                    'excluded': "false",
                    'fullyVerified': "false",
                    'description': example['text']
                }
            
            tag = 'sample'
            text = example['text']
        except Exception as e:
            print(e)
            print(json.dumps(example,indent=2))
            if not skip:
                quit()
        attribs.append(attrib)
        tags.append(tag)
        texts.append(text)
        intentrefs.append(fullpath)

    # zip into a dataframe so we can run some checks
    df = pandas.DataFrame(zip(intentrefs,texts,tags,attribs),columns=['intentref','text','tag','attrib'])

    # get skeleton
    root = get_nuance_skeleton(countrycode)

    # determine intents to build
    all_intents = []
    for intent in intents.keys():
        all_intents.append(intents[intent]['fullpath'])
    print(f'Intents including empty is {len(all_intents)}')
    print(f'Intents that are not empty is {df["intentref"].nunique()}')
    if empty:
        print(f'Including empty intents')
        intents_to_build = all_intents
    else:
        print(f'Excluding empty intents')
        intents_to_build = df["intentref"].unique()
    

    build_intents(intents_to_build, root)
    df.apply(build_samples,args=[root],axis=1)
    print(df)
             
    write_pretty_print(directory, f'{directory}{file.replace(".json",".trsx")}', root)

    print(f'Input file contained intents: {len(intents)} built: {len(intents_to_build)} examples: {len(examples)}')

def write_pretty_print(directory: str, output_file_name: str, root: ET.Element):
    '''Write a pretty printed output to file based on imput root element'''
    output_file = open(output_file_name,encoding='utf8',mode='w')  
    output_xml_string = pretty_print_xml(
        ET.tostring(root,
            encoding='unicode',
            method='xml',
            xml_declaration=False)
        ,declaration='<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    )
    output_file.write(output_xml_string)
    output_file.close()
    print(f'Wrote to: {output_file_name}')   
   
def pretty_print_xml(input_xml_string: str, declaration: str) -> str:
    '''Do pretty print and strip empty lines and sort out declaration'''
    dom = xml.dom.minidom.parseString(input_xml_string)
    assert(isinstance(dom,xml.dom.minidom.Document))
    if not declaration.endswith('\n'):
        declaration = declaration + '\n'
    output_xml_string = dom.toprettyxml()
    assert(isinstance(output_xml_string,str))
    if output_xml_string.startswith('<?xml version="1.0" ?>\n'):
        output_xml_string = declaration + output_xml_string[23:] 
    else:
        output_xml_string = declaration + output_xml_string
    re_remove_blank_lines = re.compile(r"(?m)^[\s]*\n")
    output_xml_string = re_remove_blank_lines.sub("",output_xml_string)
    return output_xml_string

def extract_intents_dict(workspace_dict: dict, delim: str) -> dict:
    ''' Return a dict lookup of intent id to fullpath intent hierarchy'''
    # build full path of intent names
    intents = workspace_dict['intents']
    intents_dict = {}
    for intent in intents:
        intents_dict[intent['id']] = {'name':intent['name']}
    for intent in intents:
        fullpath = intent['name']
        working = intent
        while 'parent_intent_id' in working:
            fullpath = f'{intents_dict[working["parent_intent_id"]]["name"]}{delim}{fullpath}'
            working = intents_dict[working["parent_intent_id"]]
        intents_dict[intent['id']]['fullpath'] = fullpath
    return intents_dict

def build_intents(intent_names: list, root: ET.Element):
    '''built the intents onto the root element'''
    # intents like this
    #   <intent name="hello"/>
    for intent in intent_names:
        tag='intent'
        attrib = {
            'name':intent
        }
        ET.SubElement(root.find('ontology').find('intents'),tag,attrib)

def build_samples(row: pandas.Series, root: ET.Element):
    '''Build the samples onto the root element'''
    ET.SubElement(root.find('samples'),row['tag'],row['attrib']).text=row['text']

def parse_nuance_mix_xml(root: ET.Element, directory: str, file: str, skip: bool, delim: str, verbose: bool):    

    # new humanfirst workspace
    labelled = common.HFWorkspace()

    # Sanity check the intent names
    print(f'Using delimiter of "{delim}"')
    intents = []
    for intent in root.find('ontology').find('intents').findall('intent'):
        try:
            fullpath = str(intent.attrib['name'])
            hierarchy = fullpath.split(delim)
            intents.append(hierarchy)
        except Exception as e:
            print(e)
            print('Intent attrib')
            print(json.dumps(intent.attrib,indent=2))
            if not skip:
                quit()
    print(f'Nuance file contains an indicated {len(intents)} intents to process into HF format')
    print(f'There maybe more parent intents included depending on delimiter')

    # turn samples into examples    
    samples = 0
    exceptions = 0
    no_label=0
    for sample in root.find('samples').findall('sample'):

        try:
            # skip any utterances which are not labelled
            if not 'intentref' in sample.attrib.keys():
                no_label = no_label+1
                continue

            # created_at date = today
            today = datetime.date.today()

            # id for both example and conversation
            id = common.hash_string(sample.text,'example')   

            # Extract text in case of annotation
            text = sample.text
            if len(sample) > 0:
                for child in sample:
                    text = text + child.text + child.tail

            labelled.add_example(
                common.HFExample(
                    text=text,
                    id=id,
                    created_at=today,
                    intents=[labelled.intent(str(sample.attrib['intentref']).split(delim))], # get the id of the intent or creates it and gets the id of intent
                    tags=[], 
                    metadata=sample.attrib, # metadata just a dict needs to be all strings
                    context=common.HFContext(str(id),'conversation','client') # treat sample as conversation and all as client speaking
                )
            )
            samples = samples + 1
            
        except Exception as e:
            print('Warning: failed to parse this sample')
            print(e)
            print('Sample Attributes')
            print(json.dumps(sample.attrib,indent=2))
            print(f'Sample test: {sample.text}')
            exceptions = exceptions + 1
            if not skip:
                quit()

    print(f'Number of samples processed is {samples}')

    if no_label > 0:
        print(f'Skipped {no_label} samples as they do not have an intentref label in Nuance')

    if exceptions > 0:
        print(f'Could not process {exceptions} samples due to exceptions')
    
    # write labelled
    file_name_out = f'{directory}{file.replace(".trsx",".json")}'
    file_out = open(file_name_out, 'w', encoding='utf8')
    labelled.write_json(file_out)
    file_out.close()
    print(f'Wrote to output: {file_name_out}')


if __name__ == '__main__':
    main()

