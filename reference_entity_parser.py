#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python reference_entity_parser.py 
# 
#  --download, optional    Build an example input file and quit
#  --inputfile             ./data/mycsv_list_of_values.csv
#  --outputfile            ./data/output_entities.json
#
# Example of creating a large reference entity from an external source
# Here creating 1000 English Towns from Wikipedia.
# Note this is only if you need a list like that because your target NLU
# does not support or is not trained for contextual or system detection methods
# 
# Attributation: Example downloader uses data from this Wikipedia page
# https://en.wikipedia.org/wiki/List_of_towns_in_England
#
# Under :
# https://en.wikipedia.org/wiki/Wikipedia:Text_of_Creative_Commons_Attribution-ShareAlike_3.0_Unported_License
# https://en.wikipedia.org/wiki/Wikipedia:Reusing_Wikipedia_content
#
# ./examples/towns.csv and towns.json are redistributed under
# Creative Commons Attribution 4.0 International License. 
# see ./LICENSE.txt
#
#
# *****************************************************************************

# standard imports
import re
import datetime
import json

# third party imports
import click
import pandas
from mediawikiapi import MediaWikiAPI
from bs4 import BeautifulSoup 
import bs4

# custom imports
import humanfirst # TODO: entities


@click.command()
@click.option('-d', '--download', is_flag=True, default=False, help='Whether to build the input file as a download')
@click.option('-i', '--inputfile', type=str, default='./examples/towns.csv', help='What the input file is')
@click.option('-o', '--outputfile', type=str, default='./examples/towns.json', help='Where to output file')
def main(download: bool, inputfile: str, outputfile: str):

    # if download build an example input file with around 1000 English towns    
    if download:
        download_towns(inputfile)
    
    # otherwise just load your values and upload
    # assumes first row is header row
    df = pandas.read_csv(inputfile,header=0,encoding='utf8')
    
    now = datetime.datetime.now()
    now = str(now.isoformat())
    
    # This is the entity format
    entity_name = 'towns'
    entity = {
        "id": humanfirst.hash_string(entity_name,'entity'),
        "name": entity_name,
        "values": [],
        "created_at": now, # could obviously make these some other set of dates in the loader
        "updated_at": now
    }

    # Each value created like this
    values = []
    for town in df['towns']:
        value = {
            "id": humanfirst.hash_string(town,'value') ,
            "key_value": town,
            "created_at": now,
            "updated_at": now
        }
        values.append(value)  
    
    # Add all the entities as a list to the entity
    entity['values'] = values       
    
    # add all entities to a otherwise empty json so won't overwrite anything else
    workspace_json = {
        "entities": [entity]
    }
    
    # write output.
    file_out = open(outputfile,'w',encoding='utf8')
    json.dump(workspace_json,file_out,indent=2)
    file_out.close
    
# Functions below here only required for creating the example, above is all code you need to upload entities
    
def download_towns(inputfile: str):
    '''Build an example csv or txt input file'''
    towns = wiki_get_list('List_of_towns_in_England',1)
    df = pandas.DataFrame(towns,columns=['towns'])
    
    # cleanse
    df = cleanse_rows(df)
    
    # deduplicate
    df = df[~df.duplicated(keep='last')]
    
    # sort
    df = df.sort_values('towns').reset_index(drop=True)
    
    # output
    df.to_csv(inputfile,header=True,index=False)
    print(df)
    print('Built example town input file will now quit')
    quit()
    
def cleanse_rows(df: pandas.DataFrame) -> pandas.DataFrame:
    '''Cleanse function to clean up downloaded WikiPedia data'''
    re_strip = re.compile(r',.*$')
    df['towns'] = df['towns'].apply(strip_something,args=[re_strip])
    re_bracket_bits = re.compile(r'\(.*\)')
    df['towns'] = df['towns'].apply(strip_something,args=[re_bracket_bits])
    return df
    
def strip_something(input_string: str, re_strip: re) -> str:
    '''Use a provided regex to strip the data from the provided input string'''
    output_string = re_strip.sub('',input_string)
    assert(isinstance(output_string,str))
    output_string = output_string.strip()
    return output_string   
    
def wiki_get_list(wiki_page_title:str, col: int) -> list:
    '''Download a page from wikipedia and mine out a column of data'''
    
    # you'll need to customise this for your table if you want to use.
    
    # load page 
    mediawikiapi = MediaWikiAPI()
    test_page = mediawikiapi.page(wiki_page_title)

    # scrape the HTML with BeautifulSoup to find tables
    soup = BeautifulSoup(test_page.html(), 'html.parser')
    tables = soup.findAll("table", { "class" : "wikitable sortable" })
    
    # this has tables in for each letter of the alphabet
    output = []
    for table in tables:
        assert(isinstance(table,bs4.element.Tag))           
        tbody = table.find("tbody")      
        assert(isinstance(tbody,bs4.element.Tag))           
        for i, c in enumerate(tbody.contents):
            # skip the header
            if i in [0]:
                continue
            if isinstance(c,bs4.element.Tag):
                assert(isinstance(c,bs4.element.Tag))           
                # check if a row of data
                if c.name == "tr":
                    # get the column interested in note - \n makes this annoying
                    column = c.contents[col]
                    assert(isinstance(column,bs4.element.Tag))
                    data = column.contents[0]
                    assert(isinstance(data,bs4.element.Tag))
                    # print(f'{i}: {data.attrs["title"]}') 
                    output.append(data.attrs["title"])
            elif isinstance(c,bs4.element.NavigableString):
                # skip these
                # print(f'{i}: Skipped')
                continue
            else:
                print(type(c))
    return output

if __name__ == '__main__':
    main()