#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python simple_json_labelled.py
# basic hardcoded example
#
# *****************************************************************************

# standard imports
import humanfirst
import json
import click
from datetime import datetime

@click.command()
@click.option('-f','--filename',type=str,required=False,default="./examples/json_model_example.json",help='Input File')
def main(filename: str):
    # read the json here
    input_json = json.loads(open(filename,'r',encoding='utf8').read())
    
    process(input_json,filename)
   
def process(input_json: dict, filename: str):
      
    # declare a labelled workspace
    labelled = humanfirst.HFWorkspace()
    
    # add a loop through the json file and for every example in it create the intent and example
    intent_names = list(input_json.keys())
    for name in intent_names:
               
        # get the group from the intent name or whereevet
        assert(isinstance(name,str))
        group = name.split('_')[0]
             
        # create the intent name in the workspace - this gives us the intent to associate with each example for this intent we are importing.
        intents = [labelled.intent(name_or_hier=[group,name])]
        
        # get the full input_intent for the language we are interested in
        input_intent = input_json[name]['EN']
        assert(isinstance(input_intent,dict))
        for example_id in list(input_intent.keys()):

            # build example
            example = humanfirst.HFExample(
                    text=input_intent[example_id],
                    id=example_id,
                    created_at=datetime.now(),
                    intents=intents,
                    tags=[], # recommend uploading metadata for unlabelled and tags for labelled
                    metadata={},
            )

            # add example to workspace
            labelled.add_example(example)
    
    # write to output
    file_out = open(filename.replace(".json","_output.json"),mode='w',encoding='utf8')
    labelled.write_json(file_out)
    file_out.close()

if __name__ == '__main__':
    main()