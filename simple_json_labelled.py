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
from datetime import datetime

def main():

    # read the json here
      
    # declare a labelled workspace
    labelled = humanfirst.HFWorkspace()
    
    # add a loop through the json file and for every example in it create the intent and example
        
    # create the intent name in the workspace
    intents = [labelled.intent(name_or_hier=['BE','BE_DE_SOME_INTENT_NAME'])]
    
    # build examples
    example = humanfirst.HFExample(
        text="Some utterance text",
        id=f'example-SomeIDString',
        created_at=datetime.now(),
        intents=intents,
        tags=[], # recommend uploading metadata for unlabelled and tags for labelled
        metadata={},
    )
    
    # add example to workspace
    labelled.add_example(example)
    
    # write to output
    file_out = open('./data/some_output_filename.json',mode='w',encoding='utf8')
    labelled.write_json(file_out)
    file_out.close()

if __name__ == '__main__':
    main()