#!/usr/bin/env python
# -*- coding: utf-8 -*-
# *******************************************************************************************************************120
#  Code Language: python
#  Script: humanfirst_to_watson.py
#  Imports: json, click
#  Functions: main(), entities_conversion(), intents_conversion(), find_intent_name()
#  Description: Takes in two files - HumanFirst json obj and Watson json obj and converts the intents and entities in
#               HumanFirst json format to watson json format and updates the intents and enitites in watson with the
#               converted values
#
# **********************************************************************************************************************

# Core imports
import json

# Third party imports
import click

# Custom imports

@click.command()
@click.option('--hf', type=str,required=True,help='input hf file')
@click.option('--watson', type=str,required=True,help='input watson file')
@click.option('-o','--output', type=str,required=True,help='output watson file')
@click.option('-s','--skill_name', type=str,required=True,help='watson skill name')
@click.option('-d','--indent', type=int,default=4,help='ouptut file indent level')
@click.option('-f','--fuzzy_match', type=bool,default=False,help='sets the fuzzy matching in Watson')
@click.option('-m','--delimiter', type=str,default="-",help='delimiter for parent and child intent names')

def main(hf: str, watson:str, output: str, indent: int, fuzzy_match: bool, skill_name: str, delimiter: str):
  """Main function

  Parameters
  ----------
  hf : str
    HumanFirst json file name 
  
  watson : str
    Watson json file name 
  
  output : str
    updated Watson json file name 
  
  indent : int
    specifies the level of intent in output file
  
  skill_name: str
    Watson skill name
  
  Returns
  -------
  None
  """

  # load the HumanFirst data
  hf_file = open(hf,mode='r',encoding='utf8')
  hf_data = json.load(hf_file)
  print(f'\nLoaded: {hf}')
  hf_file.close()

  # load the Watson data
  watson_file = open(watson,mode='r',encoding='utf8')
  watson_data = json.load(watson_file)
  print(f'\nLoaded: {watson}')
  watson_file.close()

  # conversion of entities format from humanfirst json to watson json
  watson_data["entities"] = entities_conversion(hf_data,fuzzy_match)
  
  # conversion of entities format from humanfirst json to watson json
  watson_data["intents"] = intents_conversion(hf_data, delimiter)

  # Assigning the new skill name to the object
  watson_data["name"] = skill_name

  # writing to a new_watson_obj.json file
  with open(output, "w",encoding='utf8') as outfile:
        json.dump(watson_data,outfile,indent=indent)
        
def intents_conversion(hf_data : dict, delimiter: str) -> list:
  """Converts the HumanFirst intents into Watson intents

  Parameters
  ----------
  hf_data : dict
    represents the HumanFirst json 

  Returns
  -------
  wastson_intents: list
    list of dictionary of Watson intents and its corresponding examples
  """

  # getting the intent name sorted
  # watson cannot have hierarchical intent taxonomy whereas humanfirst can.
  # creating parent prefixed intent names
  watson_id_intent = {}
  parent_intent_id = set()
  for hf_intent in hf_data["intents"]:
    if "parent_intent_id" in hf_intent:
      intent_name = find_intent_name(hf_data["intents"],hf_intent["parent_intent_id"],delimiter) + delimiter + hf_intent["name"]
      parent_intent_id.add(hf_intent["parent_intent_id"]) 
    else:
      intent_name = hf_intent["name"]
    
    watson_id_intent[hf_intent["id"]] = {
      "intent":intent_name,
      "examples":[],
      "description":""
    }
  
  # assigning examples to each parent prefixed intents
  # eliminate the negative training examples
  flag = 0
  for hf_example in hf_data["examples"]:
    try:
      if ("negative" in hf_example["intents"][0]):
        if flag == 0:
          flag = 1
          raise Exception()
      else:
        watson_example = {}
        watson_example["text"] = hf_example["text"]

        #  checking if there are any annotations and copying the location of entity in the text
        if "entities" in hf_example:
          watson_example["mentions"] = []
          for entity in hf_example["entities"]:
            w_entity = {
              "entity": entity["name"],
              "location": [entity["span"]["from_character"], entity["span"]["to_character"]]
            }
            watson_example["mentions"].append(w_entity)
        intent_id = hf_example["intents"][0]["intent_id"]
        watson_id_intent[intent_id]["examples"].append(watson_example)
    except Exception:
      print("""\nYou have downloaded the data along with the negative training examples.\
          \nWatson doesn't need them, so they will be neglected automatically.\n""")
  
  watson_intents = []
  flag = 0
  for intent_id in watson_id_intent:
    # check for parent intents with training examples and neglect them
    try:
      if intent_id in parent_intent_id:
        if len(watson_id_intent[intent_id]["examples"]) > 0:
          raise Exception()
      else:
        watson_intents.append(watson_id_intent[intent_id])
    except Exception:
      if flag == 0:
        flag = 1
        print("""Parent intents should not have training examples, as it would be treated separately from its children in Watson. \
        \nParent intents with training examples are:\n""")
      print(f'{watson_id_intent[intent_id]["intent"]} has training examples.')

  return watson_intents

def find_intent_name(hfintents : list, id : str, delimiter: str) -> str:
  """Recursive function to find the parent intent name

  Parameters
  ----------
  hfintents : list
    list of dictionaries of HumanFirst intents
    
  id : str
    parent intent id

  Returns
  -------
  wastson_intents: list
    list of dictionaries of watson intents and its corresponding examples
  """

  for hfi in hfintents:
    if hfi["id"] == id:
      if "parent_intent_id" in hfi:
        return find_intent_name(hfintents,hfi["parent_intent_id"],delimiter) + delimiter + hfi["name"]
      else:
        return hfi["name"]

def entities_conversion(hf_data : dict, fuzzy_match: bool) -> list:
  """Converts the HumanFirst entities into Watson entities

  Parameters
  ----------
  hf_data : dict
    represents the HumanFirst json
  
  fuzzy_match : bool
    sets the fuzzy matching of entities in Watson

  Returns
  -------
  wastson_entities: list
    list of dictionary of Watson entities and its corresponding values
  """

  watson_entities = []
  for hf_entity_obj in hf_data["entities"]:
    watson_entity_obj = {}
    watson_entity_obj["entity"] = hf_entity_obj["name"]
    watson_entity_obj["values"] = []
    for hf_value in hf_entity_obj["values"]:
      watson_value = {
          "type": "synonyms",
          "value": hf_value["key_value"],
          "synonyms": []
      }
      for synonym in hf_value["synonyms"]:
        if synonym["value"] != watson_value["value"]:
          watson_value["synonyms"].append(synonym["value"])
      watson_entity_obj["values"].append(watson_value)
    
    # setting the fuzzy matching property for Watson - default is False
    watson_entity_obj["fuzzy_match"] = fuzzy_match

    watson_entities.append(watson_entity_obj)
    return watson_entities

if __name__ == "__main__":
    main()