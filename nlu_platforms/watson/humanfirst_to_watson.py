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
import re

# Third party imports
import click

# Custom imports


@click.command()
@click.option('--hf', type=str, required=True, help='input hf file')
@click.option('--watson', type=str, required=True, help='input watson file')
@click.option('-o', '--output', type=str, required=True, help='output watson file')
@click.option('-s', '--skill_name', type=str, required=True, help='watson skill name')
@click.option('-d', '--indent', type=int, default=4, help='ouptut file indent level')
@click.option('-f', '--fuzzy_match', type=bool, default=False, help='sets the fuzzy matching in Watson')
@click.option('-m', '--delimiter', type=str, default="-", help='delimiter for parent and child intent names')
def main(hf: str, watson: str, output: str, indent: int, fuzzy_match: bool, skill_name: str, delimiter: str):
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
    hf_file = open(hf, mode='r', encoding='utf8')
    hf_data = json.load(hf_file)
    hf_file.close()
    print(f'Loaded hf file: {hf}')
    summarise_hf_workspace(hf_data)
    
    # validate names are compatible
    re_watson_name_format = re.compile(r'^(?!sys)[A-Za-z0-9-_\.]+$')
    validate_nlu_compatible_names(hf_data,re_watson_name_format)

    # load the Watson data
    watson_file = open(watson, mode='r', encoding='utf8')
    watson_data = json.load(watson_file)
    print(f'Loaded waston file: {watson}')
    watson_file.close()

    # conversion of entities format from humanfirst json to watson json
    watson_data["entities"] = entities_conversion(hf_data, fuzzy_match)
    watson_data = cleanse_entity_synonym_case_and_duplicates(watson_data)
    print(
        f'Converted and over-written entities: {len(watson_data["entities"])}')

    # conversion of entities format from humanfirst json to watson json
    watson_data["intents"] = intents_conversion(hf_data, delimiter)
    print(
        f'Converted and over-written intents: : {len(watson_data["intents"])}')

    # Assigning the new skill name to the object
    watson_data["name"] = skill_name

    # writing to a new_watson_obj.json file
    with open(output, "w", encoding='utf8') as outfile:
        json.dump(watson_data, outfile, indent=indent)
        print(f'Wrote output Watson workspace to: {output}')
        
def cleanse_entity_synonym_case_and_duplicates(watson_data: dict) -> dict:
  for i in range(len(watson_data["entities"])):
    for j in range(len(watson_data["entities"][i]["values"])):
      diff = 0
      candidate_list = []
      candidate_set = {}
      for synonym in watson_data["entities"][i]["values"][j]["synonyms"]:
        assert(isinstance(synonym,str))
        candidate_list.append(synonym.lower())
      candidate_set = set(candidate_list)
      diff = len(candidate_list) - len(candidate_set)
      if diff > 0:
        print(f'Found {diff} case duplicates for entity: {watson_data["entities"][i]["entity"]} value: {watson_data["entities"][i]["values"][j]["value"]}')
        watson_data["entities"][i]["values"][j]["synonyms"] = list(candidate_set)
      else:
        watson_data["entities"][i]["values"][j]["synonyms"] = candidate_list
  return watson_data
       
        
def validate_nlu_compatible_names(hf_data: dict, re_nlu_name_format: re):  
  """Validates names against a provided regex for the target nlu

  Parameters
  ----------
  hf_data : dict
    HumanFirst json for a workspace
  
  re_nlu_name_format : re
    Compiled regex for the correct format of a name
    Example for Watson: r'^(?!sys)[A-Za-z0-9-_\.]+$'

  """
  invalid_entity_names = []
  for entity in hf_data["entities"]:
    if not re_nlu_name_format.search(entity["name"]):
      invalid_entity_names.append(f'{entity["name"]}') 
  invalid_intent_names = []

  for intent in hf_data["intents"]:
    if not re_nlu_name_format.search(intent["name"]):
      invalid_intent_names.append(f'{intent["name"]}')

  if len(invalid_intent_names) or len(invalid_entity_names)> 0:
    text = "*** " + f'Invalid entity names found: {",".join(invalid_entity_names)}' + " *** " \
      + f'Invalid intent names found: {",".join(invalid_intent_names)}' + " ***"
    raise Exception(text)

def summarise_hf_workspace(hf_data: dict):
    """Summarises the contents of a HF workspace

    Parameters
    ----------
    hf_data : dict
      HumanFirst json for a workspace

    """
    summary = {}
    for info in ['intents','examples','tags', 'entities']:
        summary[info] = len(hf_data[info])
    print('Summary of work to do is:')
    print(json.dumps(summary, indent=2))


def intents_conversion(hf_data: dict, delimiter: str) -> list:
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
            intent_name = find_intent_name(
                hf_data["intents"], hf_intent["parent_intent_id"], delimiter) + delimiter + hf_intent["name"]
            parent_intent_id.add(hf_intent["parent_intent_id"])
        else:
            intent_name = hf_intent["name"]

        watson_id_intent[hf_intent["id"]] = {
            "intent": intent_name,
            "examples": [],
            "description": ""
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
            print(
                f'{watson_id_intent[intent_id]["intent"]} has training examples.')

    return watson_intents


def find_intent_name(hfintents: list, id: str, delimiter: str) -> str:
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
                return find_intent_name(hfintents, hfi["parent_intent_id"], delimiter) + delimiter + hfi["name"]
            else:
                return hfi["name"]


def entities_conversion(hf_data: dict, fuzzy_match: bool) -> list:
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
                    assert(isinstance(watson_value["synonyms"],list))
                    watson_value["synonyms"].append(synonym["value"])
            watson_entity_obj["values"].append(watson_value)

        # setting the fuzzy matching property for Watson - default is False
        watson_entity_obj["fuzzy_match"] = fuzzy_match

        watson_entities.append(watson_entity_obj)
    return watson_entities


if __name__ == "__main__":
    main()
