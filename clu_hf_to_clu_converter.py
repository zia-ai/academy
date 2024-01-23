"""
python clu_hf_to_clu_converter.py
--filename <input_hf_workspace.json>
--target _filename <clu_project_file.json>
--language [Optional] en-us default

Merges HF training data in an existing CLU project json.
Wipes all intents/utterances and replaces with HF data
entities not implemented yet
leaves all the project metdata from CLU untouched
assumes all data is "Train" data unless a "Test" flag present

"""
# *********************************************************************************************************************

# standard imports
import json
import warnings
import copy

# 3rd party imports
import pandas
import click

# custom imports
import humanfirst

# CLU name constants
TRAIN="Train"
TEST="Test"

@click.command()
@click.option('-f', '--filename', type=str, required=True,
              help='Source HumanFirst workspace json to pull data from')
@click.option('-t', '--target_filename', type=str, required=True,
              help='Target CLU project json to merge to')
# HF is multi lingual by default, if target NLU requires a language code
# it is set on export
@click.option('-l', '--language', type=str, required=False, default='en-us',
              help='Target CLU language for model')
@click.option('-d', '--delimiter', type=str, required=False, default='-',
              help='Delimiter for intent hierarchy')
@click.option('-i', '--indent', type=int, required=False, default='4',
              help='Indentation for output json default = 4')
@click.option('-s', '--skip', is_flag=True, required=False, default=False,
              help='Skip name length checks')
def main(filename: str,
         target_filename: str,
         language: str,
         delimiter: str,
         indent: str,
         skip: bool) -> None:
    """Main Function"""

    # verify the input files look like json
    for candidate_name in [filename,target_filename]:
        assert candidate_name.endswith('.json')

    # open source file as a dict/json
    input_file_obj = open(filename,encoding='utf8',mode='r')
    # TODO: note potential clashes with utf16 and utf8 in future depending on PVA
    hf_json = json.load(input_file_obj)
    input_file_obj.close()
    # we could convert hf_json into a HFWorkspace object using humanfirst.obj,
    # but just going to a simple json translation for now.

    # open target file as a dict/json
    output_file_obj = open(target_filename,encoding='utf8',mode='r')
    # TODO: note potential clashes with utf16 and utf8 in future depending on PVA
    clu_json = json.load(output_file_obj)
    output_file_obj.close()

    # get a HFWorkspace object to get fully qualified intent names
    hf_workspace = humanfirst.objects.HFWorkspace.from_json(hf_json,delimiter)

    # get the tag for Test dataset
    test_tag_id = None
    found = False
    for tag in hf_json["tags"]:
        if tag["name"] == "Test":
            found = True
            test_tag_id = tag["id"]
            break
    if found:
        print(f'Found test_tag_id: {test_tag_id}\n')
    else:
        print('No test_tag_id found.\n')

    # examples section
    df_examples = pandas.json_normalize(hf_json["examples"])
    print(df_examples)
    df_examples["clu_utterance"] = df_examples.apply(utterance_mapper,
                                                     args=[language,hf_workspace,test_tag_id,skip],axis=1)
    clu_json["utterances"] = df_examples["clu_utterance"].to_list()

    # find any intents that were in utterances
    # this avoids creating any parents, but also doesn't create empty children
    clu_intent_names = set()
    for clu_utterance in clu_json["utterances"]:
        clu_intent_names.add(clu_utterance["intent"])
    # set to list
    clu_intents = []
    for intent_name in clu_intent_names:
        clu_intents.append(intent_mapper(intent_name))
    #
    clu_json["assets"]["intents"] = clu_intents

    # entities
    for hf_entity in hf_json["entities"]:
        # search to see if exists
        found_entity = False
        for i,clu_entity in enumerate(clu_json["assets"]["entities"]):
            # if found replace
            if clu_entity["category"] == hf_entity["name"]:
                found_entity = True
                clu_json["assets"]["entities"][i] = entity_mapper(hf_entity,language)
                break
        # if not append
        if not found_entity:
            clu_json["assets"]["entities"].append(entity_mapper(hf_entity,language))

    # write output verion
    output_file_name = target_filename.replace(".json","_output.json")
    output_file_obj = open(output_file_name,mode='w',encoding='utf8')
    json.dump(clu_json,output_file_obj,indent=indent)
    print(f'Wrote to {output_file_name}')

def intent_mapper(intent_name: str) -> dict:
    """Returns a clu_intent as a dict with the category set to
    the passed name"""
    # clu doesn't have separate IDs (current understanding)
    return {
        "category": intent_name
    }

def entity_mapper(hf_entity: dict, language: str) -> dict:
    """converts hf entity format to clu entity format"""


    # build entity object
    clu_entity_object = {
        "category": hf_entity["name"],
        "compositionSetting": "combineComponents",
        "list": {
            "sublists": []
        }
    }

    # fill list with key values
    for hf_key_value_object in hf_entity["values"]:
        clu_sublist_object = {
            "listKey": hf_key_value_object["key_value"],
            "synonyms": [
                {
                    "language": language,
                    "values": []
                }
            ]
        }

        # fill values with values
        for synonym in hf_key_value_object["synonyms"]:
            clu_sublist_object["synonyms"][0]["values"].append(synonym["value"])

        # insert sublist into entity
        clu_entity_object["list"]["sublists"].append(copy.deepcopy(clu_sublist_object))

    # return copy of entity
    return copy.deepcopy(clu_entity_object)


def utterance_mapper(row: pandas.Series,
                     language: str,
                     hf_workspace: humanfirst.objects.HFWorkspace,
                     test_tag_id: str,
                     skip: bool) -> dict:
    """Returns a clu_utterance as a dict with the language set to that passed
    and the fully qualified intent name of the id in humanfirst"
    if the utterance is labelled as Test in HF this will be
    put in test data set"""

    # Check fit the data is labelled Train/Test - all with no labels will be Train
    dataset = TRAIN
    if "tags" in row:
        if isinstance(row["tags"],list):
            for tag in row["tags"]:
                if tag["id"] == test_tag_id:
                    print("Found")
                    dataset = TEST
                    break
        elif pandas.isna(row["tags"]):
            pass
        else:
            warnings.warn(f'Found utterance with tags not list or Na: {row}')

    intent_name = hf_workspace.get_fully_qualified_intent_name(row["intents"][0]["intent_id"])
    if len(intent_name) > 50:
        if not skip:
            raise RuntimeError(f'intent name length of {len(intent_name)} exceeds 50 chars.  {intent_name}')
    return {
        "text": row["text"],
        "language": language,
        "intent": hf_workspace.get_fully_qualified_intent_name(row["intents"][0]["intent_id"]),
        "entities": [],
        "dataset": dataset
    }

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
