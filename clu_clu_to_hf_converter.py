"""
python clu_clu_to_hf_converter.py # pylint
--filename <input_hf_workspace.json>
--delimiter <delimiter>
--language <language> [Optional] default en-us

Convert CLU json into HF
Train and Test datasets converted into tag labels
Only extracts one language data passed at moment

"""
# *********************************************************************************************************************

# standard imports
import json
import datetime
import warnings
import copy

# 3rd party imports
import pandas
import click

# custom imports
import humanfirst

@click.command()
@click.option('-f', '--filename', type=str, required=True,
              help='CLU JSON Export File')
@click.option('-l', '--language', type=str, required=False, default='en-us',
              help='CLU language to extract values from to load to HF')
@click.option('-d', '--delimiter', type=str, required=False, default='-',
              help='Delimiter for intent hierarchy')
@click.option('-i', '--indent', type=int, required=False, default='4',
              help='Indentation for output json default = 4')
def main(filename: str,
         delimiter: str,
         language: str,
         indent: int) -> None:
    """Main Function"""

    # verify the input files look like json
    assert filename.endswith('.json')

    # open source file as a dict/json
    input_file_obj = open(filename,encoding='utf8',mode='r')

    # TODO: note potential clashes with utf16 and utf8 in future depending on PVA
    clu_json = json.load(input_file_obj)
    input_file_obj.close()

    # get a HFWorkspace object to populate
    hf_workspace = humanfirst.objects.HFWorkspace()

    # so assumptions are:
    # create everything into new workspace - if want to to do a merge do it
    # import from hf into temp workspace in HF gui
    # start merge from temp workspace to target workspace (this is how merge works under the hood)
    # TODO: entities - not matching todo in HFWorkspace!
    # Tags will come in with Train and Test

    # examples section
    df_clu_utterances = pandas.json_normalize(clu_json["assets"]["utterances"])
    df_clu_intents = pandas.json_normalize(clu_json["assets"]["intents"])
    print(df_clu_utterances)
    print(df_clu_intents)

    # make tags
    tags = df_clu_utterances["dataset"].unique().astype(list)
    # make Train and Test consistent colours
    color_mapper = {
        "Train": "#C3E2C2", # a pastel green for Train
        "Test": "#7ec4e6", # a pastel blue for Test ame color as test-regresion in Academy Ex04
    }
    for tag in tags:
        if pandas.isna(tag):
            continue
        try:
            color = color_mapper[tag]
        except KeyError:
            color = humanfirst.objects.generate_random_color()
        hf_workspace.tag(tag=tag,color=color)

    # make intents
    df_clu_intents["category"].apply(intent_mapper,args=[hf_workspace,delimiter])
    print(hf_workspace)

    # make utterances
    created_at = datetime.datetime.now().isoformat()
    df_clu_utterances.apply(utterance_mapper,axis=1,args=[hf_workspace, created_at, delimiter])

    # target filename
    target_filename = filename.replace('.json','_hf.json')
    assert target_filename != filename

    # go to JSON to do entities as not in HFWorkspace

    # write output verion a
    output_file_name = target_filename.replace(".json","_output.json")
    output_file_obj = open(output_file_name,mode='w',encoding='utf8')
    hf_workspace.write_json(output_file_obj)
    output_file_obj.close()
    print(f'Wrote to {output_file_name} without entities')

    # reread file
    output_file_obj = open(output_file_name,mode='r',encoding='utf8')
    hf_json = json.load(output_file_obj)
    output_file_obj.close()
    clu_entities = clu_json["assets"]["entities"]

    # make entities
    hf_json["entities"] = []
    for clu_entity_object in clu_entities:

        assert isinstance(clu_entity_object,dict)
        known_entity_key_types = ["prebuilts","list","requiredComponents"]
        script_supported_types = ["list"]

        # check type and skip if unknown
        known_entity = False
        for entity_type in known_entity_key_types:
            if entity_type in clu_entity_object:
                known_entity = True
                if entity_type in script_supported_types:
                    hf_json["entities"].append(entity_mapper(clu_entity_object,language=language))
        if not known_entity:
            warnings.warn(f'Unknown entity type keys are: {clu_entity_object.keys()}')
            continue

    # write output verion with entities
    output_file_name = output_file_name.replace("_output.json","_output_entities.json")
    output_file_obj = open(output_file_name,mode='w',encoding='utf8')
    json.dump(hf_json,output_file_obj,indent=indent)
    output_file_obj.close()
    print(f'Wrote to {output_file_name} including entities')


def entity_mapper(clu_entity_object: dict, language: str) -> dict:
    """Builds a HF entity object for any clu lists"""

    # hf_entity using name to generate hash id
    isonow = datetime.datetime.now().isoformat()
    hf_entity =  {
        "id": humanfirst.objects.hash_string(clu_entity_object["category"],"entity"),
        "name": clu_entity_object["category"],
        "values": [],
        "created_at": isonow,
        "updated_at": isonow
    }

    # add key values
    for clu_sublist_object in clu_entity_object["list"]["sublists"]:
        hf_key_value_object = {
            "id": humanfirst.objects.hash_string(clu_sublist_object["listKey"],"entval"),
            "key_value": clu_sublist_object["listKey"],
            "synonyms": []
        }
        # add synonyms
        for clu_synonyms_object in clu_sublist_object["synonyms"]:
            found_language = False
            if clu_synonyms_object["language"] == language:
                found_language = True
                for clu_synonym in clu_synonyms_object["values"]:
                    hf_synonym = {
                        "value": clu_synonym
                    }
                    hf_key_value_object["synonyms"].append(copy.deepcopy(hf_synonym))
            if not found_language:
                raise RuntimeError(f'Could not find language synonyms for {language}')
            hf_entity["values"].append(copy.deepcopy(hf_key_value_object))

    return copy.deepcopy(hf_entity)


def intent_mapper(intent_name: str, hf_workspace: humanfirst.objects.HFWorkspace, delimiter: str) -> None:
    """Builds the parent and child structures for an intent name"""
    # clu doesn't have separate IDs (current understanding)
    intent_hierarchy = intent_name.split(delimiter)
    hf_workspace.intent(intent_hierarchy)

def utterance_mapper(row: pandas.Series,
                     hf_workspace: humanfirst.objects.HFWorkspace,
                     created_at: datetime.datetime,
                     delimiter: str) -> None:
    """Builds HF example"""
    fully_qualified_intent_name = str(row["intent"])
    intent_hierarchy = fully_qualified_intent_name.split(delimiter)
    try:
        tag_name = row["dataset"]
        if pandas.isna(tag_name):
            tag_name = "Train"
    except KeyError:
        tag_name = "Train"
    hf_workspace.example(
        row["text"],
        intents=[hf_workspace.intent(intent_hierarchy)],
        created_at=created_at,
        tags=[{"id": hf_workspace.tag(tag_name).id }]
    )

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
