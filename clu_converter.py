"""
python clu_converter.py
--filename <input_hf_workspace.json>
--target _filename <clu_project_file.json>
--language [Optional] en-us default

Merges HF training data in an existing CLU project json.
Wipes all intents/utterances and replaces with HF data
entities not implemented yet
leaves all the project metdata from CLU untouched
assumes all data is "Train" data.

"""
# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import pandas
import click

# custom imports
import humanfirst


@click.command()
@click.option('-f', '--filename', type=str, required=True,
              help='Source HumanFirst workspace json to pull data from')
@click.option('-t', '--target_filename', type=str, required=True,
              help='Target CLU project json to merge to')
@click.option('-f', '--filename', type=str, required=True,
              help='Source HumanFirst workspace json to pull data from')
# HF is multi lingual by default, if target NLU requires a language code
# it is set on export
@click.option('-l', '--language', type=str, required=False, default='en-us',
              help='Target CLU language for model')
@click.option('-d', '--delimiter', type=str, required=False, default='-',
              help='Delimiter for intent hierarchy')
def main(filename: str,
         target_filename: str,
         language: str,
         delimiter: str) -> None:
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

    # examples section
    df_examples = pandas.json_normalize(hf_json["examples"])
    df_examples["clu_utterance"] = df_examples.apply(utterance_mapper,args=[language,hf_workspace],axis=1)
    clu_json["utterances"] = df_examples["clu_utterance"].to_list()

    # find any intents that were in utterances
    # this avoids creating any partents, but also doesn't create empty children
    clu_intent_names = set()
    for clu_utterance in clu_json["utterances"]:
        clu_intent_names.add(clu_utterance["intent"])
    # set to list
    clu_intents = []
    for intent_name in clu_intent_names:
        clu_intents.append(intent_mapper(intent_name))
    clu_json["assets"]["intents"] = clu_intents

    # write output verion
    output_file_name = target_filename.replace(".json","_output.json")
    output_file_obj = open(output_file_name,mode='w',encoding='utf8')
    json.dump(clu_json,output_file_obj,indent=2)
    print(f'Wrote to {output_file_name}')

def intent_mapper(intent_name: str) -> dict:
    """Returns a clu_intent as a dict with the category set to
    the passed name"""
    # clu doesn't have separate IDs (current understanding)
    return {
        "category": intent_name
    }

def utterance_mapper(row: pandas.Series, language: str, hf_workspace: humanfirst.objects.HFWorkspace) -> dict:
    """Returns a clu_utterance as a dict with the language set to that passed
    and the fully qualified intent name of the id in humanfirst"""
    return {
        "text": row["text"],
        "language": language,
        "intent": hf_workspace.get_fully_qualified_intent_name(row["intents"][0]["intent_id"]),
        "entities": [],
        "dataset": "Train"
    }

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
