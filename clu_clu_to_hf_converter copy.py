"""
python clu_clu_to_hf_converter.py # pylint
--filename <input_hf_workspace.json>
--delimiter <delimiter>

Convert CLU json into HF
Train and Test datasets converted into tag labels
Entities not yet implemented.

"""
# *********************************************************************************************************************

# standard imports
import json
import datetime

# 3rd party imports
import pandas
import click

# custom imports
import humanfirst

@click.command()
@click.option('-f', '--filename', type=str, required=True,
              help='CLU JSON Export File')
@click.option('-d', '--delimiter', type=str, required=False, default='-',
              help='Delimiter for intent hierarchy')
def main(filename: str,
         delimiter: str) -> None:
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
    # TODO: entities
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
        "Test": "#7ec4e6" # a pastel blue for Test ame color as test-regresion in Academy Ex04
    }
    for tag in tags:
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

    # write output verion
    output_file_name = target_filename.replace(".json","_output.json")
    output_file_obj = open(output_file_name,mode='w',encoding='utf8')
    hf_workspace.write_json(output_file_obj)
    print(f'Wrote to {output_file_name}')

def intent_mapper(intent_name: str, hf_workspace: humanfirst.objects.HFWorkspace, delimiter: str) -> None:
    """Builds the parent and child structures for an intent name"""
    # clu doesn't have separate IDs (current understanding)
    intent_hierarchy = intent_name.split(delimiter)
    hf_workspace.intent(intent_hierarchy)

def utterance_mapper(row: pandas.Series,
                     hf_workspace: humanfirst.objects.HFWorkspace,
                     created_at: datetime.datetime,
                     delimiter: str) -> None:
    """Returns a clu_utterance as a dict with the language set to that passed
    and the fully qualified intent name of the id in humanfirst"""
    fully_qualified_intent_name = str(row["intent"])
    intent_hierarchy = fully_qualified_intent_name.split(delimiter)
    hf_workspace.example(
        row["text"],
        intents=[hf_workspace.intent(intent_hierarchy)],
        created_at=created_at,
        tags=[{"id": hf_workspace.tag(row["dataset"]).id }]
    )

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
