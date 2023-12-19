"""
python find_intents_in_entity_settings.py -f <YOUR FILENAME>

This script accepts HF labelled workspace and 
lists all the fully qualified intent names that are present in the entity settings

"""
# *********************************************************************************************************************

# standard imports
import json

# third party imports
import click
import pandas

# custom imports
import humanfirst

@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
def main(input_filename: str):
    """Main function"""
    file = open(input_filename, mode = "r", encoding = "utf8")
    workspace_json = json.load(file)
    file.close()
    if "entities" in workspace_json.keys():
        df = pandas.json_normalize(workspace_json["entities"],sep="-")
    else:
        raise RuntimeError("Entities are not present for the given workspace")


    col_list = df.columns.to_list()
    if "settings-denied_intent_ids" in col_list and "settings-allowed_intent_ids" in col_list:
        get_intent_names_in_entity_settings("settings-denied_intent_ids", df, workspace_json)
        get_intent_names_in_entity_settings("settings-allowed_intent_ids", df, workspace_json)
    elif "settings-denied_intent_ids" in col_list:
        get_intent_names_in_entity_settings("settings-denied_intent_ids", df, workspace_json)
    elif "settings-allowed_intent_ids" in col_list:
        get_intent_names_in_entity_settings("settings-allowed_intent_ids", df, workspace_json)
    else:
        raise RuntimeError("None of the entities have any denied or allowed intents")


def get_intent_names_in_entity_settings(entity_setting: str, df: pandas.DataFrame, workspace_json: dict):
    """Find fully qualified intent names in entity settings"""

    df = df.loc[~df[entity_setting].isna()]

    entity = {}
    for _,row in df.iterrows():
        entity[row["name"]] = row[entity_setting]


    labelled_workspace = humanfirst.objects.HFWorkspace.from_json(workspace_json,delimiter=" -> ")
    print(entity_setting.replace("settings-",""))
    for entity_name,intent_id_list in entity.items():
        print(entity_name)
        for intent in intent_id_list:
            intent_name = labelled_workspace.get_fully_qualified_intent_name(intent)
            print(f"         {intent_name}")

    print()


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
