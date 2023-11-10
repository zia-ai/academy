# pylint: disable=invalid-name
"""
python ./summarize/10_turn       # pylint: disable=invalid-name

Makes a tagging file csv
"""
# ********************************************************************************************************************

# standard imports
import json
import datetime

# 3rd party imports
import click
import pandas
import humanfirst

class NoEmptySingleQuotesException(Exception):
    """This happens when there is no single quotes present"""

@click.command()
@click.option('-j', '--jointo', type=str, required=True, help='Path of original data to join to')
@click.option('-s', '--spliton', type=str, required=False, default='/', help='Parent Child Hierarchical delimiter')
def main(jointo:str, spliton:str):
    '''Main function'''

    # read original workspace
    with open(jointo,mode="r",encoding="utf8") as original_file:
        original_dict = json.load(original_file)
    df_original = pandas.json_normalize(original_dict["intents"])
    df_original.set_index(["id"],drop=False,inplace=True)
    df_original["tags"] = [[] for _ in range(len(df_original.index))]
    df_original["metadata"] = [{} for _ in range(len(df_original.index))]
    intent_id_dict = df_original[["name","tags","metadata"]].to_dict("index")

    tags = []
    for idx in intent_id_dict:
        name = intent_id_dict[idx]["name"]
        assert isinstance(name,str)
        potential_tags = name.split(spliton)
        if isinstance(potential_tags,str):
            tags.append(potential_tags)
        else:
            tags.extend(potential_tags)
    tag_names = list(set(tags))
    try:
        tag_names.remove('')
    except NoEmptySingleQuotesException as e:
        print(f"Didn't have \'\' {e}")
    print(f'Total tag names to create: {len(tag_names)}')

    tags = []
    for tag_name in tag_names:
        tags.append(build_tags(tag_name))
    original_dict["tags"] = tags

    for idx in intent_id_dict:
        for tag in tags:
            if intent_id_dict[idx]["name"].find(tag["name"]) >= 0:
                intent_id_dict[idx]["tags"].append(tag)
                intent_id_dict[idx]["metadata"][tag["name"]] = datetime.datetime.now().isoformat()

    intents = original_dict["intents"]
    for intent in intents:
        intent["tags"] = intent_id_dict[intent["id"]]["tags"]
        intent["metadata"] = intent_id_dict[intent["id"]]["metadata"]
        if "parent_intent_id" in intent.keys():
            intent["tags"].extend(intent_id_dict[intent["parent_intent_id"]]["tags"])
            intent["metadata"].update(intent_id_dict[intent["parent_intent_id"]]["metadata"])

    original_dict["intents"] = intents

   # work out a file name
    output_file_candidate = jointo.replace(".json","_output.json")

    # write to filename
    with open(output_file_candidate,mode='w',encoding='utf8') as output_file:
        json.dump(original_dict,output_file,indent=2)

    print(f'Write complete: {output_file_candidate}')

def build_tags(tag_name:str) -> dict:
    """Build the tags"""
    return {
        "id": f'tagid-{tag_name}',
        "name": tag_name,
        "color": humanfirst.objects.generate_random_color()
    }

def no_args_gen_color(_: pandas.Series) -> str:
    """Do the thing with no args"""
    return humanfirst.objects.generate_random_color()

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
