"""
python tagging_intents_by_workspace.py

This accepts an input csv
--filename <filename>
With at least two columns
--intent_col <name>        Column containing names of intents (no duplicates)
--tag_col    <tag>         name of tag to apply to intent
And then a downloaded Humanfirst JSON workspace
--workspacejsonname <name>
Which it will update with the provided tags for each intent name given
You can then merge this file back into the original workspace
No api calls necessary

"""
# ******************************************************************************************************************120

# standard imports
import json

# third part imports
import pandas
import click

@click.command()
@click.option('-f', '--filename', type=str, default='./examples/example_tagging_list_ccai_bau.csv',
              help='Two column utf8 csv (other columns can be present but will be ignored)')
@click.option('-w', '--workspacejsonname', type=str, default='./workspaces/abcd/Academy-Ex03-Disambiguation-flat.json',
              help='Downloaded humanfirst workspace json file to be enriched with tags')
@click.option('-i', '--intent_col', type=str, default='intent_name',
              help='Name of intent_name column - default intent_name')
@click.option('-t', '--tag_col', type=str, default='tag_name',
              help='Name of intent_name column - default tag_name')
def main(filename: str, workspacejsonname: str,
         intent_col: str, tag_col: str):
    '''Main function'''

    # columns using.
    print(f'Columns to be read are intent_col:{intent_col} tag_col:{tag_col}')

    # accept a csv of intent names and tags based on flat Ex03 csv
    df = pandas.read_csv(filename, usecols=[intent_col, tag_col])
    # print(df)

    # get the workspace from file
    workspace_file = open(workspacejsonname, "r", encoding='utf8')
    workspace = json.load(workspace_file)
    assert isinstance(workspace, dict)
    # print(json.dumps(workspace,indent=2))
    workspace_file.close()

    # work out distinct tags from sheet
    tag_names = list(df[tag_col].unique())
    print(f'distinct names from inputs to tag tag_names:{tag_names}')

    # check what tags exist
    if "tags" in workspace.keys():
        all_tags = workspace["tags"]
        print("All current tags in workspace")
        print(all_tags)
        print("")
    else:
        print("No current tags in workspace")
        all_tags = []

    # check for those tags
    tag_index = {}
    count_to_create = 0
    tags_to_create = []
    for tag_name in tag_names:
        found = False
        for tag in all_tags:
            if tag_name == tag["name"]:
                found = True
                print(f"Tag {tag_name} found")
                tag_index[tag_name] = tag
                break
        if not found:
            count_to_create = count_to_create + 1
            tags_to_create.append(tag_name)
            print(f"Tag {tag_name} not found - please create manually")
            # not sure whether this api is exposed - note method not allowed, field mask etc?
            # humanfirst.apis.create_tag(headers,namespace,playbook,tag_id=f'tag-{tag_name}',
            # name=tag_name,description='',color=humanfirst.objects.generate_random_color())
    print(f"You need to create a total of tags: {count_to_create}")
    print(tags_to_create)
    if count_to_create > 0:
        quit()

    print("Tag_index")
    print(json.dumps(tag_index, indent=2))
    print("\n")

    # then download each intent by name
    intent_names = list(df[intent_col].unique())
    print(f'There are a total of named intents to tag: {len(intent_names)}')

    # check for intent name duplicates - this won't work if there are any
    df_gb = df[[intent_col, tag_col]].groupby(intent_col).count()
    if df_gb[tag_col].max() > 1:
        print('Your workspace has duplicate names cannot proceed')
        print(df_gb)
        quit()

    # get all intents
    if "intents" in workspace.keys():
        all_intents = workspace["intents"]
        print("All current intents in workspace")
        print(all_intents)
        print("")
    else:
        print("No current intents in workspace")
        quit()

    # make intent_index
    intent_index = {}
    for intent_name in intent_names:
        found = False
        for intent in all_intents:
            if intent_name == intent["name"]:
                found = True
                print(f"Intent {intent_name} found")
                intent_index[intent_name] = intent
                break
        if not found:
            print(f"Intent {intent_name} not found in workspace")

    # counters - note we don't check for other tags, just whether the tag on the sheet is present
    # if not add it.  So theoretically if you had an intent moving CCAI -> BAU in example
    # then you'd find it would end up with both.
    # This can be searched for in studio using the tag filter for both and resolved for the class you want.
    # or delete the BAU/CCAI tags in tags tab, then recreate them, this will clear from all intents, and this
    # script will reload all
    intents_already_with_tag = 0
    intents_tagged = 0

    # then update them with their tag
    for i, row in df.iterrows():

        # get intent
        print(f'{i} {row[intent_col]} begin:')
        intent = intent_index[row[intent_col]]
        assert isinstance(intent, dict)

        # check if tags field exists if not create it
        if not "tags" in intent.keys():
            print(
                f'- No current tags creating tags field for intent: {intent["name"]}')
            intent["tags"] = []

        # see if the tag we are trying to add is already there
        found = False
        for existing_tag in intent["tags"]:
            print(f'- Already has len(tags):{len(intent["tags"])}')
            if existing_tag["name"] == row[tag_col]:
                print(f'- Intent {row[intent_col]} already has {row[tag_col]}')
                found = True
                intents_already_with_tag = intents_already_with_tag + 1
                break

        # if it is not found add it and update intent_index
        if not found:
            print(
                f'- Intent: {row[intent_col]} does not have tag:{row[tag_col]}')
            additional_tag = {
                'id': tag_index[row[tag_col]]['id'],
                'name': tag_index[row[tag_col]]['name']
            }
            intent["tags"].append(additional_tag)
            intent_index[row[intent_col]] = intent
            intents_tagged = intents_tagged + 1

    # summary
    print(f'Already with tag: {intents_already_with_tag}')
    print(f'Added tag to tag: {intents_tagged}')
    print('\n')

    # intent_index is now fully updated
    # loop back through the original intents in the workspace and update them from the index
    # this should leave them in the same order as originally in workspace when then exported
    for i,intent in enumerate(all_intents):
        # lookup by intent name risks duplicates hence previous duplicates check
        all_intents[i] = intent_index[all_intents[i]["name"]]
    workspace["intents"] = all_intents

    # work out output name
    output_file_name = workspacejsonname.replace(".json", "-output.json")
    output_file = open(output_file_name,"w",encoding='utf8')
    json.dump(workspace, output_file,indent=2)
    output_file.close()
    print(f'Wrote output file to {output_file_name}')
    print('Use import > Model > Upload an intent file > HumanFirst JSON')
    print('Make sure you set:')
    print('☑ Merge intents by name')
    print('☑ Merge tags by name')
    print('☑ Merge entities by name')


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
