"""
python coverage_download_heatmap.py
 -n <namespace>
 -b <playbook id>

Make sure you have setup your nlu engine excluding parents

This script downloads the data from the intent Tab for unique or total coverage.
It looks up the full qualified name (FQN) for the intent based on a provided delimiter
It calculates other heatmap box for a given clip
It can be used to generate data for total or unique
and for generated or unlabelled data.
It then builds a heatmap using plotly auto detecting the number of levels in the hierarchy

-d <delimiter>      used for parent to child intent generation of fqn
-u                  flag to switch from default total behaviour to unique
-g                  flag to switch from unlabelled to generated data

"""
# ******************************************************************************************************************120

# standard imports
import io
import os
import json

# 3rd party imports
import click
import pandas
import plotly.express as px

# custom imports
import humanfirst

@click.command()
# Mandatory
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
# Set in env variables normally
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
# optional to override defaults
@click.option('-c', '--clip', type=int, required=False, default=70, help='Clip Point')
@click.option('-d', '--hierarchical_delimiter', type=str, required=False, default='-',
              help='Delimiter for hierarchical intents')
@click.option('-u', '--unique', is_flag=True, type=bool, required=False, default=False,
              help='Set for unique coverage rather than total coverage')
# experimental
@click.option('-g', '--generated', is_flag=True, type=bool, required=False, default=False,
              help='Download generated data instead of unlabelled')
def main(
         username: str, password: str,
         namespace: str, playbook: str,
         hierarchical_delimiter: str,
         clip: float,
         unique: bool,
         generated: bool):
    """Main Function"""

    # do authorisation
    hf_api = humanfirst.apis.HFAPI(username=username,password=password)

    # check how many converation sets there are
    playbook_info = hf_api.get_playbook_info(playbook=playbook,namespace=namespace)
    num_conversation_sets = len(playbook_info["conversationSets"])
    if num_conversation_sets == 0:
        raise RuntimeError("No conversationset attached")
    elif num_conversation_sets > 1:
        print(f'Warning: {num_conversation_sets} attached - check whether intentional')
    print("\nConvosets:")
    print(json.dumps(playbook_info["conversationSets"], indent=2))

    # get the playbook name.
    playbook_name = playbook_info["name"]
    assert isinstance(playbook_name,str)

    # Check for trained NLU engines with runids
    runs = hf_api.list_trained_nlu(namespace=namespace,playbook=playbook)
    df_runs = pandas.json_normalize(runs)
    # TODO: doesn't currently do anything with this run_id
    # could look up the latest fort he latest nluIds

    # check how many nlus and get the default
    nlu_engines = hf_api.get_nlu_engines(namespace=namespace,playbook=playbook)
    df_nlu_engines = pandas.json_normalize(nlu_engines)
    default_nlu_engine = None
    for nlu in nlu_engines:
        if nlu["isDefault"] is True:

            default_nlu_engine = nlu["id"]

            # check if default has parents
            if not "hierarchicalRemapScore" in nlu:
                err = 'Please ensure "include parent intents in predictions"'
                err = err + 'is set and set to false on your NLU engine'
                raise RuntimeError(err)
            elif not nlu["hierarchicalRemapScore"] is False:
                err = '"include parent intents in predictions" is set to True on your NLU engine'
                err = err + '- needs to be set to False'
                raise RuntimeError(err)

            break
    if default_nlu_engine is None:
        raise RuntimeError("Can't find default nlu engine")
    print(f'\nDefault NLU engine: {default_nlu_engine}')

    # get the coverage_export
    data_selection = 1 # DATA_TYPE_ALL - this is what the GUI defaults to.
    if generated:
        data_selection = 3 # DATA_TYPE_GENERATED
    coverage_export = hf_api.export_intents_coverage(namespace=namespace,
                                                     playbook=playbook,
                                                     confidence_threshold=clip,
                                                     coverage_type=1, # TOTAL
                                                     data_selection=data_selection, # ALL
                                                     model_id=default_nlu_engine)

    df = pandas.read_csv(io.StringIO(coverage_export),delimiter=",")

    # get workspace to lookup names
    workspace_dict = hf_api.get_playbook(namespace=namespace,
                                         playbook=playbook,
                                         hierarchical_delimiter=hierarchical_delimiter)
    workspace = humanfirst.objects.HFWorkspace.from_json(workspace_dict,delimiter=hierarchical_delimiter)
    assert isinstance(workspace,humanfirst.objects.HFWorkspace)
    print("Downloaded workspace")

    # Two different names in case unique or total set variables for them adjusting names
    unique_prefix = ''
    if unique:
        unique_prefix = 'unique_'
    utterance_count = unique_prefix + 'utterance_count'
    utterance_hier_count = unique_prefix + 'utterance_hier_count'
    utterance_score_histogram_thresholded_sum = unique_prefix + 'utterance_score_histogram_thresholded_sum'
    utterance_hier_score_histogram_thresholded_sum = unique_prefix + 'utterance_hier_score_histogram_thresholded_sum'

    # get FQN
    df["fqn_list"] = df["intent_id"].apply(workspace.get_fully_qualified_intent_name).str.split(hierarchical_delimiter)
    df = df.join(pandas.DataFrame(df["fqn_list"].values.tolist()))

    # get levels
    max_levels = df["fqn_list"].apply(len).max()
    levels = list(range(0,max_levels,1))


    # work out other
    other = {
        'intent_id':'other',
        'model_id': df.loc[0,"model_id"],
        utterance_count: 0,
        utterance_hier_count: 0,
        utterance_score_histogram_thresholded_sum: 0,
        utterance_hier_score_histogram_thresholded_sum: 0
    }
    other[utterance_score_histogram_thresholded_sum] = df[utterance_count].sum() - df[utterance_score_histogram_thresholded_sum].sum()
    for level in levels:
        if level == 0:
            other[level] = 'other'
        else:
            other[level] = None
    df = pandas.concat([df,pandas.json_normalize(other)],axis=0).reset_index()
    pandas.set_option('display.max_rows',1000)

    # drop any rows with 0
    before_drop = df.shape[0]
    df = df[~(df[utterance_score_histogram_thresholded_sum]==0)]
    after_drop = df.shape[0]
    print(f'Dropped categories with no results, before: {before_drop} after: {after_drop}')


    # Create the treemap plot using Plotly - using px.Constant("<br>") makes a prettier hover info for the root level
    fig = px.treemap(df, path=[px.Constant("<br>")] + levels, values=utterance_score_histogram_thresholded_sum)

    # format main body of treemap and add labels
    # colours set using template
    fig.update_traces(marker={"cornerradius":3})
    fig.update_layout(template='plotly', width=1500, height=750)
    fig.update_traces(textinfo="label + percent root")
    fig.update_traces(root_color="#343D54")


    # set the label font and size
    fig.data[0]['textfont']['size'] = 12
    fig.data[0]['textfont']['family'] = 'Calibri'

    #format title, hover info fonts and background
    fig.update_layout(
    title= "Overview of categories by total count",
    title_y=0.98,
    title_font_color = 'white',
    title_font_size = 24,
    hoverlabel=dict(
            font_size=16,
            font_family="Calibri"
        ),
    paper_bgcolor="#343D54",
    )

    # Update the hover info
    fig.data[0].hovertemplate = (
    '<b>%{label}</b>'
        '<br>' +
    'Count: %{value}' +
    '<br>' +
    'Percent of all utterances: <i>%{percentRoot:.1%} </i>'+
    '<br>' +
    'Percent of all parent category: <i>%{percentParent:.1%} </i>'+
    '<br>'
    )
    #change margin size - make the plot bigger within the frame
    fig.update_layout(margin = dict(t=38, l=10, r=10, b=15))

    # print final data this is based on
    print(df[levels + [utterance_score_histogram_thresholded_sum]])

    # ouptut to ./data/based on workspace name
    output_filename=os.path.join('data','html',f'{playbook}_{playbook_name.replace(" ","_")}.html')
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')

    # overall totals
    print(f'Total number of utterances is: {df[utterance_count].sum()}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
