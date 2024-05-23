"""
python coverage_quality_bar.py
-n <namespace id>
-b <playbook>
-s <field to create stack bars on>
-l <pipeline name with the summarisation against the model

Example:
python coverage_quality_bar.py -n humanfirst-abcd-summarised -b playbook-UHP4VVQM2VFRXMOXNBFUOBRH -s total_score -l key_issue

Function:
Downloads the generatd data for a pipeline name and extracts the class for the model in the worksapce against it.
Also downloads the workspace in order to work out the intent names
Then downloads the unlabelled data with an assumed metadata annotation like total_score
And produces a horizontal two level bar chart

Limitations:
Plotly horizontal bar charts only support two levels
This only works with conversations

Options:
-c <clip level                 this script it is in 0.35 format float>
-d <hierarchical delimiter>    how to join your fully qualified intent names
-w <nlu id>                    if you want to select a particular NLU on workspaces which have many

"""
# ******************************************************************************************************************120

# standard imports
import json
import io
import os

# 3rd party imports
import click
import pandas
import plotly.graph_objects # express can't do multi category

# custom imports
import humanfirst

@click.command()
# Mandatory
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-s', '--stack_field', type=str, required=True, help='Metadata field to stack bars on i.e total score')
@click.option('-l', '--pipeline', type=str, required=True, help='Name of pipeline to get results from')

# Set in env variables normally
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
# optional to override defaults
@click.option('-c', '--clip', type=float, required=False, default=0.35, help='Clip Point as 0.00 format')
@click.option('-d', '--hierarchical_delimiter', type=str, required=False, default='-',
              help='Delimiter for hierarchical intents')
@click.option('-w', '--which_nlu', type=str, required=False, default='',
              help='NLU name like nlu-57QM7EN3UFEZPGH7PI3FCJGV(HumanFirst NLU) if blank will just take the first')
def main(namespace: str, playbook: str,
         stack_field: str,
         pipeline: str,
         username: str, password: str,
         clip: float,
         hierarchical_delimiter: str,
         which_nlu: str) -> None:
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
    conversation_set_id = playbook_info["conversationSets"][0]["id"]
    print(f'Using conversation_set_id: {conversation_set_id}')

    # get the playbook name.
    playbook_name = playbook_info["name"]
    assert isinstance(playbook_name,str)

    # Check for trained NLU engines with runids
    # runs = hf_api.list_trained_nlu(namespace=namespace,playbook=playbook)
    # df_runs = pandas.json_normalize(runs)
    # TODO: doesn't currently do anything with this run_id
    # could look up the latest fort he latest nluIds

    # check how many nlus and get the default
    nlu_engines = hf_api.get_nlu_engines(namespace=namespace,playbook=playbook)
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

    # get workspace to lookup names
    workspace_dict = hf_api.get_playbook(namespace=namespace,
                                         playbook=playbook,
                                         hierarchical_delimiter=hierarchical_delimiter)
    workspace = humanfirst.objects.HFWorkspace.from_json(workspace_dict,delimiter=hierarchical_delimiter)
    assert isinstance(workspace,humanfirst.objects.HFWorkspace)
    print("Downloaded workspace")


    # work out which pipelines there are
    pipelines = hf_api.list_playbook_pipelines(namespace=namespace,playbook_id=playbook)
    if len(pipelines) == 0:
        raise RuntimeError("No pipelines to download from have you created it")
    count_found = 0
    pipeline_id = None
    for pl in pipelines:
        if pl["name"] == pipeline:
            count_found = count_found + 1
            pipeline_id = pl["id"]
            pipeline_step_id = pl["steps"][0]["id"] # Assumption that each pipeline has one step at the moment
    if count_found == 0:
        raise RuntimeError(f'Couldn\'t find a pipeline called: {pipeline}')
    elif count_found > 1:
        raise RuntimeError(f'Multiple pipelines called: {pipeline}')
    else:
        print(f'Found pipeline: {pipeline} id: {pipeline_id} step_id: {pipeline_step_id}')

    # download the pipeline data
    data = hf_api.export_query_conversation_inputs(
        namespace=namespace,
        playbook_id=playbook,
        pipeline_id=pipeline_id,
        pipeline_step_id=pipeline_step_id,
        download_format=2,
        dedup_by_hash=False,
        dedup_by_convo=False,
        source_kind = 2 # SOURCE_KIND_GENERATED
    )
    df_pipeline = pandas.read_csv(io.StringIO(data),delimiter=",")
    print(f'Downloaded csv for pipeline run: {df_pipeline.shape}')
    csv_pipeline = df_pipeline.shape[0]

    # Get the correct column names
    if which_nlu == '':
        which_nlu = default_nlu_engine
    col_list = df_pipeline.columns.to_list()
    top_matching_intent_id = get_col_name("top_matching_intent_id",col_list,which_nlu)
    top_matching_intent_score = get_col_name("top_matching_intent_score",col_list,which_nlu)

    # calc clips
    df_pipeline = df_pipeline.apply(apply_clip,
                  args=[
                            clip,
                            top_matching_intent_score,
                            top_matching_intent_id,
                            workspace
                        ],
                  axis=1)
    print('Calculated clips')

    # Now get the metadata data (which we have put on using batch actions)
    data = hf_api.export_query_conversation_inputs(
        namespace=namespace,
        playbook_id=playbook,
        exists_filter_key_name=stack_field,
        download_format=2,
        dedup_by_hash=False,
        dedup_by_convo=False,
        source_kind=1 # SOURCE_KIND_UNLABELLED
    )
    df = pandas.read_csv(io.StringIO(data),delimiter=",")
    print(f'Downloaded csv from unlabelled: {df.shape}')
    csv_unlabelled = df.shape[0]
    if csv_pipeline != csv_unlabelled:
        print(f'Warning: csv_pipeline: {csv_pipeline} csv_unlabelled: {csv_unlabelled}')

    # expand dynamicaly that to a list and then columns per level
    df_pipeline["fqn_list"] = df_pipeline["fqn"].str.split(hierarchical_delimiter)
    df_pipeline = df_pipeline.join(pandas.DataFrame(df_pipeline["fqn_list"].values.tolist()))

    # get levels
    max_levels = df_pipeline["fqn_list"].apply(len).max()
    levels = list(range(0,max_levels,1))
    if len(levels) > 2:
        print("Warning: Plotly cannot support more than two levels.  Grouping your data by the top 2 levels")
        levels = list(range(0,2,1))
    print(levels)

    # join the classification to the rating
    metadata_stack_field = f'metadata:{stack_field}'
    df = df[["context_id",metadata_stack_field]]
    df_pipeline = df_pipeline[["metadata:sourceConversationId"]+levels]
    df_pipeline.set_index("metadata:sourceConversationId",inplace=True)
    df = df.join(df_pipeline,on="context_id")



    # this is the pivot
    placeholder = '-'
    for level in levels:
        df[level].fillna(placeholder,inplace=True)
    print(df)
    pivot = pandas.pivot_table(df,values="context_id",fill_value=0,
                              index=levels,columns=[metadata_stack_field],aggfunc="count")
    # for level in levels:
    #     pivot.loc[pivot[level] == placeholder,level] = None
    pandas.set_option('display.max_rows',1000)
    print(pivot)

    # horizontal
    # y categories
    y = []
    for level in levels:
        y.append(pivot.index.get_level_values(level))

    # continous to discrete colour scale
    colors = plotly.colors.n_colors('rgb(255, 0, 0)', 'rgb(0, 255, 0)', len(pivot.columns.to_list()) , colortype = 'rgb')

    fig = plotly.graph_objects.Figure()

    # y cat
    for i,col in enumerate(pivot.columns.to_list()):
        fig.add_bar(
            y=y,
            x=pivot[col].to_list(),
            name=f'{stack_field} {col}',
            orientation='h',
            marker_color=colors[i]
        )

    fig.update_layout(barmode="stack")
    fig.update_yaxes(dtick=1)
    file_part = f'{playbook}_coverage_bar_{playbook_name.replace(" ","_")}_{pipeline}_{stack_field}.html'
    output_filename=os.path.join('data','html',file_part)
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')

def apply_clip(row: pandas.Series,
               clip: float,
               top_matching_intent_score: str,
               top_matching_intent_id: str,
               workspace: humanfirst.objects.HFWorkspace) -> pandas.Series:
    """Apply clip"""
    if float(row[top_matching_intent_score]) >= clip:
        row["fqn"] = workspace.get_fully_qualified_intent_name(row[top_matching_intent_id])
    else:
        row["fqn"] = 'other'
    return row

def get_col_name(starts_with: str, col_list: list, which_nlu: str = '') -> str:
    """Work out the name of the first NLU engine"""
    for col in col_list:
        assert isinstance(col,str)
        if col.startswith(starts_with):
            if which_nlu == '':
                print(col)
                return col
            else:
                if which_nlu in col:
                    print(col)
                    return col
    print(col_list)
    raise RuntimeError(f'No column starting: {starts_with} maybe you did not train the NLU engine')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
