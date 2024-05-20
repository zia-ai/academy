"""
python coverage_query_heatmap.py
-f <download_from_hf>
-m <lookup for model>

Want this to
Download the full unlabelled (and to know how much of that is there compared to labelled add a record)
Deal with FQN
Auto detect the levels
Be multi threaded for performance
Deal with the dates.


"""
# ******************************************************************************************************************120

# standard imports
import json
import io
import os

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

    data = hf_api.export_query_conversation_inputs(
        namespace=namespace,
        playbook_id=playbook,
        pipeline_id=pipeline_id,
        pipeline_step_id=pipeline_step_id,
        download_format=2,
        dedup_by_hash=False,
        dedup_by_convo=False
    )
    df = pandas.read_csv(io.StringIO(data),delimiter=",")
    print(f'Downloaded csv for pipeline run: {df.shape}')

    # Get the correct column names
    if which_nlu == '':
        which_nlu = default_nlu_engine
    col_list = df.columns.to_list()
    top_matching_intent_id = get_col_name("top_matching_intent_id",col_list,which_nlu)
    top_matching_intent_score = get_col_name("top_matching_intent_score",col_list,which_nlu)

    # calc clips
    df = df.apply(apply_clip,
                  args=[
                            clip,
                            top_matching_intent_score,
                            top_matching_intent_id,
                            workspace
                        ],
                  axis=1)

    # expand dynamicaly that to a list and then columns per level
    df["fqn_list"] = df["fqn"].str.split(hierarchical_delimiter)
    df = df.join(pandas.DataFrame(df["fqn_list"].values.tolist()))

    # rename columns
    col_mapper = {
        top_matching_intent_score: "score"
    }
    df.rename(inplace=True,columns=col_mapper)

    # get levels
    max_levels = df["fqn_list"].apply(len).max()
    levels = list(range(0,max_levels,1))
    print(levels)

    # group it
    placeholder = 'none_placeholder'
    for level in levels:
        df[level].fillna(placeholder,inplace=True)
    gb = df[levels + ["id"]].groupby(levels,as_index=False).count()
    gb.rename(columns={"id":"id_count"},inplace=True)
    pandas.options.display.max_rows = 1000
    for level in levels:
        gb.loc[gb[level] == placeholder,level] = None
    print("Groupings are")
    print(gb)

    # Create the treemap plot using Plotly - using px.Constant("<br>") makes a prettier hover info for the root level
    fig = px.treemap(gb, path=[px.Constant("<br>")] + levels, values='id_count')

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
    output_filename=os.path.join('data','html',f'{playbook}_coverage_query_{playbook_name.replace(" ","_")}_{pipeline}.html')
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
