"""
python heatmap.py -f <download_from_hf> -m <lookup for model>

Want this to
Download the full unlabelled (and to know how much of that is there compared to labelled add a record)
Deal with FQN
Auto detect the levels
Be multi threaded for performance
Deal with the dates.


"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas
import plotly.express as px
import io
import os
import json

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
@click.option('-c', '--clip', type=float, required=False, default=0.35, help='Clip Point')
@click.option('-d', '--hierarchical_delimiter', type=str, required=False, default='-',
              help='Delimiter for hierarchical intents')
def main(
         username: str, password: str,
         namespace: str, playbook: str,
         hierarchical_delimiter: str,
         clip: float):
    """Main Function"""

    # do authorisation
    hf_api = humanfirst.apis.HFAPI(username=username,password=password)

    # check how many converation sets
    playbook_info = hf_api.get_playbook_info(playbook=playbook,namespace=namespace)
    print(json.dumps(playbook_info["conversationSets"]))
    playbook_name = playbook_info["name"]
    assert isinstance(playbook_name,str)

    # Check for trained NLU runids
    runs = hf_api.list_trained_nlu(namespace=namespace,playbook=playbook)
    df_runs = pandas.json_normalize(runs)
    print(df_runs)

    # check how many nlus - could look up get default here
    nlu_engines = hf_api.get_nlu_engines(namespace=namespace,playbook=playbook)
    df_nlu_engines = pandas.json_normalize(nlu_engines)
    print(df_nlu_engines)
    default_nlu_engine = None
    for nlu in nlu_engines:
        if nlu["isDefault"] == True:
            default_nlu_engine = nlu["id"]
            print(f'\nDefault NLU engine: {default_nlu_engine}')
            break
    if default_nlu_engine == None:
        raise RuntimeError("Can't find default nlu engine")

    # get the coverage_export
    coverage_export = hf_api.export_intents_coverage(namespace=namespace,
                                                     playbook=playbook,
                                                     confidence_threshold=clip,
                                                     coverage_type=1, # TOTAL
                                                     data_selection=1, # ALL
                                                     model_id=default_nlu_engine)
    df = pandas.read_csv(io.StringIO(coverage_export),delimiter=",")
    dump = './data/whatever.csv'
    df.to_csv(dump)
    print(dump)


    # get workspace to lookup names
    workspace_dict = hf_api.get_playbook(namespace=namespace,
                                         playbook=playbook,
                                         hierarchical_delimiter=hierarchical_delimiter)
    workspace = humanfirst.objects.HFWorkspace.from_json(workspace_dict,delimiter=hierarchical_delimiter)
    assert isinstance(workspace,humanfirst.objects.HFWorkspace)
    print("Downloaded workspace")

    analysis_field = "unique_utterance_score_histogram_thresholded_sum"
    # unique_utterance_count
    # unique_utterance_hier_count
    # unique_utterance_score_histogram_thresholded_sum
    # unique_utterance_hier_score_histogram_thresholded_sum

    # get FQN
    df["fqn_list"] = df["intent_id"].apply(workspace.get_fully_qualified_intent_name).str.split(hierarchical_delimiter)
    df = df.join(pandas.DataFrame(df["fqn_list"].values.tolist()))

    # get levels
    max_levels = df["fqn_list"].apply(len).max()
    levels = list(range(0,max_levels,1))
    print(levels)
    pandas.set_option('display.max_rows',1000)
    print(df[levels + [analysis_field]])

    # work out other
    other = {
        'intent_id':'other',
        'model_id': df.loc[0,"model_id"],
        'unique_utterance_count': 0,
        'unique_utterance_hier_count': 0,
        'unique_utterance_score_histogram_thresholded_sum': 0,
        'unique_utterance_hier_score_histogram_thresholded_sum':0
    }
    print(df['unique_utterance_count'].sum())
    print(df['unique_utterance_score_histogram_thresholded_sum'].sum())
    other[analysis_field] = df['unique_utterance_count'].sum() - df['unique_utterance_score_histogram_thresholded_sum'].sum()
    for level in levels:
        if level == 0:
            other[level] = 'other'
        else:
            other[level] = None
    df = pandas.concat([df,pandas.json_normalize(other)],axis=0).reset_index()


    # drop any rows with 0
    df = df[~(df[analysis_field]==0)]

    # Create the treemap plot using Plotly - using px.Constant("<br>") makes a prettier hover info for the root level
    fig = px.treemap(df, path=[px.Constant("<br>")] + levels, values='unique_utterance_count')

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

    # ouptut to ./data/based on workspace name
    playbook_name_replaced = playbook_name.replace(" ","_")
    output_filename=os.path.join('data','html',f'{playbook}_{playbook_name.replace(" ","_")}.html')
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
