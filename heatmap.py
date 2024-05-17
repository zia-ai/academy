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
import json
import io

# custom imports
import humanfirst

@click.command()
# Mandatory
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
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
@click.option('-w', '--which_nlu', type=str, required=False, default='',
              help='NLU name like nlu-57QM7EN3UFEZPGH7PI3FCJGV(HumanFirst NLU) if blank will just take the first')
@click.option('-s', '--start_isodate', type=str,
              default=None, help='Date range to extract conversations from')
@click.option('-e', '--end_isodate', type=str,
              default=None, help='Date range to extract conversations to')
def main(filename: str, clip: float,
         username: str, password: str, namespace: str, playbook: str,
         hierarchical_delimiter: str,
         which_nlu: str,
         start_isodate: str, end_isodate: str) -> None:
    """Main Function"""

    # do authorisation
    hf_api = humanfirst.apis.HFAPI(username=username,password=password)

    # check how many converation sets
    playbook_info = hf_api.get_playbook_info(playbook=playbook,namespace=namespace)
    print(json.dumps(playbook_info["conversationSets"]))

    # Check for trained NLU runids
    runs = hf_api.list_trained_nlu(namespace=namespace,playbook=playbook)
    df_runs = pandas.json_normalize(runs)
    print(df_runs)

    # check how many nlus - could look up get default here
    nlu_engines = hf_api.get_nlu_engines(namespace=namespace,playbook=playbook)
    for nlu in nlu_engines:
        print(hf_api.get_nlu_engine(namespace=namespace,playbook=playbook,nlu_id=nlu["id"]))

    # try and get coverage - here we are letting it default NLU id and run-id - it doesn't report which run_id used
    coverage_request = hf_api.get_intents_coverage_request(namespace=namespace,playbook=playbook,data_selection=1)
    df_coverage_request = pandas.json_normalize(coverage_request["intents"])
    print(df_coverage_request)

    # get the actual coverage_export
    coverage_export = hf_api.export_intents_coverage(namespace=namespace,playbook=playbook)
    df_coverage = pandas.read_csv(io.StringIO(coverage_export),delimiter=",")
    print(df_coverage)

    quit()





    # query conversation set
    response_json = hf_api.query_conversation_set(
        namespace,
        playbook,
        start_isodate=start_isodate,
        end_isodate=end_isodate,
        convsetsource=conversation_set_id
    )
    print(response_json)
    quit()

    # get workspace to lookup names
    workspace_dict = hf_api.get_playbook(namespace=namespace,
                                         playbook=playbook,
                                         hierarchical_delimiter=hierarchical_delimiter)
    workspace = humanfirst.objects.HFWorkspace.from_json(workspace_dict,delimiter=hierarchical_delimiter)
    assert isinstance(workspace,humanfirst.objects.HFWorkspace)
    print("Downloaded workspace")

    # read file
    df = pandas.read_csv(filename,delimiter=",",encoding="utf8")

    # assumes the first of NLU engines
    col_list = df.columns.to_list()
    top_matching_intent_id = get_col_name("top_matching_intent_id",col_list,which_nlu)
    top_matching_intent_name = get_col_name("top_matching_intent_name",col_list,which_nlu)
    top_matching_intent_score = get_col_name("top_matching_intent_score",col_list,which_nlu)

    # calc clips
    df = df.apply(apply_clip,args=[clip,
                                   top_matching_intent_name,
                                   top_matching_intent_score,
                                   top_matching_intent_id,
                                   workspace,
                                   hierarchical_delimiter,
                                   ],axis=1)

    # group it
    gb = df[["parent","child","id"]].groupby(["parent","child"]).count().reset_index()
    gb.rename(columns={"id":"id_count"},inplace=True)
    print(gb)

    # Create the treemap plot using Plotly - using px.Constant("<br>") makes a prettier hover info for the root level
    fig = px.treemap(gb, path=[px.Constant("<br>"), 'parent', 'child'], values='id_count')

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
    output_filename=filename.replace(".csv","_output_treemap.html")
    assert filename != output_filename
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')

def apply_clip(row: pandas.Series, clip: float,
               top_matching_intent_name: str,
               top_matching_intent_score: str,
               top_matching_intent_id: str,
               workspace: humanfirst.objects.HFWorkspace,
               delimiter: str) -> pandas.Series:
    """Apply clip"""
    if float(row[top_matching_intent_score]) >= clip:
        row["fqn"] = workspace.get_fully_qualified_intent_name(row[top_matching_intent_id])
        #TODO: make any levelled
        row["parent"] = str(row["fqn"]).split(delimiter,maxsplit=1)[0]
        row["child"] = row[top_matching_intent_name]
        row["score"] = row[top_matching_intent_score]
    else:
        row["fqn"] = f'other{delimiter}other'
        row["parent"] = "other"
        row["child"] = "other"
        row["score"] = row[top_matching_intent_score]
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
                if col.endswith(which_nlu):
                    print(col)
                    return col
    raise RuntimeError(f'No column starting: {starts_with} maybe you did not train the NLU engine')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
