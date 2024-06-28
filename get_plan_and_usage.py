"""
python get_plan_and_usage.py -u <username> -p <password>

Retrieves the subscription plan and usage for the organisation a user belongs to

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
import json

# third party imports
import click
import pandas
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-o', '--output', type=str, default=None,
              help='Defaults to no output, if provided for instance ./data/someclient it will output there')
def main(username: str, password: int, output:str):
    '''Gets plan information, workspace and conversationset usage
    Optionall outputs to file'''

    # authorise
    hf_api = humanfirst.apis.HFAPI(username=username, password=password)

    # get plan info as a json
    print("Plan info")
    plan_dict = hf_api.get_plan()
    print(json.dumps(plan_dict, indent=2))
    print("\n")

    # get usage info
    usage_dict = hf_api.get_usage()

    # turn into dfs for df.to_csv or similar
    print("dataPoints.conversationSets")
    df_convo_summary = pandas.json_normalize(usage_dict["dataPoints"]["conversationSets"])
    if output:
        output_convo_name = f'{output}_convosets.csv'
        df_convo_summary.to_csv(output_convo_name, index=False,header=True)
        print(f'Wrote to: {output_convo_name}')
    else:
        print(df_workspace_summary)

    print("\ndataPoints.workspaces")
    df_workspace_summary = pandas.json_normalize(usage_dict["dataPoints"]["workspaces"])
    if output:
        output_workspaces_name = f'{output}_workspaces.csv'
        df_workspace_summary.to_csv(output_workspaces_name,index=False,header=True)
        print(f'Wrote to: {output_workspaces_name}')
    else:
        print(df_workspace_summary)
            

    df = pandas.json_normalize(usage_dict)
    print("\nUsage summary")
    df.drop(columns=["dataPoints.conversationSets","dataPoints.workspaces"],inplace=True)
    print(df)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
