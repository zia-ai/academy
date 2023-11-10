"""
python get_plan_and_usage.py -u <username> -p <password>

Retrieves the subscription plan and usage for the organisation a user belongs to

"""
# *********************************************************************************************************************

# standard imports
import json

# third party imports
import click
import pandas
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
def main(username: str, password: int, bearertoken: str):
    '''Main'''

    # authorise
    headers = humanfirst.apis.process_auth(username=username, password=password, bearertoken=bearertoken)

    # get plan info as a json
    print("Plan info")
    plan_dict = humanfirst.apis.get_plan(headers)
    print(json.dumps(plan_dict, indent=2))
    print("\n")

    # get usage info
    usage_dict = humanfirst.apis.get_usage(headers)

    # turn into dfs for df.to_csv or similar
    print("dataPoints.conversationSets")
    print(pandas.json_normalize(usage_dict["dataPoints"]["conversationSets"]))
    print("\n")
    print("dataPoints.workspaces")
    print(pandas.json_normalize(usage_dict["dataPoints"]["workspaces"]))
    print("\n")
    df = pandas.json_normalize(usage_dict)
    print("Usage summary")
    df.drop(columns=["dataPoints.conversationSets","dataPoints.workspaces"],inplace=True)
    print(df)
    print("\n")


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
