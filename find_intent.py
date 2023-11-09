"""
python find_intent.py
-u <username>
-p <password>
-n <namespace>
-b <playbook>
-i <intentid>

Lists all workspaces in an organisation and then searches it for an id found
example use is to find the intent mentioned in an error of the workspace

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# ******************************************************************************************************************120

# 3rd party imports
import click
import pandas
import humanfirst

@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-i','--intent_id', type=str, required=True, help='Intentid to search for')
def main(username: str, password: int, namespace: str, playbook: str,intent_id: str):
    """Main Function"""

    hf_api = humanfirst.apis.HFAPI(username,password)
    intents = hf_api.get_intents(namespace, playbook)
    df = pandas.json_normalize(intents)
    df.set_index("id",drop=True,inplace=True)
    df_with_parents = df[df["parentId"]==intent_id]
    print(df_with_parents[["name","parentId"]])
    try:
        print(df.loc[intent_id,["name","parentId"]])
    except KeyError:
        print("Did not find intent")
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
