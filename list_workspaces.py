"""
python list_workspaces.py -u <username> -p <password>

Lists all workspaces in an organisation

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# 3rd party imports
import humanfirst
import click
import pandas

@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
def main(username: str, password: int, namespace: str):
    """Main Function"""

    hf_api = humanfirst.apis.HFAPI(username,password)
    playbooks_list = hf_api.list_playbooks(namespace) # automatically does it for full organisation
    df = pandas.json_normalize(playbooks_list)
    print(df[["id","namespace","name"]])

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
