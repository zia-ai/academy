"""
python list_workspaces.py -u <username> -p <password>

Lists all workspaces in an organisation

"""
# *********************************************************************************************************************

# 3rd party imports
import humanfirst
import click
import pandas

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password')
def main(username: str, password: int):
    """Main Function"""

    headers = humanfirst.apis.process_auth("",username,password)
    playbooks_list = humanfirst.apis.list_playbooks(headers) # automatically does it for full organisation
    df = pandas.json_normalize(playbooks_list)
    print(df[["id","namespace","name"]])

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
