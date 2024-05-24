"""
python check_counts.py -f <filename>

Does a groupby check count of client and expert examples from a HFWorkspace format.
I.e download your unlabelled - check how many results you should have.

"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst

@click.command()
# Mandatory
@click.option('-f', '--filename', type=str, required=True,
              help='Path to downloaded humanfirst conversationset of examples')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
# Set in env variables normally
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
def main(filename: str, #TODO: remove me when able to download convoset
         username: str, password: str,
         namespace: str, playbook: str,
         ) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # do authorisation
    hf_api = humanfirst.apis.HFAPI(username=username,password=password)

    # get the convoset for the playbook
    convoset = hf_api.get_one_convoset_id_for_playbook(namespace=namespace,playbook=playbook)
    print(f'name:              {convoset["name"]}')
    print(f'id:                {convoset["id"]}')
    print(f'conversationCount: {convoset["state"]["statistics"]["conversationCount"]}')
    print(f'inputCount:        {convoset["state"]["statistics"]["inputCount"]}')

    # TODO: at the moment can't download the convoset, so still have to do this from a file
    file_in = open(filename,encoding='utf8')
    df = pandas.json_normalize(data=json.load(file_in)["examples"])
    file_in.close()

    # perform groupby
    gb = df[["context.role","id"]].groupby("context.role").count()
    print(gb)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter