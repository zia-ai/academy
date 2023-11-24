"""
export HF_PASSWORD=<password>

python get_workspace_example
-u <username>
-p $HF_PASSWORD
-n <namepspace>
-b <playbook-id>

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
import json

# third party imports
import click
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-o', '--outputdir', type=str, default='./data/', help='Where to output playbook')
def main(username: str, password: int, namespace: bool, playbook: str, outputdir: str):
    '''Example showing how to download the metadata for a playbook and the playbook itself
    using the humanfirst library.  Downloads both as JSON and then writes to file'''

    # check which authorization method using
    hf_api = humanfirst.apis.HFAPI(username, password)

    if not outputdir.endswith('/'):
        outputdir = outputdir + '/'

    # get the metadata info for a playbook
    playbook_info_dict = hf_api.get_playbook_info(namespace, playbook)
    playbook_name = playbook_info_dict["name"]
    playbook_info_out = f'{outputdir}{namespace}-{playbook_name}-info.json'
    with open(playbook_info_out, 'w', encoding="utf-8") as file_out:
        file_out.write(json.dumps(playbook_info_dict, indent=2))
        print(f'Wrote workspace info to: {playbook_info_out}')

    # get the playbook itself
    playbook_dict = hf_api.get_playbook(namespace, playbook)
    playbook_name = playbook_dict["name"]
    playbook_out = f'{outputdir}{namespace}-{playbook_name}.json'
    with open(playbook_out, 'w', encoding="utf-8") as file_out:
        file_out.write(json.dumps(playbook_dict, indent=2))
        print(f'Wrote workspace to:      {playbook_out}')


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
