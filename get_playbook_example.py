#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
#
# export HF_PASSWORD=<password>
#
# python get_workspace_example
# -u <username>
# -p $HF_PASSWORD
# -n <namepspace>
# -b <playbook-id>
#
#
# *****************************************************************************

# standard imports
import requests
import json
import base64

# third party imports
import click

# custom imports
import humanfirst_apis


@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with if not providing username/password')
@click.option('-o', '--outputdir', type=str, default='./data/', help='Where to output playbook')
def main(username: str, password: int, namespace: bool, playbook: str, bearertoken: str, outputdir: str):
    '''Example showing how to download the metadata for a playbook and the playbook itself
    using the humanfirst_apis library.  Downloads both as JSON and then writes to file'''

    # check which authorization method using
    headers = humanfirst_apis.process_auth(bearertoken, username, password)

    if not outputdir.endswith('/'):
        outputdir = outputdir + '/'

    # get the metadata info for a playbook
    playbook_info_dict = humanfirst_apis.get_playbook_info(headers, namespace, playbook)
    playbook_name = playbook_info_dict["name"]
    playbook_info_out = f'{outputdir}{namespace}-{playbook_name}-info.json'
    with open(playbook_info_out, 'w') as file_out:
        file_out.write(json.dumps(playbook_info_dict, indent=2))
        print(f'Wrote workspace info to: {playbook_info_out}')

    # get the playbook itself
    playbook_dict = humanfirst_apis.get_playbook(headers, namespace, playbook)
    playbook_name = playbook_dict["name"]
    playbook_out = f'{outputdir}{namespace}-{playbook_name}.json'
    with open(playbook_out, 'w') as file_out:
        file_out.write(json.dumps(playbook_dict, indent=2))
        print(f'Wrote workspace to:      {playbook_out}')


if __name__ == '__main__':
    main()
