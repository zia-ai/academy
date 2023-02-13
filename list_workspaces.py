#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python list_workspaces.py -u <username> -p <password>
#
# Lists all workspaces in an organisation
#
# *****************************************************************************

import humanfirst_apis
import click
import pandas

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password')
def main(username: str, password: int):
    headers = humanfirst_apis.process_auth("",username,password)
    playbooks_list = humanfirst_apis.list_playbooks(headers) # automatically does it for full organisation
    df = pandas.json_normalize(playbooks_list)
    print(df[["id","namespace","name"]])

if __name__ == '__main__':
    main()
