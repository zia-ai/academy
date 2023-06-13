#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python find_intent.py 
# -u <username> 
# -p <password>
# -n <namespace>
# -b <playbook>
# -i <intentid>
#
# Lists all workspaces in an organisation and then searches it for an id found
# example use is to find the intent mentioned in an error of the workspace
#
# *****************************************************************************

import humanfirst_apis
import click
import pandas

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-i','--intent_id', type=str, required=True, help='Intentid to search for')
def main(username: str, password: int, namespace: str, playbook: str,intent_id: str):
    headers = humanfirst_apis.process_auth("",username,password)
    intents = humanfirst_apis.get_intents(headers, namespace, playbook)
    df = pandas.json_normalize(intents)
    df.set_index("id",drop=True,inplace=True)
    df_with_parents = df[df["parentId"]==intent_id]
    print(df_with_parents[["name","parentId"]])
    try:
        print(df.loc[intent_id,["name","parendId"]])
    except KeyError:
        print("Did not find intent")
if __name__ == '__main__':
    main()
