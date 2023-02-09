#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python write_csv.py
#
# *****************************************************************************

# standard imports
import json
from os.path import join

# third part imports
import click

# custom imports
import humanfirst
import humanfirst_apis

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-o', '--output_dir', type=str, default="./", help='Output file path')

def main(username: str, password: int, namespace: bool, playbook: str, bearertoken: str, output_dir: str) -> None:
    """Main Function"""

    write_csv(username,password,namespace,playbook,bearertoken,output_dir)
    
def write_csv(username: str, password: int, namespace: bool, playbook: str, bearertoken: str, output_dir: str) -> None:
    """Writes the HF workspace to a CSV file and stores it in the output path"""

    headers = humanfirst_apis.process_auth(bearertoken, username, password)
    playbook_dict = humanfirst_apis.get_playbook(headers, namespace, playbook)
    labelled_workspace = humanfirst.HFWorkspace.from_json(playbook_dict)
    assert(isinstance(labelled_workspace,humanfirst.HFWorkspace))
    print("Got playbook and parsed it")
    output_path_json = join(output_dir,playbook_dict["name"]+".json")
    with open(output_path_json,mode="w",encoding="utf8") as f:
        json.dump(playbook_dict,f,indent=3)
    print(f"Workspace is stored at {output_path_json}")
    output_path_csv = join(output_dir,playbook_dict["name"]+".csv")
    labelled_workspace.write_csv(output_path_csv)
    print(f"CSV file is stored at {output_path_csv}")  

if __name__ == '__main__':
    main()