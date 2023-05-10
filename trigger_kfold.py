#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python trigger_kfold.py -n 5
#
# *****************************************************************************

# standard imports
import humanfirst_apis
import click

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-n', '--num_folds',type=int,required=True,help='Number of folds')
def main(username: str, password: int, namespace: bool, playbook: str, bearertoken: str, num_folds: int):
    
    # do authorisation
    headers = humanfirst_apis.process_auth(bearertoken=bearertoken,username=username,password=password)       
    
    # check playbook
    print(humanfirst_apis.get_playbook_info(headers, namespace, playbook))
    
    print("ATHORISED")
    print(humanfirst_apis.trigger_kfold_eval(headers, namespace, playbook, num_folds))

if __name__ == '__main__':
    main()