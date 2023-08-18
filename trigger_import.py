#!/usr/bin/env python  # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python trigger_import.py
#
# trigger the import from a previously configured integration (using the gui)
# i.e if you want to nightly bring in your dialogflow workspace
#
# *****************************************************************************

# standard imports
import json

# 3rd party imports
import click

# custom imports
import humanfirst_apis

@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst./ playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
def main(username: str, password: int, namespace: bool, playbook: str, bearertoken: str):
    '''Main'''

    # do authorisation
    headers = humanfirst_apis.process_auth(bearertoken=bearertoken,username=username,password=password)

    # check playbook
    print(json.dumps(humanfirst_apis.get_playbook_info(headers, namespace, playbook),indent=2))

    # get integrations
    print(json.dumps(humanfirst_apis.get_integrations(headers,namespace),indent=2))
    #   {
    #     "namespace": "humanfirst-academy",
    #     "id": "intg-OHEMFYBQONBABMMWFTGUPIEJ",
    #     "name": "humanfirst-clients-df",
    #     "account": {
    #       "credentialId": "aaff6c7e-4f42-48e4-af05-51fcf61d9c36",
    #       "tiedCredentialLifecycle": true
    #     },
    #     "workspaceConnection": {},
    #     "type": "TYPE_DIALOGFLOW_CX",
    #     "dialogflowCx": {
    #       "defaultProjectId": "unified-skein-357013",
    #       "defaultLocation": "europe-west2"
    #     }
    #   },

    # get integration workspace for an integration
    # (i.e call HF to call Google using the integration to see what agents there are agent = integration_workspace)
    print(json.dumps(humanfirst_apis.get_integration_workspaces(headers,
                                                                namespace,
                                                                integration_id="intg-OHEMFYBQONBABMMWFTGUPIEJ"),
                     indent=2))
    # {
    #   "id": "6e13a7a7-3a69-4bec-87af-ce747eb780ee",
    #   "name": "Horribly Injured - Test Woolies - From CSV (20230420111722)"
    # },

    blah = humanfirst_apis.trigger_import_from_integration(
        headers,
        namespace,
        playbook,
        integration_id="intg-OHEMFYBQONBABMMWFTGUPIEJ",
        integration_workspace_id="6e13a7a7-3a69-4bec-87af-ce747eb780ee",
        project="unified-skein-357013",
        region="europe-west2",
        integration_language="en"
    )
    print(blah)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
