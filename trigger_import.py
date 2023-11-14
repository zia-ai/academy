"""
python trigger_import.py

trigger the import from a previously configured integration (using the gui)
i.e if you want to nightly bring in your dialogflow workspace

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# ******************************************************************************************************************120

# 3rd party imports
import click
import pandas
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
# Default behaviour is for the namespace and playbook to retrieve the information to trigger an import.
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--integration_type', type=str, required=False, default='TYPE_DIALOGFLOW_CX',
              help='Integration type to trigger default of TYPE_DIALOGFLOW_CX')
# If you have run in info mode you can then provide execute and the required information to trigger import
@click.option('-e', '--execute', is_flag=True, default=False, help='If not provided it will just print the information')
@click.option('-i', '--integration_id', type=str, default='', help='Integration to trigger')
@click.option('-w', '--integration_workspace_id', type=str, default='', help='Integration Workspace to import')
@click.option('-l', '--language', type=str, default='en', help='Language for integration workspace')
@click.option('-m', '--merge_type',type=str,default='merge',
              help='clear|merge|none clear all intents or merge all intents')
def main(username: str, password: int,
         namespace: bool, playbook: str, integration_type: str,
         execute: bool,
         integration_id: str,
         integration_workspace_id: str,
         language: str,
         merge_type: str
    ):
    '''Main'''

    # do authorisation
    hf_api = humanfirst.apis.HFAPI(username=username,password=password)

    # if we are not executing provide the information to let the user to pick
    # their integration_id and integration_workspace
    if not execute:

        # check playbook
        print("Playbook Information")
        playbook_info = hf_api.get_playbook_info(namespace, playbook)
        playbook_df = pandas.json_normalize(playbook_info)
        print(playbook_df[["namespace","id","name"]])
        print("\n")

        # get integrations
        print(f'Integrations for namespace {namespace} playbook {playbook}')
        integrations = hf_api.get_integrations(namespace)
        integrations_df = pandas.json_normalize(integrations)
        print(integrations)
        print(integrations_df[["id","name","type"]])
        print("\n")

        # for each integration get workspaces
        for i,integration in enumerate(integrations):
            if integration["type"] == integration_type:
                print(f'Retreiving integration {i} {integration["name"]} workspaces')
                try:
                    workspaces = hf_api.get_integration_workspaces(
                        namespace=namespace,
                        integration_id=integration["id"])
                    workspaces_df = pandas.json_normalize(workspaces)
                    print(workspaces_df[["id","name"]])
                    print("\n")
                except humanfirst.apis.HFAPIResponseValidationException as e:
                    print(f'Couldn\'t get workspaces for {integration["name"]}')
                    print(e)
                    print("\n")

    # If we are executing check the required information is present and execut
    else:
        # check have correct info
        assert integration_id != ''
        assert integration_workspace_id != ''
        integrations = hf_api.get_integrations(namespace=namespace)
        integration = None
        for integration_candidate in integrations:
            if integration_candidate["id"] == integration_id:
                integration = integration_candidate
                break
        assert isinstance(integration, dict)
        workspaces = hf_api. get_integration_workspaces(
            namespace=namespace,
            integration_id=integration["id"])
        for workspace_candidate in workspaces:
            if workspace_candidate["id"] == integration_workspace_id:
                workspace = workspace_candidate
                break
        assert isinstance(workspace, dict)

        if merge_type == 'merge':
            clear = False
            merge = True
        elif merge_type == 'clear':
            clear = True
            merge = False
        elif merge_type == 'none':
            clear = False
            merge = False
        else:
            print(f'Unknown merge type: {merge_type}')
            quit()

        import_return = hf_api.trigger_import_from_df_cx_integration(
            namespace,
            playbook,
            integration_id=integration["id"],
            integration_workspace_id=workspace["id"],
            project=integration["dialogflowCx"]["defaultProjectId"],
            region=integration["dialogflowCx"]["defaultLocation"],
            integration_language=language,
            clear_entities=clear,
            clear_intents=clear,
            clear_tags=clear,
            merge_entities=merge,
            merge_intents=merge,
            merge_tags=merge
        )
        print(import_return)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
