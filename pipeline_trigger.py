"""
python pipeline_trigger.py 

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

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
@click.option('-l', '--pipeline', type=str, required=True, help='HumanFirst pipelineid')
def main(username: str, password: int, namespace: bool, playbook: str, pipeline: str):
    '''Trigger pipeline'''

    # check which authorization method using
    hf_api = humanfirst.apis.HFAPI(username, password)

    res = hf_api.trigger_playbook_pipeline(namespace=namespace,
                                     playbook_id=playbook,
                                     pipeline_id=pipeline)

    print(res)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
