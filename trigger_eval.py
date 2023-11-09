"""
python trigger_kfold.py
-u <username>
-p <password>
-b <playbook>
OPTIONAL
-e <evalpresetname>

Trigger a kfold evaluation based on a preconfigured (using GUI) preset
for instance if you want to nightly run long running evals

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-e', '--evalpresetname', type=str, default='Default Auto Evaluation Preset',
              help='The name of the evaluation preset you want to run')
def main(username: str, password: int, namespace: bool, playbook: str, evalpresetname: str):
    '''Main'''

    # do authorisation
    hf_api = humanfirst.apis.HFAPI(username=username,password=password)

    # get the preset evaluation id you want.
    presets = hf_api.get_evaluation_presets(namespace,playbook)
    print("All presets found")
    print(json.dumps(presets,indent=2))

    # for purposes of this example cycle through and find the
    print("Warning: evaluation preset names may not be unique! Use the evaluation preset ID by preference")
    evaluation_preset_id = ''
    for preset in presets:
        if preset["name"] == evalpresetname:
            evaluation_preset_id = preset["id"]
            print(f'Found {evaluation_preset_id} "{preset["name"]}"')
            break
    if evaluation_preset_id == '':
        print("Failed to find preset id")
        quit()

    # trigger eval - no options - need
    print(json.dumps(hf_api.trigger_preset_evaluation(namespace,
                                                      playbook,
                                                      evaluation_preset_id),
                                                      indent=2))


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
