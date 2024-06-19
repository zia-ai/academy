"""
export HF_PASSWORD=<password>

python download_generated_data.py

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
import json
import os

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
@click.option('-l', '--pipeline', type=str, default="", help='HumanFirst pipelineid')
@click.option('-s', '--pipeline_step_id', type=str, default="", help='HumanFirst pipeline step id')
@click.option('-t', '--prompt_id', type=str, default="", help='HumanFirst prompt id')
@click.option('-g', '--generation_run_id', type=str, default="", help='HumanFirst generation run id')
@click.option('-o', '--output_dir', type=str, default="./data/", help='Output directory')
def main(username: str,
         password: int,
         namespace: bool,
         playbook: str,
         pipeline: str,
         pipeline_step_id: str,
         prompt_id: str,
         generation_run_id: str,
         output_dir: str):
    '''Download generated data'''

    # check which authorization method using
    hf_api = humanfirst.apis.HFAPI(username, password)
    print('Connected')

    res = hf_api.export_query_conversation_inputs(
        namespace=namespace,
        playbook_id=playbook,
        pipeline_id=pipeline,
        pipeline_step_id=pipeline_step_id,
        prompt_id=prompt_id,
        generation_run_id=generation_run_id,
        dedup_by_hash=False,
        dedup_by_convo=False)
    print('Download complete')

    output_filename=os.path.join(output_dir,f"{namespace}_{playbook}.json")
    with open(output_filename,mode="w",encoding="utf8") as f:
<<<<<<< HEAD
        json.dump(res.json(),f,indent=2)
=======
        json.dump(res,f,indent=2)
>>>>>>> master
        print(f'Wrote to: {output_filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
