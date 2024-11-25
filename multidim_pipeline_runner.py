"""
python multidim_pipeline_runner.py

Run all the pipelines

"""
# ******************************************************************************************************************120

# standard imports
import os
import time
import math

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst
import multidim_data_generation

@click.command()
@click.option('-n', '--namespace', type=str, required=True, help='Namespace with pipelines in')
@click.option('-p', '--pipelines', type=str, required=True, help='Pipelines to run nas a comma separated list of playbook_id1:pipeline_id1,playbook_id2:pipeline_id2')
@click.option('-s', '--sequential', is_flag=True, type=bool, required=False, default=False, help='Run sequential instead of parallel')
def main(namespace: str,
         pipelines: str, 
         sequential: bool) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    if not sequential:
        raise RuntimeError("Parallel mode not yet implemented")
    
    # Open connection
    hf_api = humanfirst.apis.HFAPI()
    
    # get arrays for playbooks and pipelines
    to_run = pipelines.split(",")
    for r in to_run:
        playbook_id,pipeline_id = r.split(":")
    
        pipeline_response = hf_api.trigger_playbook_pipeline(namespace=namespace,
                                         playbook_id=playbook_id,
                                         pipeline_id=pipeline_id)
        
        # returns triggerId and generationRunI
    
        print(pipeline_response)
    
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
