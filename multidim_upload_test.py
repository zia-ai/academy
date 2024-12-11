"""
python multidim_download_pipeline_test.py

Use a trigger ID for a pipeline on a namespace to lookup the status of that trigger
Optionally if 

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import json

# custom imports
import humanfirst

@click.command()
@click.option('-n', '--namespace', type=str, required=True, help='Namespace name')
@click.option('-t', '--trigger_id', type=str, required=True, help='Trigger id')
@click.option('-f', '--filter_key', type=str, required=False, default='', help='Filter Column')
@click.option('-v', '--filter_val', type=str, required=False, default='', help='Filter value')
def main(namespace: str,
         trigger_id: str,
         filter_key: str,
         filter_val: str
         ) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    trigger_check = check_pipeline_trigger_id(namespace=namespace,
            trigger_id=trigger_id,
            filter_key=filter_key,
            filter_val=filter_val)
    print(json.dumps(trigger_check,indent=2))
    
def check_pipeline_trigger_id(namespace: str, 
            trigger_id: str,
            filter_key: str = "",
            filter_val: str = "",) -> dict:
    """For a given namespace and trigger id checks that 
    It is a pipeline transform trigger
    That trigger completed
    The key information for that trigger
    Optionally based on a passed filter_key and filter_value
    tries to download a filtered section from the output of that trigger
    If it does so parses the output to check valid JSON"""
    
    # Open connection
    hf_api = humanfirst.apis.HFAPI()
    
    # Check the status of the trigger on the id 
    trigger_response = hf_api.describe_trigger(namespace=namespace,
                            trigger_id=trigger_id)
    
    # Check status
    trigger_status = trigger_response["triggerState"]["status"]
    if not trigger_status == "TRIGGER_STATUS_COMPLETED":
        print(json.dumps(trigger_response,indent=2))
        raise RuntimeError("Trigger did not complete")
    trigger_created_at = trigger_response["triggerState"]["trigger"]["createdAt"]
    trigger_completed_at = trigger_response["triggerState"]["trigger"]["completedAt"]

        
    # Check if pipeline transform
    if not "pipelineTransform" in trigger_response["triggerState"]["trigger"]["metadata"].keys():
        raise RuntimeError("Not a pipelineTransform")
        
    # get the data 
    playbook_transform = trigger_response["triggerState"]["trigger"]["metadata"]["pipelineTransform"]
    playbook_id = playbook_transform["playbookId"]
    pipeline_id = playbook_transform["pipelineId"]
    pipeline_step_id = playbook_transform["pipelineStepId"]
    generation_run_id = playbook_transform["generationRunId"]

    
    # check the playbook name
    playbook_info = hf_api.get_playbook_info(namespace=namespace,playbook=playbook_id)
    playbook_name = playbook_info["name"]
    
    # check the pipeline name
    playbook_pipelines = hf_api.list_playbook_pipelines(namespace=namespace,playbook_id=playbook_id)
    
    # find the one interested in
    found = False
    for p in playbook_pipelines:
        if p["id"] == pipeline_id:
            found=True
            pipeline_name = p["name"]
    if not found:
        raise RuntimeError("Couldn't find pipeline_id for that playbook")
    
    
    response_dict = {
        'playbook_name':        playbook_name,
        'playbook_id':          playbook_id,
        'pipeline_name':        pipeline_name,
        'pipeline_id':          pipeline_id,
        'pipeline_step_id':     pipeline_step_id,
        'generation_run_id':    generation_run_id,
        'trigger_id':           trigger_id,
        'trigger_created_at':   trigger_created_at,
        'trigger_completed_at': trigger_completed_at,
        'trigger_status':       trigger_status
    }
    
    # assemble filter 
    metadata_predicate = [
        {
            "key": filter_key,
            "operator": "EQUALS",
            "value": filter_val
        }
    ]
    
    if filter_key == "" or filter_val == "":
        response_dict["json_parse"] = "skipped"
        response_dict["records_returned"] = 0
        
    else:
        # try download
        download_from_trigger = hf_api.export_query_conversation_inputs(namespace=namespace,
                                                playbook_id=playbook_id,
                                                pipeline_id=pipeline_id,
                                                pipeline_step_id=pipeline_step_id,
                                                metadata_predicate=metadata_predicate)
        
        # check valid json
        json.dumps(download_from_trigger,indent=2)
        
        # check had some results
        if not "examples" in download_from_trigger:
            response_dict["records_returned"] = 0
        else:
            response_dict["records_returned"] = len(download_from_trigger["examples"])
                
    return response_dict

        
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
