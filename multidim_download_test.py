"""
python multidim_upload_test.py

Use a trigger ID for a pipeline on a namespace to lookup the status of that trigger
This one is for checking the triggers on File Uploads rather than pipeline downloads.

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import json
import pandas

# custom imports
import humanfirst

@click.command()
@click.option('-n', '--namespace', type=str, required=True, help='Namespace name')
@click.option('-t', '--trigger_id', type=str, required=True, help='Trigger id')
@click.option('-d', '--dump', is_flag=True, required=False, default=False, help='Dump the full json of the trigger id')
def main(namespace: str,
         trigger_id: str,
         dump: bool
         ) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    trigger_check = check_file_upload_trigger_id(namespace=namespace,
            trigger_id=trigger_id, dump=dump)
    print(json.dumps(trigger_check,indent=2))
    
def check_file_upload_trigger_id(namespace: str, 
            trigger_id: str, dump: bool = False) -> dict:
    """For a given namespace and trigger id checks that 
    It is a file upload trigger
    That trigger completed
    The key information for that trigger"""
    
    # Open connection
    hf_api = humanfirst.apis.HFAPI()
    
    # Check the status of the trigger on the id 
    trigger_response = hf_api.describe_trigger(namespace=namespace,
                            trigger_id=trigger_id)
    
    if dump:
        print(json.dumps(trigger_response,indent=2))
    
    # Check status
    trigger_status = trigger_response["triggerState"]["status"]
    if not trigger_status == "TRIGGER_STATUS_COMPLETED":
        print(json.dumps(trigger_response,indent=2))
        raise RuntimeError("Trigger did not complete")
    trigger_created_at = trigger_response["triggerState"]["trigger"]["createdAt"]
    trigger_completed_at = trigger_response["triggerState"]["trigger"]["completedAt"]

        
    # Check if file upload
    if not "conversationsFileImport" in trigger_response["triggerState"]["trigger"]["metadata"].keys():
        print(json.dumps(trigger_response,indent=2))
        raise RuntimeError("Not a conversationsFileImport")
        
    # get the data 
    conversations_file_import = trigger_response["triggerState"]["trigger"]["metadata"]["conversationsFileImport"]
    conversation_source_id = conversations_file_import["conversationSourceId"]
    filename = conversations_file_import["filename"]
    
    # check the convoset exists and the export is there
    conversation_source_info = hf_api.get_conversation_source(namespace=namespace, conversation_source_id=conversation_source_id)
    assert "exportId" in conversation_source_info.keys()
    assert "exportUrlPath" in conversation_source_info.keys()
    
    # check the convset config
    # TODO: need convoset_id have convosource_id
    # conversation_set_config = hf_api.get_conversation_set_configuration(namespace=namespace,convoset_id=conversation)
    
    # Check that the file is in the conversation set
    convoset_files = hf_api.list_conversation_src_files(namespace=namespace, conversation_set_src_id=conversation_source_id)
    assert len(convoset_files) > 0
    df_files= pandas.json_normalize(convoset_files)
    assert filename in df_files["name"].to_list()
    df_files = df_files.set_index("name")
    file_upload_time=df_files.loc[filename,"uploadTime"]
    file_upload_format=df_files.loc[filename,"format"]
    print(df_files)
    
    # see what jobs it ran
    jobs = trigger_response["triggerState"]["jobs"]
    assert len(jobs) > 0
    number_total_jobs = len(jobs)
    df_jobs = pandas.json_normalize(jobs)
    number_jobs_job_done = len(df_jobs[df_jobs["triggeredRun.status"]=="JOB_DONE"])
    
    # earliest and latest jobs
    first_job_starttime = df_jobs["triggeredRun.startTime"].min()
    final_job_endtime = df_jobs["triggeredRun.endTime"].max()     
        
    response_dict = {
        'namespace':             namespace,
        'conversation_source_id':conversation_source_id,
        'filename':              filename,
        'file_upload_time':      file_upload_time,
        'file_upload_format':    file_upload_format,
        'trigger_id':            trigger_id,
        'trigger_created_at':    trigger_created_at,
        'trigger_completed_at':  trigger_completed_at,
        'trigger_status':        trigger_status,
        'number_total_jobs':     number_total_jobs,
        'number_jobs_job_done':  number_jobs_job_done,
        'first_job_starttime':   first_job_starttime,
        'final_job_endtime':     final_job_endtime
    }
                    
    return response_dict

        
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
