"""
python multidim_loader.py

Load abcd generated files, depends on multidim_data_generation

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
@click.option('-f', '--input_folder', type=str, required=True, help='Name of input files')
@click.option('-p', '--prefix', type=str, required=False, default="abcd", help='Prefix for input')
@click.option('-c', '--convoset_id', type=str, required=True, help='Convoset id') 
@click.option('-d', '--delete_mode', is_flag=True, required=False, default=False, help='DELETE instead of uploading') 
@click.option('-m', '--max_files', type=int, required=False, default=0, help='Limit number of files to this') 
@click.option('-u', '--max_loops', type=int, required=False, default=256, help='Maximum number of loops to attempt on trigger checks') 
@click.option('-t', '--no_trigger', is_flag=True, required=False, default=False, help='Whether to load all the files in a batch') 
# qa_abcd_multidim is the name convset-AZZ3KOSIZRF5LI43V4KDP3PR id
# ./zia --n multidim convset list
@click.option('-n', '--namespace', type=str, required=False, default="multidim", help='Namespace name')
def main(input_folder: str, prefix: str,
         convoset_id: str, namespace: str,
         delete_mode: bool,
         max_files: int,
         max_loops: int,
         no_trigger: bool) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    # read input directory
    load_files = read_input_directory(input_folder=input_folder,prefix=prefix)
    
    # Open connection
    hf_api = humanfirst.apis.HFAPI()
    
    # Check we are authenticated and print some playbooks - later on will be attached
    playbooks = pandas.json_normalize(hf_api.list_playbooks(namespace=namespace))
    print("PLATBOOKS")
    print(playbooks[["namespace","playbookName"]])

    # check we've got a convoset
    convoset = hf_api.get_conversation_set(namespace=namespace,conversation_set_id=convoset_id)
    print("CONVOSET")
    print(convoset)
    
    # if no file here it doesn't have a conversation source ID this is really annoying
    # get the conversation source underlying conversation set
    convosource_id = convoset["sources"][0]["conversationSourceId"]
        
    # List the existing files
    
    list_convset_files = hf_api.list_conversation_src_files(namespace=namespace,conversation_set_src_id=convosource_id)
    # handle case where get one file
    if not isinstance(list_convset_files,list):
        list_convset_files = list(list_convset_files)
    print(list_convset_files)                            
    df_files_at_start = pandas.json_normalize(list_convset_files)
    print(df_files_at_start)
    files_at_start = []
    print(df_files_at_start.shape)
    if df_files_at_start.shape[0] >= 1:
        files_at_start = df_files_at_start["name"].to_list()
    for i in range(len(files_at_start)):
        files_at_start[i] = os.path.join(input_folder,files_at_start[i])
    print(len(files_at_start))
    
    # Do some logic to look at how many files we want to load
    symetric_difference = set(load_files) ^ set(files_at_start)
    print(symetric_difference)
       
    # Check whether we need to load each file one by one.
    trigger_responses = []
    files_loaded = 0
    for i,f in enumerate(load_files):
        if delete_mode:
            multidim_data_generation.logit("Delete mode skip loading","")
            break
        if f in files_at_start:
            multidim_data_generation.logit("Skipping loading",f)
        else:

            if max_files > 0 and files_loaded == max_files:
                break

            if i >= (len(load_files) - 1):
                this_file_no_trigger = False
            elif i >= max_files - 1:
                this_file_no_trigger = False
            else:
                this_file_no_trigger = no_trigger
                
            multidim_data_generation.logit("this_file_no_trigger",no_trigger)

            start=time.perf_counter_ns()
            upload_response = hf_api.upload_json_file_to_conversation_source(namespace=namespace,
                                                           conversation_source_id=convosource_id,
                                                           upload_name=f,
                                                           fqfp=f,
                                                           no_trigger=this_file_no_trigger
                                                           )
            end=time.perf_counter_ns()
            duration_ns = end-start
            duration_s = math.ceil(duration_ns/1000000000)
            multidim_data_generation.logit(f"{duration_s:<20} Response is",upload_response)
            # 4-6 seconds no connected workspace
            
            # Test with 2 connected - no pipelines
            # ./zia -n multidim pb list -S 
            # id                                name        disabled datasets                         intents phrases utters convs
            # playbook-VZUT22TGVJATJIJ56LC5OFNP qa_resolved false    convset-AZZ3KOSIZRF5LI43V4KDP3PR 33      196     0      0
            # playbook-KPAW6OKL45DVDLB5RUXMVMB4 qa_topic    false    convset-AZZ3KOSIZRF5LI43V4KDP3PR 33      196     0      0
            # 5-7 seconds slightly slower, but kicks off a converation file import for every attached workspace they spin for quite a while
            
            # 3 minutes job length
            
            # get a bunch of trigger IDs and conversationSourceIds - how do I check trigger processes
            #{'filename': 'abcd-2022-05-28.json', 'triggerId': 'trig-DNXBIEHX5BENDP6GQDWCAR3Z', 'conversationSourceId': 'convsrc-VDXKI7INMBHKPMYNNNSFVPND'}
            
            total_wait = 0
            if this_file_no_trigger == False:
                if "triggerId" in upload_response.keys():
                    total_wait = loop_trigger_check_until_done(hf_api=hf_api,
                                                max_loops=max_loops, 
                                                namespace=namespace, 
                                                trigger_id=upload_response["triggerId"],
                                                debug=True,
                                                log_note="upload")
                else:
                    multidim_data_generation.logit("No triggerid in upload response",upload_response)
            
                if total_wait == 0:
                    raise RuntimeError(f"Did not get TRIGGER_STATUS_COMPLETED with max_loops: {max_loops}")
                
            
            multidim_data_generation.logit(f"File: {f} total_time:",total_wait)
            
            files_loaded = files_loaded + 1

    if delete_mode:
        multidim_data_generation.logit("files_at_start",files_at_start)      
        multidim_data_generation.logit("Reverse Order for deletes",0)
        files_at_start.sort(reverse=True)
            
    # delete
    files_deleted = 0
    for i,f in enumerate(files_at_start):
        if not delete_mode:
            multidim_data_generation.logit("Skipping Deletes","")
            break
          
        if max_files > 0 and files_deleted == max_files:
            break
        
        if i >= (len(files_at_start) - 1):
            this_file_no_trigger = False
        elif i >= max_files - 1:
            this_file_no_trigger = False
        else:
            this_file_no_trigger = no_trigger
            
        multidim_data_generation.logit("this_file_no_trigger",this_file_no_trigger)
        
        start=time.perf_counter_ns()
        delete_response = hf_api.delete_conversation_file(namespace=namespace,
                                        conversation_set_src_id=convosource_id,
                                        no_trigger=this_file_no_trigger,
                                        file_name=os.path.split(f)[-1])
        end=time.perf_counter_ns()
        duration_ns = end-start
        duration_s = math.ceil(duration_ns/1000000000)
        multidim_data_generation.logit(f"{duration_s:<20} Delete {f} is ",delete_response)
        
        # 3-4 seconds with nothing attached - 1 spike to 11
        # 5 seconds
        
        if this_file_no_trigger == False:
            if "triggerId" in delete_response.keys():
                total_wait = 0
                total_wait = loop_trigger_check_until_done(hf_api=hf_api,
                                                max_loops=max_loops, 
                                                namespace=namespace, 
                                                trigger_id=delete_response["triggerId"],
                                                debug=True,
                                                log_note="delete")
                if total_wait == 0:
                    raise RuntimeError(f"Did not get TRIGGER_STATUS_COMPLETED with max_loops: {max_loops}")
                
                multidim_data_generation.logit(f"File delete: {f} total_time:",total_wait)
            else:
                multidim_data_generation.logit("triggerid not in delete_response",delete_response)
        
        files_deleted = files_deleted + 1
        

def loop_trigger_check_until_done(hf_api: humanfirst.apis.HFAPI, max_loops: int,
                                  namespace: str, trigger_id: str, increment: int = 0, debug: bool = False,
                                  log_note: str = "") -> int:
    """Loops round and waits for TRIGGER_STATUS_COMPLETE
    The returns the total time
    Return 0 if error """
    loops = 0
    wait = 1
    total_wait = 0
    done = False
    while done == False:
        trigger_response = hf_api.describe_trigger(namespace=namespace,trigger_id=trigger_id,timeout=120)
        summary = {
            "triggerId": trigger_response["triggerState"]["trigger"]["triggerId"],
            "message": trigger_response["triggerState"]["trigger"]["message"],
            "status": trigger_response["triggerState"]["status"]
        }
        if "progress" in trigger_response.keys():
            summary["total"] = trigger_response["triggerState"]["progress"]["total"],
            summary["completed"] = trigger_response["triggerState"]["progress"]["completed"],
            summary["percentageComplete"] = trigger_response["triggerState"]["progress"]["percentageComplete"]
 
        if debug:
            multidim_data_generation.logit(f"{loops:>5} total_wait {log_note}: {total_wait}",summary["status"])
        total_wait = total_wait + wait
        loops = loops + 1
        wait = wait + increment
        if summary["status"] == "TRIGGER_STATUS_COMPLETED":
            done = True
            break
        if loops > max_loops:
            break
        time.sleep(wait)
    if done:
        return total_wait
    else:
        return 0

def read_input_directory(input_folder: str, prefix: str):
    
    # get regex
    re_output_format = multidim_data_generation.get_file_format_regex(prefix)
    
    # Read inputs
    assert os.path.isdir(input_folder)
    list_files = os.listdir(input_folder)
    load_files = []
    for f in list_files:
        assert isinstance(f,str)
        # skip it if in output format
        if f.endswith(".json"):
            if re_output_format.match(f):
                load_files.append(os.path.join(input_folder,f))
            else:
                multidim_data_generation.logit("Skipping original abcd",f)
    load_files.sort()
    multidim_data_generation.logit("Read number load_files is",len(load_files))
    
    return load_files
       
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
