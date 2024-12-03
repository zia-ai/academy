"""
python multidim_parallel.py

Run all the pipelines for a playbook

"""
# ******************************************************************************************************************120

# standard imports
import os
import time
import math
import json

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst
import multidim_data_generation

@click.command()
@click.option('-n', '--namespace', type=str, required=True, help='Namespace with pipelines in')
@click.option('-p', '--filter_prefix', type=str, required=False, default="", help='Filter playbooks for things starting with this')
@click.option('-m', '--max_loops', type=int, required=False, default=1500, help='Maximum number of loops to attempt on trigger checks') 
@click.option('-d', '--date_filter_day', type=str, required=True, help='Which YYYY-MM-DD to filter for and which file to use')
@click.option('-c', '--convoset_id', type=str, required=True, help='Convoset id to upload to') 
@click.option('-s', '--skip_load', is_flag=True, type=bool, required=False, default=False, help='Skip load file') 
@click.option('-t', '--skip_pipelines', is_flag=True, type=bool, required=False, default=False, help='Skip pipelines') 
def main(namespace: str,
         filter_prefix: str,
         max_loops: int,
         date_filter_day: str,
         convoset_id: str,
         skip_load: bool,
         skip_pipelines: bool) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    
    # max playbookx
    max_playbooks = max_loops
    print(f'Max playbooks: {max_playbooks}')
        
    # Open connection
    hf_api = humanfirst.apis.HFAPI()
    
    # check we've got a convoset and convosource
    convoset = hf_api.get_conversation_set(namespace=namespace,conversation_set_id=convoset_id)
    convosource_id = convoset["sources"][0]["conversationSourceId"]
    
    # workout filename
    filename = os.path.join('./data/abcd/multidim',f'abcd-{date_filter_day}.json')
    
    # upload one file - as just one trigger doesn't matter but setting to Flase for clarity
    if not skip_load:
        upload_response = hf_api.upload_json_file_to_conversation_source(namespace=namespace,
                                                    conversation_source_id=convosource_id,
                                                    upload_name=filename,
                                                    fqfp=filename,
                                                    no_trigger=False
                                                    )
        print(f'/n/nUploaded file')
        print(upload_response)
    else:
        print(f'/n/nSkipped Uploading file')
    
    # Normally we would loop here but we're just pressing on to run everything in parallel
    
    # list all playbooks
    playbooks = hf_api.list_playbooks(namespace=namespace)
    df_playbooks = pandas.json_normalize(playbooks)
    print(df_playbooks)
    
    # filter
    if filter_prefix != "":
        print(f"\n\nFiltering for things starting {filter_prefix}")
        df_playbooks = df_playbooks[df_playbooks["playbookName"].str.startswith(filter_prefix)]
        df_playbooks[["namespace","playbookName","etcdId"]]
    print(df_playbooks[["namespace","playbookName","etcdId"]])
    
    # go through all playbooks twice
    print('\n\nPipeline running')
    playbook_ids = list(df_playbooks["etcdId"].unique())
    playbooks_run = 0
    for playbook_id in playbook_ids:
        if skip_pipelines:
            print('Skipping pipelines')
            break
        if max_playbooks > 0 and playbooks_run == max_playbooks:
            print(f'Max Pipelines reached: {max_playbooks}')
            break
        print(playbook_id)
        run_all_piplines_no_wait(hf_api=hf_api,
                         namespace=namespace,
                         playbook_id=playbook_id,
                         max_loops=max_loops
                         )
        
        playbooks_run = playbooks_run + 1
        
    # second loop to download everything
    print('\n\Downloads running')
    downloads_made = 0
    for playbook_id in playbook_ids:
        if max_playbooks > 0 and downloads_made == max_playbooks:
            print(f'Max Dowanloads reached: {max_playbooks}')
            break

        download_everything(hf_api=hf_api,
                    namespace=namespace,
                    playbook_id=playbook_id,
                    max_loops=max_loops,
                    date_filter_day=date_filter_day
                    )
        downloads_made = downloads_made + 1

def run_all_piplines_no_wait(hf_api: humanfirst.apis.HFAPI, namespace:str, playbook_id: str, max_loops: int) -> bool:
    """Run all the pipelines without waiting unless max_loops says to do one"""
    pipelines = hf_api.list_playbook_pipelines(namespace=namespace,
                                   playbook_id=playbook_id)   
    pipelines_run = 0
    for p in pipelines:
        
        if pipelines_run > max_loops:
            break
              
        multidim_data_generation.logit("Running",p["name"])
        trigger_pipeline = hf_api.trigger_playbook_pipeline(namespace=namespace,
                                                            playbook_id=playbook_id,
                                                            pipeline_id=p["id"])
        if not "triggerId" in trigger_pipeline.keys():
            raise RuntimeError(f'This pipeline did not provide a triggerId')
        multidim_data_generation.logit(f'Pipline: {p["name"]}',trigger_pipeline)
        
        pipelines_run = pipelines_run + 1
        
def download_everything(hf_api: humanfirst.apis.HFAPI, namespace:str, playbook_id: str, 
                     max_loops: int, date_filter_day: str) -> bool:
    """Download everything for the file unless max loops says to do one."""
    pipelines = hf_api.list_playbook_pipelines(namespace=namespace,
                                   playbook_id=playbook_id)   
    pipelines_run = 0
    for p in pipelines:
        
        if pipelines_run > max_loops:
            break
              
        print(f'Download {p["name"]}')
        
        # metadata_predicate
        # [
        #     {
        #         "key": "INSERT_KEY_NAME",
        #         "operator": "EQUALS|NOT_EQUALS|CONTAINS|NOT_CONTAINS|KEY_EXISTS|KEY_NOT_EXISTS|KEY_MATCHES|ANY",
        #         "value": "VALUE|''"
        #     },
        #     #other filters..
        # ]
        metadata_predicate = [
            {
                "key": "date_of_convo",
                "operator": "EQUALS",
                "value": f"{date_filter_day}"
            },
            #other filters..
        ]
        
        start=time.perf_counter_ns()
        # This now has await next index as default
        some_data = hf_api.export_query_conversation_inputs(namespace=namespace,
                                                playbook_id=playbook_id,
                                                pipeline_id=p["id"],
                                                pipeline_step_id=p["steps"][0]["id"],
                                                metadata_predicate=metadata_predicate,
                                                source_kind=2, # SOURCE_KIND_GENERATED                                             
                                                timeout=480# just take the first step)
        )
        print(playbook_id)
        print(p["id"])
        print(p["steps"][0]["id"])
        print(metadata_predicate)
        end=time.perf_counter_ns()
        duration_ns = end-start
        duration_s = math.ceil(duration_ns/1000000000)
        multidim_data_generation.logit(f"{duration_s:<20} Some_data download time is ",f'{duration_s}s')
        
        # dump output to file
        filename_out = f'./data/{playbook_id}-{p["id"]}-{p["steps"][0]["id"]}.json'
        with open(filename_out,mode="w",encoding="utf8") as file_out:
            json.dump(some_data,file_out,indent=2)
            print(f'Wrote to {filename_out}')
        
        # do some checks on the data
        if "examples" in some_data.keys() and len(some_data["examples"]) > 0:
            df_downloaded = pandas.json_normalize(some_data["examples"])
            multidim_data_generation.logit("df_downloaded.shape",df_downloaded.shape)
        else:
            multidim_data_generation.logit("No results downloaded",0)
        
        pipelines_run = pipelines_run + 1        
                                                   
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
