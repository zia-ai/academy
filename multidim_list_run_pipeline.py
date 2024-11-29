"""
python multidim_list_run_pipeline.py

Run all the pipelines for a playbook

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas
import json

# custom imports
import humanfirst
import multidim_loader
import multidim_data_generation

@click.command()
@click.option('-n', '--namespace', type=str, required=True, help='Namespace with pipelines in')
@click.option('-f', '--filter_prefix', type=str, required=False, default="", help='Filter playbooks for things starting with this')
@click.option('-s', '--sequential', is_flag=True, type=bool, required=False, default=False, help='Run sequential instead of parallel')
@click.option('-u', '--max_loops', type=int, required=False, default=1500, help='Maximum number of loops to attempt on trigger checks') 
@click.option('-b', '--max_playbooks', type=int, required=False, default=0, help='Maximum number of playbooks') 
@click.option('-d', '--date_filter_day', type=str, required=True, help='Which YYYY-MM-DD to filter for')
def main(namespace: str,
         filter_prefix: str,
         sequential: bool,
         max_loops: int,
         max_playbooks: int,
         date_filter_day: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    if not sequential:
        raise RuntimeError("Parallel mode not yet implemented")
    
    # Open connection
    hf_api = humanfirst.apis.HFAPI()
    
    # list all playbooks
    playbooks = hf_api.list_playbooks(namespace=namespace)
    df_playbooks = pandas.json_normalize(playbooks)
    print(df_playbooks)
    
    # filter
    if filter_prefix != "":
        print(f"Filtering for things starting {filter_prefix}")
        df_playbooks = df_playbooks[df_playbooks["playbookName"].str.startswith(filter_prefix)]
        df_playbooks[["namespace","playbookName","etcdId"]]
    print(df_playbooks[["namespace","playbookName","etcdId"]])
    
    playbook_ids = list(df_playbooks["etcdId"].unique())
    playbooks_run = 0
    for playbook_id in playbook_ids:
        if max_playbooks > 0 and playbooks_run == max_playbooks:
            print(f'Max Pipelines reached: {max_playbooks}')
            break
        print(playbook_id)
        run_all_piplines(hf_api=hf_api,
                         namespace=namespace,
                         playbook_id=playbook_id,
                         filter_prefix=filter_prefix,
                         max_loops=max_loops,
                         date_filter_day=date_filter_day
                         )
        playbooks_run = playbooks_run + 1

def run_all_piplines(hf_api: humanfirst.apis.HFAPI, namespace:str, playbook_id: str, 
                     filter_prefix: str, max_loops: int, date_filter_day:str) -> bool:
    """Some string"""
    pipelines = hf_api.list_playbook_pipelines(namespace=namespace,
                                   playbook_id=playbook_id)   
    pipelines_run = 0
    for p in pipelines:
              
        multidim_data_generation.logit("Running",p["name"])
        trigger_pipeline = hf_api.trigger_playbook_pipeline(namespace=namespace,
                                                            playbook_id=playbook_id,
                                                            pipeline_id=p["id"])
        if "triggerId" in trigger_pipeline.keys():
            total_wait = multidim_loader.loop_trigger_check_until_done(hf_api=hf_api,
                                                                       namespace=namespace,
                                                          max_loops=max_loops,
                                                          trigger_id=trigger_pipeline["triggerId"],
                                                          debug=False)
            if total_wait == 0:
                raise RuntimeError(f'This pipeline did not complete: {trigger_pipeline["triggerId"]}')
            
            multidim_data_generation.logit(f'Pipline: {p["name"]} total_time:',total_wait)
        
        print("Download whatever was in it")
        
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
        
        some_data = hf_api.export_query_conversation_inputs(namespace=namespace,
                                                playbook_id=playbook_id,
                                                pipeline_id=p["id"],
                                                pipeline_step_id=p["steps"][0]["id"],
                                                # await_next_index=False,
                                                metadata_predicate=metadata_predicate,
                                                source_kind=2, # SOURCE_KIND_GENERATED                                             
                                                timeout=240# just take the first step)
        )
        
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
