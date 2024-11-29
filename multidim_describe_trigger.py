"""
python multidim_list_pipeline.py

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
def main(namespace: str,
         filter_prefix: str,
         sequential: bool,
         max_loops: int,
         max_playbooks: int) -> None: # pylint: disable=unused-argument
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
                         max_loops=max_loops
                         )
        playbooks_run = playbooks_run + 1

def run_all_piplines(hf_api: humanfirst.apis.HFAPI, namespace:str, playbook_id: str, 
                     filter_prefix: str, max_loops: int) -> bool:
    """Some string"""
    pipelines = hf_api.list_playbook_pipelines(namespace=namespace,
                                   playbook_id=playbook_id)   
    print(pipelines)
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
                                                          debug=True)
            if total_wait == 0:
                raise RuntimeError(f'This pipeline did not complete: {trigger_pipeline["triggerId"]}')
            
            multidim_data_generation.logit(f"Pipline: {p} total_time:",total_wait)
        
        print("Download whatever was in it")
        some_data = hf_api.export_query_conversation_inputs(namespace=namespace,
                                                playbook_id=playbook_id,
                                                pipeline_id=p["id"],
                                                pipeline_step_id=p["steps"][0]["id"],
                                                timeout=240# just take the first step)
        )
        
        # dump output to file
        with open(f"./data/{playbook_id}-{p["id"]}-{p["steps"][0]["id"]}") as file_out:
            file_out.write(json.dump(some_data,file_out,indent=2))
        
        pipelines_run = pipelines_run + 1
        
    print(f"Ran a total of pipelines for playboo: {pipelines_run}")
                                                   
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
