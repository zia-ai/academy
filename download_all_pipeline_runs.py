"""
python template.py

Downloads all runs of a pipeline to a provided dir

"""
# ******************************************************************************************************************120

# standard imports
import os

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst

@click.command()
@click.option('-o', '--output_dirname', type=str, required=True, help='Output directoy path')
@click.option("-n", "--namespace", type=str, required=True, help="Target Workspace")
@click.option("-b", "--playbook_id", type=str, required=True, help="Target Workspace")
@click.option("-p", "--pipeline_id", type=str, required=True, help="Pipeline for testing")
def main(output_dirname: str,
         namespace: str,
         playbook_id: str,
         pipeline_id: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    assert os.path.isdir(output_dirname)
    
    # how to tell what we get?
    hf_api = humanfirst.apis.HFAPI()
    
    # get all the pipelines which includes the step ids and the lastGenerationRunId
    playbook_pipelines = hf_api.list_playbook_pipelines(namespace,playbook_id)
    df_pp = pandas.json_normalize(playbook_pipelines).set_index("id")
    print(df_pp.loc[pipeline_id])
    
    max_run_id = df_pp.loc[pipeline_id,"lastGenerationRunId"]
    
    # download each 
    eqci_response = hf_api.export_query_conversation_inputs(namespace,playbook_id,pipeline_id,
                                                generation_run_id=str(max_run_id),
                                                source=-1)
      
    # Current behaviour will be 2025-01-28
    # Only the last run id will have any data in it.
    # 
    # TODO: This is not ideal.  Ideally (IMO)
    # Each generation id should return only the data produced in that generation id (i.e) no cached data.
    # Then in the GUI the current most recent run functionality should be replicated with say a value of generation run id 0
    # - this should return the latest run id and all the cached data, providing a full view of the pipeline
    # - this might also be the default if no generation id is passed.
    # Then the current GUI "All runs" functionality should be available if say a generation id of -1 is passed.
    # - i.e every result ever produced for that pipeline is returned. 

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
