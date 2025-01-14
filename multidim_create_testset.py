"""
python multidim_create_testset.py

Takes a csv with the first column being named after a unique metadata key index
Extracts every utterance for both client and expert for that convo to a file
Uploads that file to a provided convo set
Checks the trigger completes and the data is ready.

"""
# ******************************************************************************************************************120

# standard imports
import json
import time

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst

# constants
DEFAULT_SCRIPT_MAX_POLLS=240
DEFAULT_SCRIPT_TIMEOUT=300

@click.command()
@click.option('-e', '--expected_results', type=str, required=True, help='path to CSV of expected results')
@click.option('-t', '--test_convoset_name', type=str, required=True, help='How to name the conversation set')
@click.option('-n', '--namespace', type=str, required=True, help='Name of namespace')
@click.option('-p', '--playbook', type=str, required=True, help='Playbook ID')
@click.option('-o', '--output_file_name', type=str, required=False, default="", help='Optional filename to write the JSON of the convos to')
@click.option('-f', '--frig', is_flag=True, required=False, default=False, help='multidim frig')
def main(expected_results: str,
         test_convoset_name: str,
         namespace: str,
         playbook: str,
         frig: bool,
         output_file_name: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    # get the DF
    df_expected_results = pandas.read_csv(expected_results,dtype=str,delimiter=",")
    
    # create client (remember to export HF_USERNAME and HF_PASSWORD)
    hf_api = humanfirst.apis.HFAPI()
    
    # clear out conversation set
    convosrc_id = clear_out_convoset(test_convoset_name=test_convoset_name,
                                     namespace=namespace,
                                     hf_api=hf_api)
    

    # Get the conovesations taking column 0 as the key to lookup
    # we deduplicate witn unique in case there are multiple classes per convo
    key_col = df_expected_results.columns[0]
    test_convos = get_convos(namespace=namespace,playbook=playbook,
                             ids=list(df_expected_results[key_col].unique()),
                             key_col=key_col,
                             hf_api=hf_api,
                             frig=frig)
       
    # write to output
    write_convo_file(test_convos=test_convos, output_file_name=output_file_name)
        # Print DF for checks
    df_convos = pandas.json_normalize(test_convos)
    print(f'Wrote {len(test_convos)} utterrances across {df_convos["context.context_id"].nunique()} convos to: {output_file_name}')
    
    # upload to convoset
    upload_response = hf_api.upload_json_file_to_conversation_source(namespace=namespace,
                                                   conversation_source_id=convosrc_id,
                                                   upload_name=expected_results,
                                                   fqfp=output_file_name)
    
    # check trigger
    check_trigger_completion(namespace=namespace,
                             trigger_id=upload_response["triggerId"],
                             hf_api=hf_api)
    
def check_trigger_completion(namespace: str, 
                             trigger_id: str, 
                             hf_api: humanfirst.apis.HFAPI,
                             max_polls: int = DEFAULT_SCRIPT_MAX_POLLS):
    # Check the status of the trigger on the id 
    trigger_response = hf_api.describe_trigger(namespace=namespace,
                            trigger_id=trigger_id)
    for i in range(max_polls):
        status = trigger_response["triggerState"]["status"]
        if status == "TRIGGER_STATUS_COMPLETED":
            break
        else:
            time.sleep(1)
            print(f'{i:<4} {trigger_id:<30} {status}')
            
def get_convos(namespace: str, playbook: str,
               ids: list, key_col: str, 
               hf_api: humanfirst.apis.HFAPI, 
               frig: bool = False) -> list:
    """Queries the workspace for the list of conversation IDs
    Downloads both client expert
    Returns the full conversations sorted by created_at"""
    
    # build the metadata_predicate
    metadata_predicate = []
    for c in ids:
        metadata_predicate.append(
            {
                "key": key_col,
                "operator": "ANY",
                "value": c,
                "optional": True
            }
        )
    
    # download both sides of the conversation
    test_convos_client = hf_api.export_query_conversation_inputs(namespace=namespace,
                                                                 playbook_id=playbook,
                                                                 metadata_predicate=metadata_predicate,
                                                                 source_kind=1,# unlabelled
                                                                 source=1, # client
                                                                 timeout=DEFAULT_SCRIPT_TIMEOUT
                                                                 )
    test_convos_expert = hf_api.export_query_conversation_inputs(namespace=namespace,
                                                                 playbook_id=playbook,
                                                                 metadata_predicate=metadata_predicate,
                                                                 source_kind=1,# unlabelled
                                                                 source=2, # expert
                                                                 timeout=DEFAULT_SCRIPT_TIMEOUT                                                                
                                                                 )
    
    # Reassmeble
    all_examples = []
    all_examples.extend(test_convos_client["examples"])
    all_examples.extend(test_convos_expert["examples"])
    
    # sort based on id and created at
    all_examples.sort(key=lambda x: x["context"]["context_id"] + "-" + x["created_at"])
    
    # this is a frig because data multiplication in multidim creates duplicates of id
    test_convos = []
    for e in all_examples:
        if frig:
            if e["context"]["context_id"] == e["metadata"]["abcd_id"]:
                test_convos.append(e)
        else:
            test_convos.append(e)
            
    return test_convos

def clear_out_convoset(test_convoset_name: str, 
                       namespace: str,
                       hf_api: humanfirst.apis.HFAPI) -> str:
    """Takes a convoset name, finds it if exists and deletes all files,
    if it doesn't creates it.  Returns the convoset_src_id to upload files to"""
   
    # check or create convoset
    convosets = hf_api.get_conversation_set_list(namespace=namespace)
    df_convosets = pandas.json_normalize(convosets)
    if test_convoset_name in df_convosets["name"].to_list():
        df_convosets = df_convosets.set_index("name")
        convosrc_id = df_convosets.loc[test_convoset_name,"sources"]
        convosrc_id = convosrc_id[0]["conversationSourceId"]
        print(f'Convoset already exists: {test_convoset_name} ({convosrc_id})')
    else:
        # else create it
        create_cs_response = hf_api.create_conversation_set_with_set_and_src_id(namespace=namespace,
                                                        convoset_name=test_convoset_name)
        convosrc_id = create_cs_response["convosrc_id"]
    
    # List any files
    list_existing_files = hf_api.list_conversation_src_files(namespace=namespace,
                                                             conversation_set_src_id=convosrc_id)
    for f in list_existing_files:
        # Delete with no trigger = true - then the upload will trigger it
        hf_api.delete_conversation_file(namespace=namespace,
                                        conversation_set_src_id=convosrc_id,
                                        file_name=f["name"],
                                        no_trigger=True)
        print(f'Deleted file from convoset: {f["name"]}')
        
    return convosrc_id

def write_convo_file(test_convos: dict, output_file_name: str):
    """Write the convoset to a HF JSON format to a file"""
    output_dict = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": test_convos
    }
    file_out = open(output_file_name,mode="w",encoding="utf-8")
    json.dump(output_dict,file_out,indent=2)
    file_out.close()

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
