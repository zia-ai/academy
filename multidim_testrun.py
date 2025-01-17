"""
python multidim_test_run.py

Takes a workspace assuming
Workspace only has the data you want to run on in it.
Workspace has the prompt name you provide to test
Workspace has a pipeline setup with the sampling you want.

"""

# ******************************************************************************************************************120

# standard imports
import datetime

# 3rd party imports
import click
import pandas
import sentence_transformers

# custom imports
import humanfirst
import multidim_loader

# constants
DEFAULT_SCRIPT_MAX_LOOPS = 240
DEFAULT_SIMILARITY_CLIP = 0.2 # This is very much only an example - probably two low if this is the primary metric - but if it's only and indication
## universal-sentence-encoder-multilingual which is same as HF uses
DEFAULT_SENTENCE_TRANFORMER = 'sentence-transformers/use-cmlm-multilingual'

@click.command()
@click.option("-n", "--namespace", type=str, required=True, help="Target Workspace")
@click.option("-b", "--playbook_id", type=str, required=True, help="Target Workspace")
@click.option("-p", "--pipeline_id", type=str, required=True, help="Pipeline for testing")
@click.option("-e", "--expected_results", type=str, required=True, help="Expected Results CSV file full path")
@click.option("-i", "--index_key", type=str, required=True, help="What the name of the unique index key is (without metadata.)")
@click.option("-k", "--similarity_keys", type=str, required=False, default="", help="comma separated list")
@click.option("-u", "--nlu_keys", type=str, required=False, default="", help="comma separated list")
@click.option("-o", "--pass_or_fail_keys", type=str, required=False, default="", help="comma separated list")
@click.option("-s", "--skip_pipeline_run", is_flag=True, required=False, default=False, help="Target Workspace")
@click.option("-d", "--delimiter", required=False, default="-", help="Delimiter to format any hierarchical intent names")
def main(
    namespace: str, 
    playbook_id: str, 
    pipeline_id: str,
    expected_results: str,
    index_key: str,
    similarity_keys: str,
    nlu_keys: str,
    pass_or_fail_keys: str,
    skip_pipeline_run: bool,
    delimiter: str
    
) -> None:  # pylint: disable=unused-argument
    """Main Function"""

    # create client (remember to export HF_USERNAME and HF_PASSWORD)
    hf_api = humanfirst.apis.HFAPI()

    # run the pipeline if not dry run
    if not skip_pipeline_run:
        trigger_pipeline = hf_api.trigger_playbook_pipeline(
            namespace=namespace, playbook_id=playbook_id, pipeline_id=pipeline_id
        )
        # responds with trigger ID and generation ID

        print(trigger_pipeline)

        # Wait until it is done
        total_wait = multidim_loader.loop_trigger_check_until_done(
            hf_api=hf_api,
            namespace=namespace,
            max_loops=DEFAULT_SCRIPT_MAX_LOOPS,
            trigger_id=trigger_pipeline["triggerId"],
            debug=False,
        )
        print(f'Pipeline finished in: {total_wait}')
    else:
        print(f'Skipped pipeline run as dryrun: {skip_pipeline_run}')

    # put the data into a DF
    some_data = hf_api.export_query_conversation_inputs(namespace=namespace,
                                            playbook_id=playbook_id,
                                            pipeline_id=pipeline_id,
                                            # pipeline_step_id=p["steps"][0]["id"],
                                            # metadata_predicate=metadata_predicate,
                                            source_kind=2, # SOURCE_KIND_GENERATED
                                            timeout=480 # just take the first step)
    )
    df = pandas.json_normalize(some_data["examples"])

    # check that index_key is there in the outputs as a metadata field
    # key by metadata index key and key
    metadata_index_key = f'metadata.{index_key}'
    if not metadata_index_key in df.columns.to_list():
        raise RuntimeError(f'Index key not found: {metadata_index_key}')

    # Check for all the key ground truth values in expected values
    similarity_keys = similarity_keys.split(",")
    nlu_keys = nlu_keys.split(",")
    pass_or_fail_keys = pass_or_fail_keys.split(",")
    all_keys = []
    all_keys.extend(similarity_keys)
    all_keys.extend(nlu_keys)
    all_keys.extend(pass_or_fail_keys)

    # check for all the key values in actual values
    for k in all_keys:
        unique_key_values = list(df["metadata.key"].unique())
        if not k in unique_key_values:
            raise RuntimeError(f'Expected key to be one of the unique metdata.key values: {unique_key_values} ')
    print('Checked all keys are present in pipeline output')
    
    # rename then set the index
    df = df.rename(columns={metadata_index_key:index_key})
    df = df.set_index([index_key,"metadata.key"])
    
    # get the workspace so we can lookup the intent_names
    workspace = hf_api.get_playbook(namespace=namespace,playbook=playbook_id)
    hf_workspace = humanfirst.objects.HFWorkspace.from_json(workspace,delimiter=delimiter)
    
    # Extract the top matching intents.
    df = df.apply(extract_top_matching_intents,args=[hf_workspace],axis=1)
    
    # Find the expected results and set the index no metadata.start
    df_expected_results = pandas.read_csv(expected_results,dtype=str,delimiter=",")
    df_expected_results = df_expected_results.set_index(index_key)
    
    # repeate our key checks
    for k in all_keys:
        search_column = f'{k}_gt'
        if not search_column in df_expected_results.columns.to_list():
            raise RuntimeError(f'Expected key ground truth column in CSV called: {search_column}')
    print('Checked have a <keyname>_gt value column for each key passed')
   
    # going to go with minimal data - don't just include all the metadata runs
    # can put playbook_pipline_id_in filename

    # Do NLU evals
    for u in nlu_keys:
        df_expected_results = df_expected_results.apply(eval_nlu_result,args=[u,df],axis=1)   
        
    # Do Pass Fail Evals
    for o in pass_or_fail_keys:
        df_expected_results = df_expected_results.apply(eval_pass_fail_result,args=[o,df],axis=1)
    

    if not skip_pipeline_run:   
        # Get the model
        print(f'Downloading: {DEFAULT_SENTENCE_TRANFORMER}')
        print(f'This may take a while if it hasn\'t been downloaded before - for instance USE Multilingual requires about 2GB of downloads')
        model = sentence_transformers.SentenceTransformer(DEFAULT_SENTENCE_TRANFORMER)
            
        # Do similarity evals
        for k in similarity_keys:
            df_expected_results = df_expected_results.apply(eval_similarity_result,args=[k,df,model],axis=1)
        
    # Dump the full output
    assert expected_results.endswith(".csv")
    now = datetime.datetime.now().isoformat(timespec="seconds")
    now = now.replace(":","")
    now = now.replace("-","")
    output_filename = expected_results.replace(".csv", f"_{now}_eval.csv")
    assert expected_results != output_filename
    df_expected_results.to_csv(output_filename,index=True, header=True)
    print(f'wrote to: {output_filename}')

    # Dump a excel
    assert expected_results.endswith(".csv")
    df_expected_results = do_formatting(df_expected_results)
    now = datetime.datetime.now().isoformat(timespec="seconds")
    now = now.replace(":","")
    now = now.replace("-","")
    output_filename = expected_results.replace(".csv", f"_{now}_eval.xlsx")
    assert expected_results != output_filename
    df_expected_results.to_excel(output_filename,index=True, header=True)
    print(f'wrote to: {output_filename}')
        
def do_formatting(df: pandas.DataFrame) -> pandas.DataFrame:    
    """Sets all gt columns to grey
    Highlights eval green/red"""
    do_these_cols = []
    for c in df.columns.to_list():
        if c.endswith("_eval") or c.endswith("_gt") or c.endswith("_gt_cp"):
            do_these_cols.append(c)
    df = df.style.apply(highlight_pass_fail,subset=do_these_cols)
    return df
    
def highlight_pass_fail(col:pandas.Series) -> str:
    output_list = []
    for value in col:
        if value == "PASS":
            output_list.append('background-color: mediumseagreen')
        elif value == "FAIL":
            output_list.append('background-color: lightcoral')
        else:
            output_list.append('background-color: lightgrey')
    return output_list

def eval_similarity_result(expected_result_row: pandas.Series,
                           similarity_key: str, 
                           df: pandas.DataFrame, 
                           model: sentence_transformers.SentenceTransformer,
                           duplicate_key: bool = True, 
                           similarity_clip: float = DEFAULT_SIMILARITY_CLIP) -> pandas.Series:
    """Similarity calculation"""

    # if passed true duplicate the key for easy reading
    if duplicate_key:
        expected_result_row[f'{similarity_key}_gt_cp'] = expected_result_row[f'{similarity_key}_gt']
        
    # get the text
    expected_result_row[f'{similarity_key}_text'] = df.loc[(expected_result_row.name,similarity_key),"text"]

    # embed gt and actual
    gt = model.encode(expected_result_row[f'{similarity_key}_gt'])
    actual = model.encode(expected_result_row[f'{similarity_key}_text'])
    
    # do the similarity 
    expected_result_row[f'{similarity_key}_sim'] = model.similarity(gt,actual)[0][0].detach().numpy() # SimilarityFunction.COSINE is default
    
    
    # work out pass fail
    if expected_result_row[f'{similarity_key}_sim'] >= similarity_clip:
        expected_result_row[f'{similarity_key}_eval'] = "PASS"
    else:
        expected_result_row[f'{similarity_key}_eval'] = "FAIL"        
    
    return expected_result_row

def eval_pass_fail_result(expected_result_row: pandas.Series, pass_or_fail_key: str, df: pandas.DataFrame, duplicate_key: bool = True) -> pandas.Series:
    """Compare an exact value"""
    
    # if passed true duplicate the key for easy reading
    if duplicate_key:
        expected_result_row[f'{pass_or_fail_key}_gt_cp'] = expected_result_row[f'{pass_or_fail_key}_gt']
    
    # get the text
    expected_result_row[f'{pass_or_fail_key}_text'] = df.loc[(expected_result_row.name,pass_or_fail_key),"text"]
    
    # compare the result directly
    if expected_result_row[f'{pass_or_fail_key}_gt'] == expected_result_row[f'{pass_or_fail_key}_text']:
        expected_result_row[f'{pass_or_fail_key}_eval'] = "PASS"
    else:
        expected_result_row[f'{pass_or_fail_key}_eval'] = "FAIL"
    
    return expected_result_row

def eval_nlu_result(expected_result_row: pandas.Series, nlu_key: str, df: pandas.DataFrame, duplicate_key: bool = True, n_level: int =1) -> pandas.Series:
    """Do it without clip"""
    
    # if passed true duplicate the key for easy reading
    if duplicate_key:
        expected_result_row[f'{nlu_key}_gt_cp'] = expected_result_row[f'{nlu_key}_gt']
    
    # get the text
    expected_result_row[f'{nlu_key}_text'] = df.loc[(expected_result_row.name,nlu_key),"text"]
    
    # get each nlu result
    for i in range(n_level):
        
        # intent name
        intent_name_column_name = f'tm{i}_name'
        expected_result_row[f'{nlu_key}_{intent_name_column_name}'] = df.loc[(expected_result_row.name,nlu_key),intent_name_column_name]

        # intent score    
        intent_score_column_name = f'tm{i}_score'
        expected_result_row[f'{nlu_key}_{intent_score_column_name}'] = df.loc[(expected_result_row.name,nlu_key),intent_score_column_name]

    # compare the top nlu result
    if expected_result_row[f'{nlu_key}_gt'] == expected_result_row[f'{nlu_key}_tm0_name']:
        expected_result_row[f'{nlu_key}_eval'] = "PASS"
    else:
        expected_result_row[f'{nlu_key}_eval'] = "FAIL"
    
    return expected_result_row

def extract_top_matching_intents(row: pandas.Series, hf_workspace: humanfirst.objects.HFWorkspace, n_levels: int = 3) -> pandas.Series:
    """Extracts the matching_intents column into 
    matching intent name, score for the first n intents (max 9)
    
    if column doesn't exist returns unaltered row"""
    
    # check not too many levels
    if n_levels >= 10:
        raise RuntimeError(f'n_levels exceeds 9: {n_levels}')
    
    # check we have the matching intents
    if not "matching_intents" in row.index.to_list():
        return row
    matching_intents = row["matching_intents"]
    
    ### build the n levels
    for i in range(n_levels):
        row[f'tm{i}_name'] = hf_workspace.get_fully_qualified_intent_name(matching_intents[0]["intent_id"])
        row[f'tm{i}_score'] = matching_intents[0]["intent_score"]
        
    return row        
    

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
