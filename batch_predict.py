"""
python ./batch_predict.py

This script predicts the intents of utterances in batches
The input CSV file should contain a column containing utterances for prediction
The output CSV contains fully_qualified_intent_name, confidence, parent_intent, and child_intent columns additionally 

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
import os

# third party imports
import click
import pandas
import humanfirst

@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-o', '--output_filename', type=str, default='', help='Output File')
@click.option('-t', '--uttr_col', type=str, required=True, help='Utterance column name')
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-d', '--delimiter', type=str, required=True, help='Where there is a hierachy what to use to join parent child (required) try "-" or "--"')
@click.option('-c', '--chunk', type=int, default=500, help='Optional size of maximum chunk to send to batch predict default 500')
@click.option('-m', '--model_id', type=str, default="", help='Optional Model ID to run a specific NLU version')
@click.option('-r', '--revision_id', type=str, default="", help='Optional Revision ID to run a specific NLU version')
def main(input_filename: str, output_filename: str, uttr_col: str,
         username: str, password: int, namespace: bool, playbook: str, 
         delimiter: str, chunk: int,
         model_id: str, revision_id: str) -> None:
    """Main Function"""
    
    # must provide both model_id and revision_id 
    if model_id != "" or revision_id != "":
        if model_id == "" or revision_id == "":
            raise RuntimeError(f'If specifying model_id or revision_id both must be present')
        
    # Read the file
    df = pandas.read_csv(input_filename, encoding='utf8')
    assert isinstance(df, pandas.DataFrame)

    # drop all rows which don't have any meaningful data
    print(f'Shape with all lines:                {df.shape}')
    df = df[~df[uttr_col].isna()]
    print(f'Shape with only non-blank verbatims: {df.shape}')

    # rename the utterance column
    rename_col = {
        uttr_col: "utterance"
    }
    df.rename(columns=rename_col,inplace=True)

    # get a HF SDK
    hf_api = humanfirst.apis.HFAPI(username=username, password=password)
       
    # get the nlu engines we might want to use
    nlu_engines = hf_api.get_nlu_engines(namespace=namespace,playbook=playbook)
    print(f"The total number of nlu engines is: {len(nlu_engines)}")
    print(nlu_engines)
    
    # if there is no nlu_engine fail
    if len(nlu_engines) == 0:
        raise RuntimeError("No nlu engine for that workspace")
    if model_id != "":
        found = False
        for n in nlu_engines:
            if n["id"] == model_id:
                found = True
                break
        if not found:
            raise RuntimeError(f"Cannot find NLU model_id: {model_id}")
        nlu_engine = hf_api.get_nlu_engine(namespace=namespace,playbook=playbook,nlu_id=model_id)
        print(nlu_engine)

    # for any model_id check the revisions
    if model_id != "":
            
        # list the trained NLU
        list_trained_nlu = hf_api.list_trained_nlu(namespace=namespace,playbook=playbook)
        if len(list_trained_nlu) == 0:
            raise RuntimeError(f'NLU engine has not been trained - please train it first before running script')

        df_trained_nlu = pandas.json_normalize(list_trained_nlu)
        df_trained_nlu = df_trained_nlu.set_index('runId')
        print("These are the trained revisions for that NLU engine")
        print(df_trained_nlu[["nluIds","name","status","createdAt"]])
        
        if revision_id != '':
            print(f'Checking for revision_id: {revision_id}')
            if df_trained_nlu.loc[revision_id,"status"] != "RUN_STATUS_AVAILABLE":
                raise RuntimeError(f'revision status is not ready: {df_trained_nlu.loc[revision_id,"status"]}')
           
        print(f'Confirmed model_id: {model_id} and revision_id: {revision_id}')
        
    # loop through in the required chunks
    fully_qualified_intent_name = []
    confidence = []
    num_processed = 0
    for i in range(0, df['utterance'].size, chunk):
        utterance_chunk = list(df['utterance'][i: i + chunk])
        
        # Batch predict will default model_id and revision_id
        if model_id == "":
            # example of simple normal call
            response_dict = hf_api.batchPredict(
                                            sentences=utterance_chunk,
                                            namespace=namespace,
                                            playbook=playbook)
        else:
            # example overriding the model and revision to use
            response_dict = hf_api.batchPredict(
                                sentences=utterance_chunk,
                                namespace=namespace,
                                playbook=playbook,
                                model_id=model_id,
                                revision_id=revision_id)


        # expecting a result for every element in the chunk - will error if one doesn't exist
        for j in range(len(utterance_chunk)):
            
            # Assign confidence to a list to make a DF later
            confidence.append(response_dict[j]['matches'][0]['score'])
                                   
            # This gives you "id" which you can look up with FQIN - except that the ID may nolonger exist in the workspace
            # fully_qualified_intent_name.append(workspace.get_fully_qualified_intent_name(response_dict[j]['matches'][0]["id"]))
            # note response_dict[j]['matches'][0]['hierarchyNames'] will also give you the path and you can join themselves
            fully_qualified_intent_name.append(delimiter.join(response_dict[j]['matches'][0]['hierarchyNames']))
            
        num_processed = num_processed + len(utterance_chunk)
        print(f'Completed: {num_processed} utterances')
    df['fully_qualified_intent_name'] = fully_qualified_intent_name
    df['confidence'] = confidence

    if output_filename == '':
        output_filename = input_filename.replace(".csv","_predictions.csv")

    df.to_csv(output_filename, index=False, encoding='utf8')
    print(f'Predictions CSV is saved at {output_filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
