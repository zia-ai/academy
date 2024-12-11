"""
python html_to_json.py

Takes a directory of .html files
Checks the token numbers and truncates to a passed model applicable limit
Produces a HumanFirst JSON format output

"""
# ******************************************************************************************************************120

# standard imports
import os
import json

# 3rd party imports
import click
import pandas
import tiktoken

# custom imports
import humanfirst

@click.command()
@click.option('-d', '--directory', type=str, required=True, help='Directory with html files in')
@click.option('-h', '--head', type=int, required=False, default=0, help='Only do first n')
@click.option('-t', '--truncate', type=int, required=False, default=124000, help='Truncate records at this number of tokens')
def main(directory: str,
         head: int,
         truncate: int) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    # read file list
    list_file_names = os.listdir(directory)
    list_contents = []
    files_processed = 0 
    
    # trim number to process
    if head > 0:
        list_file_names = list_file_names[0:head]
    
    # read file list
    for f in list_file_names:
        if f.endswith("html"):
            fqfn = os.path.join(directory,f)
            file_in = open(fqfn,encoding="utf8",mode="r")
            contents = file_in.read()
            file_in.close()
            list_contents.append(contents)
        files_processed = files_processed + 1
    print(f'Read files: {files_processed}')
    
    # make a dataframe
    df = pandas.DataFrame(data=zip(list_file_names,list_contents),
                          columns=["filename","contents"])
    df = df.fillna('')
    
    # embed the records
    embeddings = tiktoken.encoding_for_model("gpt-4o")
    df["embedded"] = df["contents"].apply(embeddings.encode)
    
    # count them
    df["original_token_count"] = df["embedded"].apply(len)
    
    # print some info
    print(f'Max tokens is: {df["original_token_count"].max()}')
    print(f'Avg tokens is: {int(df["original_token_count"].mean())}')
    
    # truncate where necessary 
    df["truncated_record"] = df["original_token_count"] >= truncate
    df["embedded_truncated"] = df["embedded"].apply(lambda x: x[0:truncate])
    df["truncated_token_count"] = df["embedded_truncated"].apply(len)
    df["contents"] = df["embedded_truncated"].apply(embeddings.decode)
    
    # summarise truncation
    print(df[["original_token_count","truncated_record"]].groupby("truncated_record").count())
    
    # drop embeddings
    df = df.drop(columns=["embedded","embedded_truncated"])
      
    # build examples
    workspace = humanfirst.objects.HFWorkspace()
    df = df.apply(build_examples,args=[workspace],axis=1)
    assert isinstance(df,pandas.DataFrame)
    
    # write output
    workspace_json = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": df["example"].to_list()
    }
    output_filename = os.path.join(directory,"output.json")
    with open(output_filename,encoding="utf8",mode="w") as file_out:
        json.dump(workspace_json,file_out,indent=2)
        print(f'Wrote to: {output_filename}')

        
        
    
def build_examples(row: pandas.Series,
                   workspace: humanfirst.objects.HFWorkspace) -> pandas.Series:
    """Build the shape of the output"""
    
    # build metadata
    metadata = {}
    for k in row.keys():
        if not k in ["contents","name"]:
            metadata[k] = str(row[k])
    
    row["example"] = {
        "id": f'example-{row["filename"]}',
        "text": row["contents"],
        "context":{
            "context_id":row["filename"],
            "type": "conversation",
            "role": "client"
        },
        "metadata": metadata
    }
    return row
    
if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
