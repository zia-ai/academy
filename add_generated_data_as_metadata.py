"""
python add_generated_data_as_metadata.py

Doesn't work for merge stash as there won't be source conversation id metadata in the result
"""
# ******************************************************************************************************************120

# standard imports
import json
import re

# 3rd party imports
import pandas
import click

# custom imports
import back_to_hf_unlabelled

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input HF JSON File Path')
@click.option('-g', '--generated_data', type=str, required=True, help='Generated data')
def main(filename: str, generated_data: str) -> None:
    """Main Function"""

    # load input data
    with open(filename, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-")
    print(f"Loaded data to annotate:              {filename}")
    print(f"Shape is:                             {df.shape}")
    if "context-context_id" not in df.columns:
        raise RuntimeError("Context id column is not present in the unlabelled dataset")
    print(f'Unique context-context_id:            {df["context-context_id"].nunique()}')

    # load generated data
    with open(generated_data, mode="r", encoding="utf8") as file:
        gen_data = json.load(file)
    df_gen = pandas.json_normalize(data=gen_data["examples"], sep="-")
    print(f"Loaded query result:                  {generated_data}")
    print(f"Shape is:                             {df_gen.shape}")
    if "metadata-sourceConversationId" not in df_gen.columns:
        raise RuntimeError("Source Conversation ID metadata is not present in the generated dataset")
    print(f'Unique metadata-sourceConversationId: {df_gen["metadata-sourceConversationId"].nunique()}')

    # Check generated metadata fields not already on target - TODO: don't really understand why this is here
    all_gen_metadata = [
        'metadata-generationRunId',
        'metadata-generationTime',
        'metadata-integrationId',
        'metadata-llmModelName',
        'metadata-prompt',
        'metadata-promptId',
        'metadata-sourceConversationId',
        'metadata-pipelineId',
        'metadata-pipelineStepId'
    ]

    # so have df_gen and df - df_gen got newline split utterances - assumption.
    # so we have text and sourceConversationId and a sequence number, and a key to
    df_gen = df_gen[["metadata-sourceConversationId","text","id","created_at"]]

    # observation keys interested in
    observation_keys = [
		"conversation_dead_end:",
		"conversation_loops:",
		"conversation_stall:",
		"escalation_capability:",
		"incomplete_utterance:",
		"interrupt_handling:",
		"staying_on_topic:",
		"tone_and_language:",
		"understanding:",
		"total_score:",
    ]

    response_keys = [
        "total_score:",
        "issues:",
        "reasoning:"
    ]

    # make a regex
    # start of text, followed by an observation_key and an optional space
    re_string = f'^({"|".join(response_keys)})[ ]*'
    re_key = re.compile(re_string)
    print("Re String")
    print(re_string)

    # TODO: this apply regex probably less effecient than individual logic on the columns and the pandas str matching fucntion.
    df_gen = df_gen.apply(match_me,args=[re_key],axis=1)

    # Make metadata TODO: de brain-fart this - some sort of transposing assembling thing?
    source_convos_list = df_gen["metadata-sourceConversationId"].unique()
    metadata_index = {}
    for convo_id in source_convos_list:
        df_temp = df_gen[df_gen["metadata-sourceConversationId"]==convo_id].copy(deep=True)
        assert isinstance(df_temp,pandas.DataFrame)
        df_temp.sort_values("key",inplace=True)
        metadata_index[convo_id] = {}
        for i,row in df_temp.iterrows():
            if row.valid_key:
                metadata_index[convo_id][row.key]=row.value
    df_metadata = pandas.DataFrame.from_dict(metadata_index,orient="index")
    print("Prepared metadata")
    print(f"Shape is: {df_metadata.shape}")


    # Work out which original metadata columns to delete
    print("Dropping columns")
    metadata_columns_to_keep = []
    columns_list = df.columns.to_list()
    metadata_columns_to_delete = []
    for column in columns_list:
        if column in metadata_columns_to_keep:
            continue
        else:
            if column.startswith("metadata-"):
                metadata_columns_to_delete.append(column)

    # drop them
    df.drop(columns=metadata_columns_to_delete,inplace=True)
    print("Remaining Columns")
    print(df.columns.to_list())
    print(f'Shape is: {df.shape}')

    # join the new metadata on
    print("Joining Data")
    df = df.join(df_metadata,on="context-context_id")
    df["metadata-context_id"] = df["context-context_id"]
    df = df.fillna("")
    print("Joined Columns")
    print(df.columns.to_list())
    print(f'Final df has shape: {df.shape}')
    print(f'Unique context-contedxt_ids in final is: {df["context-context_id"].nunique()}')

    # displaying a sample across one field
    gb = df[["metadata-total_score","metadata-reasoning"]].groupby("metadata-total_score").count()
    print(gb)
    print(gb["metadata-reasoning"].sum())

    # convo_ids not processed
    df_not_processed = df[df["metadata-total_score"]==""]
    print(df_not_processed[["context-context_id","text"]])

    # make a dict again
    workspace_dict = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": back_to_hf_unlabelled.df_to_formatted_json(df,sep="-")
    }
    print(f'Len of examples is {len(workspace_dict["examples"])}')

    # write output
    output_path = filename.replace(".json","_gen_metadata.json")
    with open(output_path,mode="w",encoding="utf8") as file_out:
        json.dump(workspace_dict,file_out,indent=2)
        print(f'Wrote to: {output_path}')


def match_me(row: pandas.Series, re_key: re) -> pandas.Series:
    row["valid_key"] = False
    row["key"] = ""
    row["value"] = ""
    matches = re_key.search(row["text"])
    if matches:
        row["valid_key"] = True
        row["key"] = matches[1]
        row["key"] = str(row["key"]).strip(":")
        row["key"] = f'metadata-{row["key"]}'
        row["value"] = str(row["text"]).replace(matches[0],"")
        row["value"] = str(row["value"]).strip(" ")
    if row["key"] == "metadata-reasoning":
        row["value"] = str(row["value"]).replace(",","")
    if row["key"] == "metadata-issues":
        row["value"] = str(row["value"]).replace(" ","")
    return row

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
