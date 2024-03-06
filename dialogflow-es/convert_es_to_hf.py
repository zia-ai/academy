"""
python convert_es_to_hf.py -f filepath

"""
# *********************************************************************************************************************

# standard imports
import json
import yaml
import re

# 3rd party imports
import pandas
import click
import humanfirst


@click.command()
@click.option('-f', '--filepath', type=str, required=True, help='ES conversation json file')
def main(filepath: str):
    """Main Function"""

    # read file
    with open(filepath, mode="r", encoding="utf8") as f:
        data = json.load(f)

    # Iterate through the JSON objects and extract key-value pairs from textPayload
    request_data = []
    response_data = []
    for item in data:
        text_payload = item.get("textPayload")
        if text_payload:
            try:
                # Extract the key-value pairs from the textPayload
                data_extract = text_payload.split(": ", 1)[1]
                data_type = item.get("labels")["type"]
                if data_type == "dialogflow_request":
                    payload_data = json.loads(data_extract)
                    payload_data["query_input"] = json.loads(payload_data["query_input"])
                    payload_data["created_at"] = item.get("timestamp")
                    payload_data["log_name"] = item.get("logName")
                    payload_data["project_id"] = item.get("resource")["labels"]["project_id"]
                    request_data.append(payload_data)

                elif data_type == "dialogflow_response":
                    # response is in the combination of yaml and protobuf
                    # Clean up the data extract to make it valid YAML
                    data_extract = re.sub(r'(\w+)\s*{', r'\1:', data_extract)
                    data_extract = re.sub(r'}', '', data_extract)
                    payload_data = yaml.safe_load(data_extract)
                    payload_data["created_at"] = item.get("timestamp")
                    payload_data["log_name"] = item.get("logName")
                    payload_data["project_id"] = item.get("resource")["labels"]["project_id"]
                    response_data.append(payload_data)

                # Print the extracted data
                # print(json.dumps(payload_data,indent=2))
            except RuntimeError as e:
                print(f"Error extracting data from textPayload: {e}")
                quit()

    # print(json.dumps(response_data,indent=2))
    df_client = pandas.json_normalize(data=request_data, sep="-")
    df_expert = pandas.json_normalize(data=response_data, sep="-")

    df_client = df_client.explode('query_input-text-textInputs',ignore_index=True)
    df_client["utterance"] = df_client["query_input-text-textInputs"].apply(
        lambda x: x if pandas.isna(x) else x["text"])

    df_client["role"] = "client"
    df_expert["role"] = "expert"
    print(df_client.columns)
    print(df_expert.columns)

    df_client = df_client[['session',
                           'created_at',
                           'utterance',
                           'role',
                           'project_id',
                           'log_name']].rename(columns={"session": "convo_id"}).reset_index(drop=True)

    df_expert = df_expert[["session_id",
                           "created_at",
                           "result-fulfillment-speech",
                           "role",
                           "project_id",
                           "log_name",
                           "result-score",
                           "result-metadata-intent_name",
                           "result-metadata-is_fallback_intent"]].rename(
                               columns={"session_id": "convo_id",
                                        "result-fulfillment-speech": "utterance",
                                        "result-score":"confidence_score",
                                        "result-metadata-intent_name": "intent_name",
                                        "result-metadata-is_fallback_intent": "is_fallback_intent"}).reset_index(
                                            drop=True)

    df = pandas.concat([df_client, df_expert]).reset_index(drop=True)

    df["utterance"] = df["utterance"].fillna("")

    # index the speakers
    df['idx'] = df.groupby(["convo_id"]).cumcount()
    df['idx_max'] = df.groupby(["convo_id"])[
        'idx'].transform("max")

    # This info lets you filter for the first or last thing the client says
    # this is very useful in boot strapping bot design
    # 0s for expert
    df['idx_client'] = df.groupby(
        ["convo_id", 'role']).cumcount().where(df.role == 'client', 0)
    df['first_client_utt'] = df.apply(decide_role_filter_values,args=['idx_client','client',0],axis=1)
    df['second_client_utt'] = df.apply(decide_role_filter_values,args=['idx_client','client',1],axis=1)

    # same for expert
    df['idx_expert'] = df.groupby(
        ["convo_id", 'role']).cumcount().where(df.role == 'expert', 0)
    df['first_expert_utt'] = df.apply(decide_role_filter_values,args=['idx_expert','expert',0],axis=1)
    df['second_expert_utt'] = df.apply(decide_role_filter_values,args=['idx_expert','expert',1],axis=1)


    df = df.sort_values(["convo_id","created_at"]).reset_index(drop=True)

    df_len = df.shape[0]
    for i,row in df.iterrows():
        if pandas.isna(row["intent_name"]) and row["role"] == "client":
            j = i+1
            while j < df_len:
                if df.loc[j]["role"] == "expert":
                    df.loc[i, "intent_name"] = df.loc[j, "intent_name"]
                    df.loc[i, "confidence_score"] = df.loc[j, "confidence_score"]
                    df.loc[i, "is_fallback_intent"] = df.loc[j, "is_fallback_intent"]
                    break
                j = j+1

    # created metadata field
    metadata_keys = ["confidence_score",
                     "intent_name",
                     "is_fallback_intent",
                     "project_id",
                     "convo_id",
                     "log_name",
                     "created_at",
                     "first_client_utt",
                     "second_client_utt",
                     "first_expert_utt",
                     "second_expert_utt"]

    df["metadata"] = df[metadata_keys].apply(create_metadata, axis=1)

    df["idx"] = df.groupby(["convo_id"]).cumcount()
    df = df.set_index(["convo_id", "idx"])
    print(df)

    # build examples
    df = df.apply(build_examples, axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.objects.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    output_filepath = f"{filepath.split('.json')[0]}_hf.json"
    file_out = open(output_filepath, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"{output_filepath} is successfully created")

def decide_role_filter_values(row: pandas.Series, column_name: str, role_filter: str, value_filter: str) -> bool:
    """Determine whether this is the 0,1,2 where the role is also somthing"""
    if row[column_name] == value_filter and row["role"] == role_filter:
        return True
    else:
        return False

def build_examples(row: pandas.Series) -> pandas.Series:
    '''Build the examples'''

    # build examples
    example = humanfirst.objects.HFExample(
        text=row['utterance'],
        id=f'example-{row.name[0]}-{row.name[1]}',
        created_at=row['created_at'],
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row["metadata"],
        # this links the individual utterances into their conversation
        context=humanfirst.objects.HFContext(
            str(row.name[0]),
            # any ID can be used recommend a hash of the text
            # which is repeatable or the external conversation id if there is one.
            'conversation',  # the type of document
            row['role']  # the speakers role in the conversations
        )
    )
    row['example'] = example
    return row


def create_metadata(row: pandas.Series) -> dict:
    """Creates metadata"""

    metadata = {}
    for col_name in list(row.index):
        metadata[col_name] = str(row[col_name])
    return metadata


if __name__ == "__main__":
    main() # pylint: disable=no-value-for-parameter
