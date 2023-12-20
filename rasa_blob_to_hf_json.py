"""
python rasa_blob_to_hf_json.py

"""
# *********************************************************************************************************************

# standard imports
import json
import datetime
from typing import Union

# 3rd party imports
import pandas
import click
import tqdm
import humanfirst


@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None:
    """Main Function"""

    # load data
    with open(filename, mode="r", encoding="utf8") as f:
        data = json.load(f)

    # form dataframe
    df = pandas.json_normalize(data["events"], sep='-')

    df["id"] = data["sender_id"]
    # print(df[["event","timestamp","text","metadata-utter_action"]])

    # timestamp is in unix format
    # convert the timestamp to datetime format
    df["timestamp"] = df["timestamp"].astype(float)
    df['created_at'] = df["timestamp"].apply(
        datetime.datetime.fromtimestamp)

    # copy only relevant information to sub dataset
    df = df.loc[(df["event"] == "user") | (df["event"] == "bot")].copy(deep=True).reset_index(drop=True)

    # rename the column names
    df.rename(columns={
        "event":"role",
        "metadata-assistant_id":"assistant_id",
        "metadata-model_id": "model_id",
        "parse_data-intent-confidence": "first_intent_confidence",
        "parse_data-intent-name":"first_intent_name",
        "metadata-utter_action": "utter_action"
    },inplace=True)

    df["role"] = df["role"].apply(rename_role)

    # sort conversation utterances
    df.sort_values(["id", "created_at"], inplace=True)
    # index the speakers
    df['idx'] = df.groupby(["id"]).cumcount()
    df['idx_max'] = df.groupby(["id"])[
        'idx'].transform("max")

    # This info lets you filter for the first or last thing the client says
    # this is very useful in boot strapping bot design
    # 0s for expert
    df['idx_client'] = df.groupby(
        ["id", 'role']).cumcount().where(df.role == 'client', 0)
    df['first_client_utt'] = df.apply(decide_role_filter_values,args=['idx_client','client',0],axis=1)
    df['second_client_utt'] = df.apply(decide_role_filter_values,args=['idx_client','client',1],axis=1)

    # same for expert
    df['idx_expert'] = df.groupby(
        ["id", 'role']).cumcount().where(df.role == 'expert', 0)
    df['first_expert_utt'] = df.apply(decide_role_filter_values,args=['idx_expert','expert',0],axis=1)
    df['second_expert_utt'] = df.apply(decide_role_filter_values,args=['idx_expert','expert',1],axis=1)

    # set value to utter_action metadata for client utterances
    df["utter_action"].fillna(value="expert metadata" ,inplace=True)

    # extract second and third intent predictions
    df = df.apply(extract_intent_ranking,axis=1)

    # metadata
    metadata_keys = [
                    'id', 'idx_client', "idx_expert", 'created_at',
                    'first_client_utt', 'second_client_utt',
                    'first_expert_utt', 'second_expert_utt',
                    "assistant_id",
                    "model_id",
                    "input_channel",
                    "message_id",
                    "first_intent_confidence",
                    "first_intent_name",
                    "second_intent_confidence",
                    "second_intent_name",
                    "third_intent_confidence",
                    "third_intent_name",
                    "utter_action"]

    # build metadata for utterances or conversations
    dict_of_file_level_values = {
        'loaded_date': datetime.datetime.now().isoformat(),
        'script_name': 'rasa_blob_to_hf_json.py'
    }
    print("Capturing these metadata keys")
    print(metadata_keys)
    print("Capturing these file level values for metaddata")
    print(dict_of_file_level_values)
    df['metadata'] = df.apply(create_metadata, args=[
                              metadata_keys, dict_of_file_level_values], axis=1)

    # build examples
    print("Commencing build examples")
    tqdm.tqdm.pandas()
    df = df.progress_apply(build_examples,
                           args=["text", "id", "created_at"], axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.objects.HFWorkspace()

    # add the examples to workspace
    print("Adding examples to workspace")
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    print("Commencing write")
    filename_out = filename.replace(".json","_hf.json")
    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"Write complete to {filename_out}")

def extract_intent_ranking(row: pandas.Series) -> pandas.Series:
    "Extract intent ranking"

    intent_ranking = row["parse_data-intent_ranking"]
    if isinstance(intent_ranking, float):
        row["second_intent_name"] = None,
        row["second_intent_confidence"]= None
        row["third_intent_name"] = None
        row["third_intent_confidence"] = None
        return row

    intent_ranking_len = len(intent_ranking)

    if intent_ranking_len > 1:
        row["second_intent_name"] = intent_ranking[1]["name"]
        row["second_intent_confidence"] = intent_ranking[1]["confidence"]
    if intent_ranking_len > 2:
        row["third_intent_name"] = intent_ranking[2]["name"]
        row["third_intent_confidence"]= intent_ranking[2]["confidence"]

    return row


def decide_role_filter_values(row: pandas.Series, column_name: str, role_filter: str, value_filter: str) -> bool:
    """Determine whether this is the 0,1,2 where the role is also somthing"""
    if row[column_name] == value_filter and row["role"] == role_filter:
        return True
    else:
        return False


def build_examples(row: pandas.Series, utterance_col: str, convo_id_col: str = '', created_at_col: str = ''):
    '''Build the examples'''

    # if utterances use the hash of the utterance for an id
    if convo_id_col == '':
        external_id = humanfirst.objects.hash_string(row[utterance_col], 'example')
        context = None

    # if convos use the convo id and sequence
    else:
        external_id = f'example-{row[convo_id_col]}-{row["idx"]}'
        context = humanfirst.objects.HFContext(
            context_id=row[convo_id_col],
            type='conversation',
            role=row["role"]
        )

    # created_at
    if created_at_col == '':
        created_at = datetime.datetime.now().isoformat()
    else:
        created_at = row[created_at_col]

    # build examples
    example = humanfirst.objects.HFExample(
        text=row[utterance_col],
        id=external_id,
        created_at=created_at,
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        context=context
    )
    row['example'] = example
    return row

def create_metadata(row: Union[pandas.Series, dict], metadata_keys_to_extract:
                    list, dict_of_values: dict = None) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    metadata = {}
    if not dict_of_values is None:
        assert isinstance(dict_of_values, dict)
        metadata = dict_of_values.copy()

    for key in metadata_keys_to_extract:
        metadata[key] = str(row[key])
    return metadata.copy()


def rename_role(role: str) -> None:
    """Change user to client and bot to expert"""

    if role == "user":
        return "client"
    elif role == "bot":
        return "expert"
    else:
        raise KeyError("Unknown role")


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
