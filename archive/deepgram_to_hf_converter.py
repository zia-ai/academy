#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python deepgram_to_hf_converter.py -f <directory> -o <output_file_path>
#
# *****************************************************************************

# standard imports
import random
import json
import click
from os import listdir
from os.path import isfile, join
import uuid
import re
from datetime import datetime, timedelta, date
from dateutil import parser

# third Party imports
import pandas
import nltk
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

# custom imports
import humanfirst


@click.command()
@click.option('-f', '--filedir', type=str, required=True, help='Directory containing Deepgram transcribed files')
@click.option('-o', '--output', type=str, required=True, help='Filepath where the HF file should be produced')
@click.option('-s', '--split', is_flag=True, default=False, help='Split utterances into logical units')
def main(filedir: str, output: str, split: bool) -> None:
    """Main Function"""

    convo_list = []
    for f in listdir(filedir):
        if isfile(join(filedir, f)):
            filepath = join(filedir, f)
            with open(filepath, mode="r", encoding="utf-8") as convo_file:
                convo = json.load(convo_file)
                convo_list.append(preprocess_convo(convo))

    df = pandas.json_normalize(convo_list, sep="-")
    # print(df.columns)

    # explode the below columns
    col_list = ['results-utterances-utterance', 'results-utterances-start',
                'results-utterances-end', 'results-utterances-avg_confidence',
                'results-utterances-channel']
    df = df.explode(col_list, ignore_index=True)
    # print(df[col_list])

    # rename columns for ease of access
    rename_col = {
        'results-utterances-utterance': 'utterance',
        'results-utterances-start': 'start',
        'results-utterances-end': 'end',
        'results-utterances-avg_confidence': 'avg_confidence',
        'metadata-created': 'convo_transcribed_at',
        'metadata-duration': 'convo_duration'
    }
    df = df.rename(columns=rename_col)

    # assign role
    df['role'] = df['results-utterances-channel'].apply(assign_role)

    if split:
        # split the utterances into logical units
        df[["utterance", "start", "end"]] = df[["utterance", "start", "end"]].apply(split_utterance, axis=1)
        df = df.explode(["utterance", "start", "end"], ignore_index=True)
        # print(df[["utterance","start","end"]])

    df["convo_duration"] = df["convo_duration"].apply(lambda x: round(x, 3))
    df["avg_confidence"] = df["avg_confidence"].apply(lambda x: round(x, 3))
    df["start"] = df["start"].apply(lambda x: round(x, 3))
    df["end"] = df["end"].apply(lambda x: round(x, 3))

    # add start seconds to the created_at time
    df["created_at"] = df[["created_at", "start"]].apply(add_seconds, axis=1)

    # determining the first and last utterances of both client and expert
    df_client_expert = df.groupby(["role"])
    df_client = assign_utterance_num(df_client_expert, "client")
    print(df_client[["conversation_id", "created_at", "role", "utterance",
                     "is_first_utterance", "is_last_utterance"]])

    df_expert = assign_utterance_num(df_client_expert, "expert")
    print(df_expert[["conversation_id", "created_at", "role", "utterance",
                     "is_first_utterance", "is_last_utterance"]])

    df = pandas.concat([df_client, df_expert])

    # Extract metadata keys and store the corresponding items in metadata column in dataframe
    metadata_keys_to_extract = ["start", "end", "avg_confidence", "convo_transcribed_at",
                                "is_first_utterance", "is_last_utterance", "convo_duration"]
    df["metadata"] = df.apply(create_metadata, args=[metadata_keys_to_extract], axis=1)

    df = df.sort_values(["conversation_id", "created_at"]).reset_index(drop=True)
    df["idx"] = df.groupby(["conversation_id"]).cumcount()
    df = df.set_index(["conversation_id", "idx"])

    print(df[["created_at", "role", "utterance", "is_first_utterance", "is_last_utterance"]])

    # build examples
    df = df.apply(build_examples, axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.objects.HFWorkspace()
    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    file_out = open(output, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"{output} is successfully created")


def assign_utterance_num(df_client_expert: pandas.DataFrame, role: str) -> tuple:
    """Assigns the sentence number to the utterances"""

    df = df_client_expert.get_group(role)
    df = df.sort_values(["conversation_id", "created_at"]).reset_index(drop=True)
    df["sentence_num"] = df.groupby(["conversation_id"]).cumcount()
    no_of_utterances_conversation_wise = (df.groupby(["conversation_id"]).size()).to_dict()

    df[["is_first_utterance",
        "is_last_utterance"]] = df[["conversation_id",
                                    "sentence_num",
                                    "role"]].apply(set_first_and_last_utterance,
                                                   args=[no_of_utterances_conversation_wise, role],
                                                   axis=1)

    return df


def set_first_and_last_utterance(row: pandas.Series, no_of_utterances: dict, role: str) -> pandas.Series:
    """Decides if a sentence is a first sentence or last_sentence or neither"""

    is_first_utterance = False
    is_last_utterance = False
    for convo_id, size in no_of_utterances.items():
        if convo_id == row.conversation_id and row.sentence_num == 0 and row.role == role:
            is_first_utterance = True
        if convo_id == row.conversation_id and row.sentence_num == size - 1 and row.role == role:
            is_last_utterance = True
    row = pandas.Series(data=[is_first_utterance, is_last_utterance],
                        index=["is_first_utterance",
                               "is_last_utterance"])
    return row


def split_utterance(row: pandas.Series) -> pandas.Series:
    """Split the utterances into logical units"""

    pt = nltk.tokenize.PunktSentenceTokenizer()
    list_sentences = pt.tokenize(row["utterance"])
    no_of_chars = 0
    for sentence in list_sentences:
        no_of_chars = no_of_chars + len(sentence)

    # calculate individual logical unit level start and end
    seconds_per_char = (row["end"] - row["start"]) / no_of_chars
    start = [row["start"]]
    end = []
    for i in range(len(list_sentences)):
        end.append(start[i] + (seconds_per_char * len(list_sentences[i])))
        if i + 1 >= len(list_sentences):
            break
        start.append(end[i])
    row["utterance"] = list_sentences
    row["start"] = start
    row["end"] = end
    return row


def preprocess_convo(convo: dict) -> dict:
    """Modify the struture of the data in order to get the entire utterance and its corresponding metadata"""

    count = 0

    # get random year,month,day
    rand_year, rand_month, rand_day = get_rand_date()
    convo["created_at"] = datetime(rand_year, rand_month, rand_day, 0, 0, 0).isoformat()

    # get values of each property and put that in a list
    # example - "start": [1,2,3,4,....], "end": [7,8,9,.....], ...
    utterances = convo["results"]["utterances"]
    u_dict = {}
    for key in utterances[0].keys():
        u_dict[key] = []
    for utterance in utterances:
        for key in u_dict:
            u_dict[key].append(utterance[key])
        count = count + 1

    # solves call overlap issue with user and client
    for i in range(len(u_dict["start"]) - 1):
        overlap = u_dict["end"][i] - u_dict["start"][i + 1]
        u_dict["end"][i] = u_dict["end"][i] - overlap / 2
        u_dict["start"][i + 1] = u_dict["start"][i + 1] + overlap / 2

    # merging the sentences that belong to a soingle utternace and storing their corresponding metadata
    i = 0
    convo_dict = {
        "utterance": [],
        "start": [],
        "end": [],
        "avg_confidence": [],
        "channel": []
    }
    start_utterance, end_utterance, transcript, prev_channel, confidence, no_of_parts = assign_initial_utterance_values(u_dict, i)
    i = i + 1
    # Example - In a list of channel number 0,0,1,1,0,0,0,1 has 4 utterances
    # first 2 0's belong to a single utterance, next two 1's belong to a single utterance,
    # next 3 0's belong to a utterance and final 1 belong to a utterance
    while (i < count):
        current_channel = u_dict["channel"][i]
        while (prev_channel == current_channel):
            end_utterance = u_dict["end"][i]
            transcript = transcript + " " + u_dict["transcript"][i]
            confidence = confidence + u_dict["confidence"][i]
            no_of_parts = no_of_parts + 1
            prev_channel = current_channel
            i = i + 1
            if (i < count):
                current_channel = u_dict["channel"][i]
            else:
                break
        convo_dict = append_final_utterance_values(convo_dict, transcript, start_utterance,
                                                   end_utterance, confidence, no_of_parts, prev_channel)
        if (i < count):
            start_utterance, end_utterance, transcript, prev_channel, confidence, no_of_parts = assign_initial_utterance_values(u_dict, i)
        else:
            break
        i = i + 1
    else:
        convo_dict = append_final_utterance_values(convo_dict, transcript, start_utterance,
                                                   end_utterance, confidence, no_of_parts, prev_channel)
    convo["results"]["utterances"] = convo_dict

    # creating a conversation id for a conversation
    convo["conversation_id"] = f"convo-{uuid.uuid4()}"
    return convo


def append_final_utterance_values(convo_dict: dict, transcript, start_utterance, end_utterance, confidence, no_of_parts, prev_channel) -> dict:
    """Helper function for preprocess_convo - appends the final utterance and its corresponding metadata"""

    convo_dict["utterance"].append(transcript)
    convo_dict["start"].append(start_utterance)
    convo_dict["end"].append(end_utterance)
    convo_dict["avg_confidence"].append(round((confidence / no_of_parts), 4))
    convo_dict["channel"].append(prev_channel)
    return convo_dict


def assign_initial_utterance_values(u_dict: dict, i: int) -> dict:
    """Helper function for preprocess_convo - assign inital values for a utterance"""

    start_utterance = u_dict["start"][i]
    end_utterance = u_dict["end"][i]
    transcript = u_dict["transcript"][i]
    prev_channel = u_dict["channel"][i]
    confidence = u_dict["confidence"][i]
    no_of_parts = 1
    return start_utterance, end_utterance, transcript, prev_channel, confidence, no_of_parts


def get_rand_date() -> tuple:
    """Generates random date from the year 2022"""

    year_now, month_now, day_now = str(date.today()).split("-")
    rand_year = random.randint(2022, int(year_now))
    rand_month = random.randint(1, int(month_now))
    rand_day = random.randint(1, int(day_now))
    return rand_year, rand_month, rand_day


def add_seconds(row: pandas.Series) -> str:
    '''Add seconds to the created_at property'''

    new_date = (parser.parse(row["created_at"]) + timedelta(seconds=row["start"])).isoformat()
    return new_date


def build_examples(row: pandas.Series) -> pandas.Series:
    '''Build the examples'''

    example_id = re.sub("^convo-", "", row.name[0])
    # build examples
    example = humanfirst.objects.HFExample(
        id=f"example-{example_id}-{row.name[1]}",
        text=row['utterance'],
        created_at=row['created_at'],
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata=row['metadata'],
        # this links the individual utterances into their conversation
        context=humanfirst.objects.HFContext(
            str(row.name[0]),  # any ID can be used recommend a hash of the text which is repeatable or the external conversation id if there is one.
            'conversation',  # the type of document
            row["role"]  # the speakers role in the conversations
        )
    )
    row['example'] = example
    return row


def create_metadata(row: pandas.Series, metadata_keys_to_extract: list) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    metadata = {}  # metadata is a simple dict object
    for key in metadata_keys_to_extract:
        if isinstance(row[key], list):
            # ensures empty cells are not added to metadata
            # this prevents the conflict that arises due to the presence of properties with similar semantics
            if not pandas.isna(row[key]).any():
                metadata[key] = ','.join(row[key])
        else:
            # ensures empty cells are not added to metadata
            # this prevents the conflict that arises due to the presence of properties with similar semantics
            if not pandas.isna(row[key]):
                metadata[key] = str(row[key])

    # all key value pairs must be strings
    for key in metadata.keys():
        try:
            assert (isinstance(metadata[key], str))
        except Exception:
            print(f'Key: {key} value {metadata[key]} is not a string')

    return metadata


def assign_role(row: int) -> str:
    """Assign role depending on channel number 0-client/1-expert"""

    if row == 0:
        return "client"
    else:
        return "expert"


if __name__ == '__main__':
    main()
