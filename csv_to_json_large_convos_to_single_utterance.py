"""
python csv_to_json_large_convos_to_single_utterance.py

Converts very large phone or chat conversations from csv format to HF JSON.
Stores entire conversations as a single utterance.
Splits large csv files to multiple JSON files to easily load data. (must specify limits if needed)

Requires CSV with Columns Headers for utterance column and convo id
"""
# *********************************************************************************************************************
# standard imports
import csv
import json
import time
import datetime
import os

#third-party imports
import click

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-l', '--limit', type=int, required=False, help='Conversation Split Limit Number (ex 6000). A new file \
                                                                will be created once limit is reached')
@click.option('-u', '--utterance_col', type=str, required=True,
              help='Column name containing utterances')
@click.option('-c', '--convo_id_col', type=str, required=True, default='',
              help='Column name containing convo id')
@click.option('-t', '--created_at_col', type=str, required=False, default='',
              help='If there is a created date for utterance otherwise defaults to now')
@click.option('-x', '--unix_date', is_flag=True, type=bool, required=False, default=False,
              help='Boolean to see if created_at column is in unix epoch format')
@click.option('-r', '--role_col', type=str, required=False, default='',
              help='Column name containing the role')
@click.option('-p', '--role_mapper', type=str, required=False, default='',
              help='If role column then role mapper in format "source_client:client,source_expert:expert,*:expert"')
def main(filename: str, limit: int, utterance_col: str,
         convo_id_col: str, created_at_col: str, unix_date: bool, role_col: str,
         role_mapper: str) -> None:

    file_name = filename
    #Check if filename is a path
    if "/" in file_name:
        file_name = filename.split("/")[-1]

    #Remove any .
    if "." in file_name:
        file_name_split = file_name.split(".")[0]
    else:
        file_name_split = file_name

    json_file_name = file_name_split + "-hf-export.json"

    convo_dict = combine_conversations(filename,utterance_col,convo_id_col,created_at_col,unix_date,
                                       role_col,role_mapper)

    if limit == 0 or limit == None:
        export_to_hf_json(convo_dict,json_file_name)
    else:
        export_to_hf_json_limit(convo_dict,json_file_name,limit=limit)



def combine_conversations(filename: str,utterance_col:str,convo_id_col:str,created_at_col:str,
                          unix_date:bool,role_col:str,role_mapper:str) -> dict:
    """Read each utterance and builds a single string for each conversation stored in a dictionary"""

    csv_file = filename
    convo_dict = {}
    no_of_unique_conversations = 0
    no_of_utterances_total = 0
    error_log = []
    date_error_log = []


    with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)

        #Need to get headers
        headers = next(csvreader)

        #Set up variables
        utterance_col_index = None
        convo_id_col_index = None
        created_at_col_index = None if created_at_col is None else headers.index(created_at_col)
        role_col_index = None if role_col is None else headers.index(role_col)
        role_mapper = None if role_mapper is None else role_mapper
        unix_date = time.now() if unix_date is None else unix_date



        #Grab utterance columns
        try:
            utterance_col_index = headers.index(utterance_col)
            convo_id_col_index = headers.index(convo_id_col)
        except Exception as e:
            print(f"Was unable to locate header. Error: {e}")


        for row_number, row in enumerate(csvreader,start=1):
            try:
                convo_id = row[convo_id_col_index]
                current_utterance = row[utterance_col_index]

                #Current speaker identify
                if role_col_index:
                    current_speaker = row[role_col_index]
                elif role_col_index == None and role_mapper == None:
                    current_speaker = "client"

                #Current speaker remapping
                if role_mapper and role_col_index:
                    current_speaker_arr = role_mapper.split(',')
                    remap_client_arr = current_speaker_arr[0].split(':')
                    remap_expert_arr = current_speaker_arr[1].split(':')
                    default_speaker = current_speaker_arr[2][2:]

                    if current_speaker == remap_client_arr[0]:
                        current_speaker = remap_client_arr[1]
                    elif current_speaker == remap_expert_arr[0]:
                        current_speaker = remap_expert_arr[1]
                    else:
                        current_speaker = default_speaker
            except IndexError as e:
                error_log.append((row_number,row))
                continue

            if convo_id in convo_dict:
                #append to that text file
                current_convo = convo_dict[convo_id][0]
                actual_date = convo_dict[convo_id][1]

                #optional clean utterance of quotes
                current_utterance = current_utterance.strip('"')

                #create string
                next_str = current_speaker + ": " + current_utterance + "\n"

                #append
                current_convo = current_convo + next_str

                convo_obj = [current_convo,actual_date]

                #replace
                convo_dict[convo_id] = convo_obj
                no_of_utterances_total += 1
            else:
                #adding first utterance
                next_str = current_speaker + ": " + current_utterance + "\n"

                #timestamps and date
                actual_date = None
                if unix_date:
                    try:
                        if row and created_at_col_index is not None and row[created_at_col_index]:
                            date = int(row[created_at_col_index])
                            actual_date = datetime.datetime.fromtimestamp(date).isoformat()

                        else:
                            actual_date = datetime.datetime.fromtimestamp(unix_date).isoformat()
                    except Exception as e:
                        #print(f"Error with date column at index {created_at_col_index}: {e}")
                        date_error_log.append(f"index {created_at_col_index}: {e}")
                        actual_date = datetime.datetime.now().isoformat()


                convo_obj = [next_str,actual_date]

                convo_dict[convo_id] = convo_obj
                no_of_unique_conversations += 1

    print(f"Processed Conversations: {no_of_unique_conversations}\n")
    print(f"Processed Utterances: {no_of_utterances_total}\n")
    print("Error utterances found: " + str(len(error_log)) + "\n")
    print("Date errors (current time was used for these): "+ str(len(date_error_log)))




    err_file_name = "Error Log--" + str(datetime.datetime.now().isoformat()) + ".csv"
    if(len(error_log) > 0):

        directory = "data"

        if not os.path.exists(directory):
            os.makedirs(directory)

        err_file_name = os.path.join(directory, err_file_name)


        with open(err_file_name, 'w', newline='') as err_csv_file:
            writer = csv.writer(err_csv_file)
            writer.writerows(error_log)


    return convo_dict


def export_to_hf_json(conversation_dict: dict, json_file:str) -> None:
    """Takes the dictionary and creates a HF JSON output file to the data folder - without limits version"""

    examples = []
    for conversation_id in conversation_dict:
        example = {
                "id": conversation_id,
                "text": conversation_dict[conversation_id][0],
                "metadata": {
                    "created_at":conversation_dict[conversation_id][1]
                }
            }
        examples.append(example)

    data = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": examples
    }

    directory = "data"

    if not os.path.exists(directory):
        os.makedirs(directory)

    changed_file = json_file.split('.')[0] + ".json"
    changed_file = os.path.join(directory, changed_file)

    with open(changed_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=2)

def export_to_hf_json_limit(conversation_dict: dict, json_file: str, limit:int) -> None:
    """Takes the dictionary and creates a HF JSON output file to the data folder - with limits version"""
    no_of_files = 0
    example_dict = {}
    conversation_counter = 0

    for conversation_id in conversation_dict:
        example = {
                "id": conversation_id,
                "text": conversation_dict[conversation_id][0],
                "metadata": {
                    "created_at":conversation_dict[conversation_id][1]
                }
            }

        if conversation_counter < limit:
            key = str(no_of_files)
            if key in example_dict:
                arr_ref = example_dict[key]
                arr_ref.append(example)
                example_dict[key] = arr_ref
            else:
                example_dict[key] = [example]
            conversation_counter+=1
        else:
            no_of_files += 1
            key = str(no_of_files)
            #append that new conversation
            example_dict[key] = [example]
            conversation_counter = 1

    #For loop to write multiple JSON files
    for example_id in example_dict:
        key = example_id
        changed_file = json_file.split('.')[0] + "-" + key + ".json"
        examples = example_dict[key]

        data = {
            "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
            "examples": examples
        }

        directory = "data"

        if not os.path.exists(directory):
            os.makedirs(directory)

        changed_file = os.path.join(directory, changed_file)

        with open(changed_file, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2)



if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
