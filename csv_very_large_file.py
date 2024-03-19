"""
python csv_to_json_very_large_file.py

"""
# *********************************************************************************************************************
import click
import csv
import json

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-l', '--limit', type=str, required=True, help='Conversation Split Limit')
@click.option('-u', '--utterance_col', type=str, required=True,
              help='Column name containing utterances')
@click.option('-c', '--convo_id_col', type=str, required=False, default='',
              help='If conversations which is the id otherwise utterances and defaults to hash of utterance_col')
@click.option('-t', '--created_at_col', type=str, required=False, default='',
              help='If there is a created date for utterance otherwise defaults to now')
@click.option('-x', '--unix_date', is_flag=True, type=bool, required=False, default=False,
              help='If created_at column is in unix epoch format')
@click.option('-r', '--role_col', type=str, required=False, default='',
              help='Which column the role in ')
@click.option('-p', '--role_mapper', type=str, required=False, default='',
              help='If role column then role mapper in format "source_client:client,source_expert:expert,*:expert"')
def main(filename: str):
    file_name = filename.split("/")[-1]
    file_name_split = file_name.split(".")[0]
    json_file_name = file_name_split + "-hf-export.json"
    convo_dict = combine_conversations(file_name)
    export_to_hf_json_limit(convo_dict,json_file_name,6000)


def combine_conversations(filename: str):
    csv_file = filename
    convo_dict = {}
    no_of_unique_conversations = 0
    no_of_utterances_total = 0
    error_log = []

    with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        for row_number, row in enumerate(csvreader,start=1):
            try:
                convo_id = row[0]
                current_speaker = row[2]
                current_utterance = row[3]
            except IndexError as e:
                error_log.append((row_number,row))
                continue

            
            if convo_id in convo_dict:
                #append to that text file
                current_convo = convo_dict[convo_id]

                #optional clean utterance of quotes
                current_utterance = current_utterance.strip('"')    

                #create string
                next_str = current_speaker + ": " + current_utterance + "\n"

                #append
                current_convo = current_convo + next_str

                #replace
                convo_dict[convo_id] = current_convo
                no_of_utterances_total += 1
            else:
                #adding first utterance
                next_str = current_speaker + ": " + current_utterance + "\n"
                convo_dict[convo_id] = next_str
                no_of_unique_conversations += 1
    
    print(f"Processed Conversations: {no_of_unique_conversations}\n")
    print(f"Processed Utterances: {no_of_utterances_total}\n")
    print("Error utterances found: " + str(len(error_log)))
    
    err_file_name = "Error Log--" + csv_file
    if(len(error_log) > 0):
        with open(err_file_name, 'w', newline='') as err_csv_file:
            writer = csv.writer(err_csv_file)
            writer.writerows(error_log)

    return convo_dict


def export_to_hf_json(conversation_dict, json_file):
    
    examples = []
    for conversation_id in conversation_dict:
        example = {
                "id": conversation_id,
                "text": conversation_dict[conversation_id],
            }
        examples.append(example)

    data = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": examples
    }

    with open(json_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=2)

def export_to_hf_json_limit(conversation_dict, json_file, limit):
    no_of_files = 0
    example_dict = {}
    conversation_counter = 0

    for conversation_id in conversation_dict:
        example = {
                "id": conversation_id,
                "text": conversation_dict[conversation_id],
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
        
        with open(changed_file, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2)



if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
