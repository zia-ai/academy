#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python ./summarize/process_summaries.py
# 
# This script 
#  - reads all the summaries
#  - process all the summaries
#  - converts the summaries to HF JSON format
# 
# This script is executed only after executing 
# summarize_transcripts.py / summarize_long_transcripts.py scripts.
# 
# Accepts
#  - Directory containing all the summaries text file
#  - Number of cores to use for parallelization(optional)
#  - File path of server log (optional)
# 
# Parallelization of API calls helps to summarize large number of transcripts
#   
# Saves the summary of each transcript, along with the summaries after 
# processing each segment as an individual text file.
#
# *****************************************************************************

# standard imports
import json
import ast
import re
from os.path import exists
from datetime import datetime
from datetime import timedelta
import os
import logging
from os.path import join
from pathlib import Path
import sys
from multiprocessing import Pool
import time
from time import perf_counter
START_TIME = perf_counter()

dir_path = os.path.dirname(os.path.realpath(__file__))
hf_module_path = str(Path(dir_path).parent)
sys.path.insert(1,hf_module_path)

# 3rd party imports
import pandas
import numpy
import click

# custome imports
import humanfirst

@click.command()
@click.option('-i','--input_dir',type=str,required=True,help='Directory containing all the summaries')
@click.option('-n','--num_cores',type=int,default=8,help='Number of cores for parallelisation')
@click.option('-s','--server',type=str,default='',help='Server log file path')
def main(input_dir: str, num_cores: int, server: str):
    '''Main Function'''
    process(input_dir, num_cores, server)

def process(input_dir: str, num_cores: int, server: str):
    '''Process Summaries'''
    
    if not exists(input_dir):
        print(f"The directory {input_dir} does not exists")
        quit()
    
    # logging config
    if server == "":
        server = join(input_dir,"server_process.log")

    logging.basicConfig(filename=server, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(process)d - %(levelname)s -- %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    
    logging.info(f"Logs are stored in {server}")
    
    filepaths = os.listdir(input_dir)
    summary_paths = []
    for filepath in filepaths:
        if "summary.txt" in filepath.split("_"):
            summary_paths.append(join(input_dir,filepath))

    # print(*summary_paths,sep="\n")
    # print(len(summary_paths))
    # quit()

    parallelization_input = numpy.array_split(summary_paths,num_cores)
    
    logging.info("Starting Parallelization")
    # parallelization
    with Pool(num_cores) as p:
        parallelization_output = p.map(process_summaries,parallelization_input)

    logging.info(f"Total number of conversations summarized is {len(summary_paths)}")

    key_reason_for_calling = []
    how_issue_resolved = []
    action_taken = []
    hindrance = []
    incorrect_format_id = []
    disability = []
    error_count = 0

    for output in parallelization_output:
        key_reason_for_calling.extend(output[0])
        action_taken.extend(output[1])
        hindrance.extend(output[2])
        disability.extend(output[3])
        how_issue_resolved.extend(output[4])
        incorrect_format_id.extend(output[5])
        error_count = error_count + output[6]
    
    incorrect_format_id = list(set(incorrect_format_id))
    incorrect_format_path = join(input_dir,"incorrect_format_convo_id.txt")
    with open(incorrect_format_path,mode="w",encoding="utf8") as f:
        f.write("\n".join(incorrect_format_id))

    df = {
        "issue": pandas.json_normalize(data=key_reason_for_calling),
        "resolution":pandas.json_normalize(data=how_issue_resolved),
        "action": pandas.json_normalize(data=action_taken),
        "hindrance": pandas.json_normalize(data=hindrance),
        "disability": pandas.json_normalize(data=disability)
    }

    df["action"] = df["action"].explode(["text","seq"])
    df["hindrance"] = df["hindrance"].explode(["text","seq"])
    df["disability"] = df["disability"].explode(["text","seq"])

    for key in df.keys():
        if key in ["issue","resolution"]:
            df[key].set_index(["id"],drop=True,inplace=True)
        else:
            df[key].set_index(["id","seq"],drop=True,inplace=True)

        df[key]["text"].fillna(value="None",inplace=True)

        # convert the summaries into unlabelled HF format
        unlabelled_workspace = humanfirst.HFWorkspace()        
        df[key] = df[key].apply(parse_utterances,axis=1,args=[unlabelled_workspace,key])

        output_filepath_csv = join(input_dir,f"summarized_{key}.csv")
        df[key].to_csv(output_filepath_csv,sep=",",encoding="utf8")
        logging.info(f'{key}- Unlabelled CSV is saved at {output_filepath_csv}')

        output_filepath_json = join(input_dir,f"summarized_{key}.json")
        with open(output_filepath_json, 'w', encoding='utf8') as file_out:
            unlabelled_workspace.write_json(file_out)
        logging.info(f'{key} - Unlabelled json is saved at {output_filepath_json}')
    logging.info(f"Number of exceptions raised is {error_count}")
    logging.info(f'Total Duration for the script: {time.strftime("%H:%M:%S", time.gmtime(perf_counter() - START_TIME))}')

def process_summaries(summary_paths: list) -> list:
    '''Process openai summaries'''

    key_reason_for_calling = []
    action_taken = []
    hindrance = []
    how_issue_resolved = []
    incorrect_format = []
    disability = []

    number_of__summaries = len(summary_paths)
    i=0
    error_count = 0
    while i<number_of__summaries:
        try:
            summary_path = summary_paths[i]
            index = summary_path.split("_")[-2]
            with open(summary_path, mode="r", encoding = "utf8") as f:
                summary = f.read()
                logging.info(f"Summary is read from {summary_path}")
                if summary == "":
                    logging.warning(f"Summary is empty in {summary_path}")
                    i = i + 1
                    continue

            # parsing through the summary(openai response) to get individual utterances
            summary1 = ensure_string_inside_list_is_quoted_and_closed(summary)
            summary1 = add_escape_char(summary1)
            summary = ast.literal_eval(summary1)
            key_reason_for_calling_dict = {}
            action_taken_dict = {}
            hindrance_dict = {}
            disability_dict = {}
            how_issue_resolved_dict = {}
            action_text = []
            action_seq = []
            hindrance_text = []
            hindrance_seq = []
            disability_text = []
            disability_seq = []

            flag = 0
            krc, hir, resolved, delivery_postcode, package_id, agent_name = "None", "None", "None", "None", "None", "None"
            
            for key,value in summary.items():
                key = key.strip()
                if isinstance(value,str):
                    value = value.strip()
                if key.find("key_reason_for_calling") != -1:
                    krc = value if (value !="" or value != None) else "None"
                elif key.find("actions_taken") != -1:
                    action_text.extend(value if (value != [] or value != [None]) else ["None"])
                    action_seq.extend([i for i in range(1,len(action_text)+1)])
                elif key.find("hindrances") != -1:
                    hindrance_text.extend(value if (value != [] or value != [None]) else ["None"])
                    hindrance_seq.extend([i for i in range(1,len(hindrance_text)+1)])
                elif key.find("disabilities") != -1:
                    disability_text.extend(value if (value != [] or value != [None]) else ["None"])
                    disability_seq.extend([i for i in range(1,len(disability_text)+1)])
                elif key.find("how_issue_resolved") != -1:
                    hir = value if (value !="" or value != None) else "None"
                elif key.find("whether_customer_issue_successfully_resolved") != -1:
                    resolved = value if (value !="" or value != None) else "None"
                elif key.find("delivery_postcode") != -1:
                    delivery_postcode = value if (value !="" or value != None) else "None"
                elif key.find("package_id") != -1:
                    package_id = value if (value !="" or value != None) else "None"
                elif key.find("agent_name") != -1:
                    agent_name = value if(value !="" or value != None) else "None"
                else:
                    flag = 1
                    logging.warning(f"Unknown key - {key} present in the summary of conversation id {index}")
                    break
            
            if flag == 1:
                logging.warning(f"Incorrect format conversation ID: {index}")
                print(f"Incorrect format conversation ID: {index}")
                i = i+1
                incorrect_format.append(index)
                continue

            key_reason_for_calling_dict.update({"id": index,
                                                "text": krc,
                                                "created_at": datetime.now(),
                                                "resolved": resolved,
                                                "delivery_postcode": delivery_postcode,
                                                "package_id": package_id,
                                                "agent_name": agent_name
                                                })
            key_reason_for_calling.append(key_reason_for_calling_dict)

            
            how_issue_resolved_dict.update({"id": index,
                                            "text": hir,
                                            "created_at": datetime.now(),
                                            "resolved": resolved,
                                            "delivery_postcode": delivery_postcode,
                                            "package_id": package_id,
                                            "agent_name": agent_name
                                            })
            how_issue_resolved.append(how_issue_resolved_dict)

            action_taken_dict.update({
                "id": index,
                "text": action_text,
                "seq": action_seq,
                "created_at": datetime.now(),
                "resolved": resolved,
                "delivery_postcode": delivery_postcode,
                "package_id": package_id,
                "agent_name":agent_name
            })
            action_taken.append(action_taken_dict)

            hindrance_dict.update({
                "id": index,
                "text": hindrance_text,
                "seq": hindrance_seq,
                "created_at": datetime.now(),
                "resolved": resolved,
                "delivery_postcode": delivery_postcode,
                "package_id": package_id,
                "agent_name": agent_name
            })
            hindrance.append(hindrance_dict)

            disability_dict.update({
                "id": index,
                "text": disability_text,
                "seq": disability_seq,
                "created_at": datetime.now(),
                "resolved": resolved,
                "delivery_postcode": delivery_postcode,
                "package_id": package_id,
                "agent_name": agent_name
            })
            disability.append(disability_dict)
        
        except Exception as e:
            logging.error(f"Conversation ID {index} - throws this exception{e}")
            print(summary,e)
            error_count = error_count + 1
        i = i + 1
    
    return [key_reason_for_calling, action_taken, hindrance, disability, how_issue_resolved, incorrect_format, error_count]

def ensure_string_inside_list_is_quoted_and_closed(text: str) -> str:
    '''Ensuring all the strings in list of strings are quoted'''

    text = text.split("\n")
    for i,_ in enumerate(text):
        text[i] = text[i].strip()
        if text[i].find("[") != -1 or text[i].find("]") != -1 or text[i].find(":") != -1 or text[i].find("{") != -1 or text[i].find("}") != -1:
            continue
        else:
            if text[i][0] == "'" and (text[i][-1] == "'" or (text[i][-2] == "'" and text[i][-1] == ",")):
                continue
            if text[i][0] == "'" and (text[i][-1] != "'" and (text[i][-2] != "'" and text[i][-1] != ",")):
                text[i] = text[i][1:]
            elif text[i][0] != "'":
                if text[i][-1] == "'" :
                    text[i] = text[i][0:-1]
                elif text[i][-2] == "'" and text[i][-1] == ",":
                    text[i] = text[i][0:-2]
                else: pass
            else:
                pass

            text[i] = f"'{text[i].split(',')[0]},'"
    text = "\n".join(text)

    if text[-1] != "}":
        if text[-1] == "'":
            text = text + "\n}"
        else:
            text = text + "'\n}"

    return text

def add_escape_char(text: str) -> str:
    '''Adds \ in pfront of apostrophes'''

    apostrophe = re.findall(r"([\w]+'[\w]+)",text)
    apostrophe.extend(re.findall(r"(\b[\w]+'[^\\n:]\b)",text))
    apostrophe.extend(re.findall(r"\b[\s|\.]'[\w]+\b",text))

    for word in apostrophe:
        org_word = word
        word = re.sub(r"'",r"\'",word)
        text = re.sub(org_word,word,text)

    return text

def parse_utterances(row: pandas.Series, unlabelled_workspace: humanfirst.HFWorkspace, key: str) -> None:
    '''parse a single utterance to an example'''

    row["resolved"] = re.sub(r'[^\w\s]', '', str(row["resolved"])).lower()
    row["delivery_postcode"] = re.sub(r'[^\w\s]', '', str(row["delivery_postcode"])).lower()
    row["package_id"] = re.sub(r'[^\w\s]', '', str(row["package_id"])).lower()
    row["agent_name"] = re.sub(r'[^\w\s]', '', str(row["agent_name"])).lower()

    if key in ["issue", "resolution"]:
        row["created_at"] = (row["created_at"]).isoformat()
        metadata = {
            "id": str(row.name),
            "resolved": row["resolved"],
            "delivery_postcode": row["delivery_postcode"],
            "package_id": row["package_id"],
            "agent_name": row["agent_name"]
        }
        example_id = f'example-{row.name}'
        conversation_id = row.name
    else:
        row["created_at"] = (row["created_at"] + timedelta(seconds=int(row.name[1]))).isoformat()
        metadata = {
            "id": str(row.name[0]),
            "seq": str(row.name[1]),
            "resolved": row["resolved"],
            "delivery_postcode": row["delivery_postcode"],
            "package_id": row["package_id"],
            "agent_name": row["agent_name"]
        }
        example_id = f'example-{row.name[0]}-{row.name[1]}'
        conversation_id = row.name[0]
    
    # Will load these as conversations where it is only the client speaking
    context = humanfirst.HFContext(conversation_id,'conversation','client')

    # Create the example
    example = humanfirst.HFExample(
        text=row['text'], 
        id=example_id, 
        created_at=row["created_at"],
        intents=[], 
        tags=[], 
        metadata=metadata, 
        context=context
    )

    # add to the unlabelled_workspace
    unlabelled_workspace.add_example(example)
    return row

if __name__=="__main__":
    main()