#!/usr/bin/env python
# -*- coding: utf-8 -*-
# *******************************************************************************************************************120
#  Code Language:   python
#  Script:          dataset_info.py
#  Imports:         click, requests, pandas, humanfirst_apis
#  Functions:       main(), get_conversion_set_list()
#  Description:     Produces a CSV contaning dataset information
#
# **********************************************************************************************************************
   
# third party imports
import requests
import click
import pandas

# custom import 
import humanfirst_apis

@click.command()
@click.option('-u', '--username', type=str, required=True, help='HumanFirst username')
@click.option('-p', '--password', type=str, required=True, help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-o', '--output_path', type=str, required=True, help='Output CSV Path')
def main(username: str, password: str, namespace: str, output_path: str) -> None:
    """Main function"""

    headers = humanfirst_apis.process_auth(username=username,password=password)
    conversation_set_list = get_conversion_set_list(headers, namespace)
    df = pandas.json_normalize(data=conversation_set_list,sep="-")
    df.rename(columns={"id":"conversation_set_id"},inplace=True)
    df["conversation_source_id"] = df["sources"].apply(lambda x: x[0]["conversationSourceId"] if not pandas.isna(x) else x)

    df.drop(columns=["sources"],inplace=True)

    df.to_csv(output_path,encoding="utf-8",sep=",",index=False)
    print(df)
    print(f"CSV is stored at {output_path}")

def get_conversion_set_list(headers: str, namespace: str) -> tuple:
    """Conversation set list"""

    payload={}
    url = f"https://api.humanfirst.ai/v1alpha1/conversation_sets?namespace={namespace}"
    response = requests.request("GET", url, headers=headers,data=payload)
    if response.status_code != 200:
        print(f"Got {response.status_code} Response\n URL - {url}")
        quit()
    conversation_sets = response.json()['conversationSets']
    conversation_set_list = []
    for conversation_set in conversation_sets:
        conversation_set_id = conversation_set['id']
    
        url = f"https://api.humanfirst.ai/v1alpha1/conversation_sets/{namespace}/{conversation_set_id}"
        response = requests.request("GET", url, headers=headers,data=payload)
        if response.status_code != 200:
            print(f"Got {response.status_code} Responsen\n URL - {url}")
            quit()
        conversation_set = response.json()

        if ("state" in conversation_set.keys()):
            conversation_set["no_data_file_is_uploaded_since_creation"] = False
            if ("jobsStatus" in conversation_set["state"].keys()) and ("jobs" in conversation_set["state"]["jobsStatus"].keys()):
                jobs_dict = {}
                jobs = conversation_set["state"]["jobsStatus"]["jobs"]
                for i in range(len(jobs)):
                    if jobs[i]["name"] in ["merged","filtered","indexed","embedded"]:
                        jobs_dict[jobs[i]["name"]] = jobs[i]
                        del jobs_dict[jobs[i]["name"]]["name"]
                conversation_set["is_datafolder_empty"] = False
                conversation_set["state"]["jobsStatus"]["jobs"] = jobs_dict
            else:
                conversation_set["is_datafolder_empty"] = True
        else:
            conversation_set["is_datafolder_empty"] = True
            conversation_set["no_data_file_is_uploaded_since_creation"] = True
        conversation_set_list.append(conversation_set)

    return conversation_set_list

if __name__ == "__main__":
    main()