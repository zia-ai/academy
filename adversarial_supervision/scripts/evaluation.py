#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./adversarial_supervision\
#         /scripts\
#         /evaluation.py                                                                 # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# Core imports
import uuid
from os.path import join, exists
from multiprocessing import Pool
from datetime import datetime
import os

# 3rd party imports
import pandas
import numpy
import click
import requests

class UnrecognisedEnvironmentException(Exception):
    """This happens when entered environmenis neither dev nor prod"""

class UnscuccessfulAPICallException(Exception):
    """This happens when an API call goes unsuccessful"""

@click.command()
@click.option('-f', '--folder_path', type=str, default="./adversarial_supervision/dataset",
              help='file containing adversarial examples')
@click.option('-u','--username' ,type=str,required=True,help='username of HTTP endpoint in Node-RED')
@click.option('-p','--password' ,type=str,required=True,help='password of HTTP endpoint in Node-RED')
@click.option('-e','--env' ,type=click.Choice(['dev', 'prod']),default='dev',help='Dev or prod to update')
@click.option('-n', '--num_cores', type=int, default=2, help='Number of cores for parallelisation')
@click.option('-s','--sample',type=int,default=4,help='n text to sample from dataset')
def main(folder_path: str, username: str, password: str, env:str, num_cores:int, sample: int) -> None:
    '''Main Function'''

    process(folder_path, username, password, env, num_cores, sample)


def process(folder_path: str, username: str, password: str, env:str, num_cores:int, sample: int) -> None:
    '''Evaluate adversarial examples'''

    reply_folder_path = "./adversarial_supervision/replies"

    # combine both train and test data into single dataframe
    train_filepath = join(folder_path,"train.parquet")
    test_filepath = join(folder_path,"test.parquet")

    df_train = pandas.read_parquet(train_filepath)
    df_test = pandas.read_parquet(test_filepath)

    df = pandas.concat([df_train,df_test],ignore_index=True)

    print(df.columns)
    print(df.shape)
    print(df)
    print(df.groupby("label").count())

    if env == 'dev':
        url = "https://elb.devvending.com/api/predict"
    elif env == 'prod':
        url = "https://elb.cwrtvending.com/api/predict"
    else:
        raise UnrecognisedEnvironmentException('Unrecognised environment')
    print(f'Attempting to update: {url}')

    # set the api
    df["url"] = url

    # set the GUID
    # GUIDs are read from a text file if it was already present otherwise it creates them and
    # writes it into a text file. This is because it helps in comparing the results of utterances
    # in multiple runs
    guid_filepath = join(folder_path,"guid.txt")

    if exists(guid_filepath):
        with open(guid_filepath,mode="r",encoding="utf8") as f:
            guid_list = f.read()
            guid_list = guid_list.strip().split("\n")
            if df.shape[0] != len(guid_list):
                for _ in range(abs(df.shape[0] - len(guid_list))):
                    guid_list.append(f"{uuid.uuid4()}")
        df["id"] = guid_list
    else:
        df["id"] = df.apply(set_guid,axis=1)

    guid_list = df["id"].unique().tolist()
    with open(guid_filepath, mode="w", encoding="utf8") as f:
        f.write("\n".join(guid_list))

    # strip if text has any whitespaces prefixed or suffixed
    df["text"] = df["text"].apply(lambda x: x.strip())

    # set username and password
    df["username"] = username
    df["password"] = password

    # reply path
    df["reply_path"] = df["id"].apply(lambda x: join(reply_folder_path,f"{x}.txt"))

    # get all the completed files
    completed_text_ids, completed_texts = get_completed_text_ids(reply_folder_path)
    df.set_index("id",inplace=True, drop=True)

    uncompleted_text_ids = list(set(guid_list) - set(completed_text_ids))
    # print(completed_text_ids)
    uncompleted_df = df.loc[uncompleted_text_ids]
    completed_df = df.loc[completed_text_ids]
    completed_df["response"] = completed_texts

    # print(df.shape[0])

    # sample n number of rows from dataset
    uncompleted_df = uncompleted_df if sample > uncompleted_df.shape[0] else uncompleted_df.sample(sample)

    print(df[["text","label"]])

    # parallelization
    pool = Pool(num_cores)
    dfs = numpy.array_split(uncompleted_df, num_cores)
    pool_results = pool.map(parallelise_calls, dfs)
    pool.close()
    pool.join()
    uncompleted_df = pandas.concat(pool_results)

    df = pandas.concat([completed_df,uncompleted_df])
    df.drop(columns=["username","password"],inplace=True)
    print(df)

    df.to_csv(join(folder_path,f"final_result_{datetime.now().isoformat()}.csv"),sep=",",index=True)

    # TODO - to check if all responses have been received


def get_completed_text_ids(output_file_path: str) -> pandas.DataFrame:
    '''Find ids that have already been created'''

    file_names = os.listdir(output_file_path)
    completed_texts = []
    completed_ids = []
    for file_name in file_names:
        if file_name.endswith(".txt"):
            with open(join(output_file_path,file_name), mode="r", encoding="utf8") as f:
                text = f.read().strip()
                if text != "":
                    completed_ids.append(file_name[0:-4])
                    completed_texts.append(text)
    return completed_ids, completed_texts


def parallelise_calls(df: pandas.DataFrame) -> pandas.DataFrame:
    '''Parallelise dataframe processing'''

    return df.apply(send_text, axis=1)


def send_text(row: pandas.Series) -> pandas.Series:
    """Send text to Charlie"""

    data = {
        "id": row.name,
        "text": row.text
    }

    response  = requests.post(url=row.url, # pylint: disable=missing-timeout
                              auth=(row.username,row.password),
                              json=data)

    try:
        if response.status_code != 200:
            raise UnscuccessfulAPICallException(
                f"Status Code :{response.status_code} \n\nResponse:\n\n{response.json()}")

        # Writing to text file
        with open(row["reply_path"],mode="w",encoding="utf-8") as f:
            f.write(response.text)
        row["response"] = response.text

    except UnscuccessfulAPICallException as e:
        print(f"Rerunning {row.name} due to {e}")
        row = send_text(row) # rerun the text

    return row


def set_guid(_: pandas.Series) -> str:
    """Sets the GUID for a text"""

    return str(uuid.uuid4())


if __name__=="__main__":
    main() # pylint: disable=no-value-for-parameter
