#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python deepgram_single_channel_csv_converter.py
# parse deepgram into csv
#
# *****************************************************************************

# standard imports
import json
import os
import datetime
import re

# third Party imports
import click
import pandas

@click.command()
@click.option('-d', '--directory', type=str, required=True, help='directory to convert')
@click.option('-s', '--sample', type=int, required=False, default=0, help='Only process X')
def main(directory: str, sample: int) -> None:
    """Main Function"""

    # check directory format
    if not directory.endswith("/"):
        directory = directory + "/"

    # check if output file already exists
    output_file_name = get_output_file_name(directory)
    if os.path.isfile(output_file_name):
        print(f"Removing pre-existing outputfile {output_file_name}")
        os.remove(output_file_name)
    assert isinstance(output_file_name,str)
    json_output_file_name = output_file_name.replace(".csv",".json")
    if os.path.isfile(json_output_file_name):
        print(f"Removing pre-existing converted outputfile {json_output_file_name}")
        os.remove(json_output_file_name)

    # read input
    file_list = os.listdir(directory)

    # process for every file concatentating into a single dataframe
    df = pandas.DataFrame()
    total_files = len(file_list)
    print(f'Total files {total_files}')
    for i,file_name in enumerate(file_list):
        if sample > 0 and i >= sample:
            break
        if file_name.endswith(".json"):
            file_uri = directory + file_name
            print(f'{i}/{total_files} Processing: {file_uri}')
            df = pandas.concat([df,convert_single_channel_deepgram(file_uri)],axis=0)
        else:
            print(f'{i}/{total_files} Skipped non json file: {file_name}')

    # write to source directory, with a name based on the input directory
    print(f'Writing to {output_file_name}')
    df.to_csv(output_file_name,index=False,header=True)


    # sort to preserve order
    df.sort_values(["filename","sequence_number"],ignore_index=True)

def get_output_file_name(directory: str) -> str:
    """Generate output name"""
    re_filename = re.compile(r'[\. \-\/]')
    output_file_name = re_filename.sub("_",directory)
    output_file_name = output_file_name.strip("_") + ".csv"
    output_file_name = directory + output_file_name
    return output_file_name

def convert_single_channel_deepgram(filename: str) -> pandas.DataFrame:
    """Read a single file and convert it to a df"""

    # if the filename doesn't have the time of the call all the property we only have offsets
    timestamp_base = datetime.datetime.now()

    # load this file
    file_obj = open(filename,mode="r",encoding="utf8")
    file_dict = json.load(file_obj)
    file_obj.close()

    # normalise just the 1 channel paragraphs
    df = pandas.json_normalize(
        file_dict["results"]["channels"][0]["alternatives"][0]["paragraphs"]["paragraphs"]
        , sep="-"
    )

    # expand out within each paragraph the sentances expanding the data to specific fields
    df = df.explode("sentences",ignore_index=True)
    df = df.apply(sentences_expand,axis=1)
    df.drop(columns=["sentences"],inplace=True)

    # work out the file name independent of endings.
    stripped_filename = filename.split("/")[-1].split(".json")[0]
    df["filename"] = stripped_filename

    # add additonal metadata from the deepgram return
    model_info = file_dict["metadata"]["model_info"][file_dict["metadata"]["models"][0]]
    df["model_name"] = model_info["name"]
    df["model_version"] = model_info["version"]
    df["channels"] = file_dict["metadata"]["channels"]
    df["created"] = file_dict["metadata"]["created"]
    df["duration"] = file_dict["metadata"]["duration"]

    # calculate an index for this dataframe as a field which will then preserve when concatenated with other convos
    # make sure it's not called idx for csv to unlabelled
    df['sequence_number'] = df.groupby(["filename"]).cumcount()

    # generate a simtestamp for this sentance for huamnfirst created_at
    df["timestamp"] = df["sentences-start"].apply(calculate_timestamp,args=[timestamp_base])

    return df

def calculate_timestamp(sentance_start: float, timestamp_base: datetime.datetime) -> datetime.datetime:
    "Add a delta based on the start time of the sentance to an arbitary passed base"
    return timestamp_base + datetime.timedelta(seconds=sentance_start)

def sentences_expand(row: pandas.Series) -> pandas.Series:
    """Example subobject"""
    row["sentences-text"] = row["sentences"]["text"]
    row["sentences-start"] = row["sentences"]["start"]
    row["sentences-end"] = row["sentences"]["end"]
    return row

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
