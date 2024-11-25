# pylint: disable=invalid-name
"""
python ./multidim_data_generation.py
       -f <folder>

Takes a folder of humanfirst json and breaks them down.

Erm: split_hf_unlabelled_into_multiple_parts.py

"""
# *********************************************************************************************************************
# standard imports
import os
import re

# third party imports
import click
import pandas
import json

# custom imports

@click.command()
@click.option('-f', '--input_folder', type=str, required=True, help='Input Folder with HumanFirst JSON in')
@click.option('-p', '--prefix', type=str, required=False, default="abcd", help='Prefix for output')
def main(input_folder: str, prefix: str):
    
    # get regex
    re_output_format = get_file_format_regex(prefix)

    # Read inputs
    assert os.path.isdir(input_folder)
    list_files = os.listdir(input_folder)
    json_files = []
    for f in list_files:
        assert isinstance(f,str)
        # skip it if in output format
        if f.endswith(".json"):
            if not re_output_format.match(f):
                file_in = open(os.path.join(input_folder,f),mode="r",encoding="utf8")
                json_files.append(json.load(file_in))
                file_in.close()
            else:
                logit("Skipping output",f)
    logit("Read number json_files is",len(json_files))
    
    # Join together - assumes only examples section important
    examples = []
    for f in json_files:
        examples.extend(f["examples"])
    del json_files
    logit("Examples number is",len(examples))
    
    # Make a data frame for some stats and get an index
    df = pandas.json_normalize(examples)
    list_contextids = list(df["context.context_id"].unique())
    logit("Total number of conversations is",len(list_contextids))
    
    # hmm here we need some sort date ordering across the months
    # created_at is in iso format
    # add it to metdata
    df["metadata.date_of_convo"] = df["created_at"].str[0:10]
    
    # Data looks like this
    print(df[["metadata.date_of_convo","context.context_id","id"]].groupby(["metadata.date_of_convo","context.context_id"]).count())
    
    # unique days
    unique_days = list(df["metadata.date_of_convo"].unique())
    logit("Unique days",len(unique_days))
    
    # loop through and build batches
    for d in unique_days:
        
        # slice the day
        df_day = df[df["metadata.date_of_convo"]==d]
        assert isinstance(df_day,pandas.DataFrame)
        df_day = df_day.copy(deep=True)
        
        # make up a filename kebab case to match date format
        filename = f'{prefix}-{d}.json'
        filename = os.path.join(input_folder,filename)
        
        # turn it back into hf json
        json_output = denormalize_to_hf_json(df_day,delimiter=".")
        
        with open(filename,mode="w",encoding="utf8") as file_out:
            json.dump(json_output,file_out)
        

def denormalize_to_hf_json(df:pandas.DataFrame,delimiter) -> dict:
    """Takes a dataframe of extracted from unlabelled examples
    Puts i"""
    # There is an old function frot his in academy  back_to_hf_unlabelled.back_to_hf(df_day,file_output=filename)
    # but I do not like it.  I think this is more readable
    delimiter="."
    all_cols = df.columns.to_list()
    metadata_cols = []
    context_cols = []
    other_cols = []
    for c in all_cols:
        if c.startswith("metadata"):
            metadata_cols.append(c)
        elif c.startswith("context"):
            context_cols.append(c)
        else:
            other_cols.append(c)
    df_day_output = df[other_cols].copy(deep=True)
    df_day_output["metadata"] = df[metadata_cols].apply(make_object,args=["metadata",delimiter],axis=1).copy(deep=True)
    df_day_output["context"] = df[context_cols].apply(make_object,args=["context",delimiter],axis=1).copy(deep=True)
    json_output = {
            "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
            "examples":df_day_output.to_dict(orient="records")
    }
    return json_output

        

def make_object(row: pandas.Series, object_name: str, delimiter: str) -> dict:
    obj = {}
    for k in row.keys().to_list():
        assert isinstance(k, str)
        obj[k.split(f'{object_name}{delimiter}')[-1]] = row[k]
    return obj

def logit(log_string: str, value: str, separator: str = ":"):
    """Nicely aligned logging """
    log_string = log_string + separator
    print(f'{log_string:<40} {value}')
    
def get_file_format_regex(prefix: str) -> re.Pattern:
    """fileoutput format"""
    return re.compile(prefix + "-[0-9]{,4}-[0-9]{,2}-[0-9]{,2}.json")

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
    