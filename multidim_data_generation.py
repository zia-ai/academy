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
import back_to_hf_unlabelled

@click.command()
@click.option('-f', '--input_folder', type=str, required=True, help='Input Folder with HumanFirst JSON in')
@click.option('-p', '--prefix', type=str, required=False, default="abcd", help='Prefix for output')
def main(input_folder: str, prefix: str):
    
    # fileoutput format
    re_output_format = re.compile(prefix + "-[0-9]{,4}-[0-9]{,2}-[0-9]{,2}.json")

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
        
        # these are 
        back_to_hf_unlabelled.back_to_hf(df_day,file_output=filename)

def logit(log_string: str, value: str, separator: str = ":"):
    """Nicely aligned logging """
    log_string = log_string + separator
    print(f'{log_string:<40} {value}')
    

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
    