"""
python add_generated_data_as_metadata_ib2.py

Version that works with key values

Doesn't work for merge stash as there won't be source conversation id metadata in the result
"""
# ******************************************************************************************************************120

# standard imports
import json
import re

# 3rd party imports
import pandas
import click

# custom imports
import back_to_hf_unlabelled

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input HF JSON File Path')
@click.option('-g', '--generated_data', type=str, required=True, help='Generated data as a JSON or CSV')
@click.option('-d', '--delimiter', type=str, required=False, default='-',
              help='How will create metadata"X"sourceConversationId')
@click.option('-o', '--observation_fields', type=str, required=True,
              help='Comma delimitered list of columns to enrich data with')
@click.option('-c', '--cleanse_fields', type=str, required=True,
              help='Comma delimitered list of columns to cleanse')
@click.option('-p', '--drop_no_metadata_fields', is_flag=True, type=bool, required=False, default=False,
              help='Whether to drop data without metadata')
def main(filename: str,
         generated_data: str,
         delimiter: str,
         observation_fields: str,
         cleanse_fields: str,
         drop_no_metadata_fields: bool) -> None:
    """Main Function"""

    # load input data
    assert filename.endswith(".json")
    with open(filename, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-")
    print(f"Loaded data to annotate:              {filename}")
    print(f"Shape is:                             {df.shape}")
    if "context-context_id" not in df.columns:
        raise RuntimeError("Context id column is not present in the unlabelled dataset")
    print(f'Unique context-context_id:            {df["context-context_id"].nunique()}')

    # load generated data
    if generated_data.endswith(".json"):
        with open(generated_data, mode="r", encoding="utf8") as file:
            gen_data = json.load(file)
            df_gen = pandas.json_normalize(data=gen_data["examples"], sep="-")
    elif generated_data.endswith(".csv"):
        df_gen = pandas.read_csv(generated_data)
    else:
        raise RuntimeError("Generated data must be a csv or json")
    print(f"Loaded query result:                  {generated_data}")
    print(f"Shape is:                             {df_gen.shape}")

    # check for key join field - this is the field we will join the original data to the generated on
    join_field = f'metadata{delimiter}sourceConversationId'
    if join_field not in df_gen.columns.tolist():
        raise RuntimeError(f'{join_field} is not present in the generated dataset')
    print(f'Unique {join_field}: {df_gen[join_field].nunique()}')

    # drop
    if drop_no_metadata_fields:
        ids_we_need = df_gen[join_field].unique().tolist()
        df = df[df["context-context_id"].isin(ids_we_need)]
        print("After drop")
        print(f'Unique context-context_id:            {df["context-context_id"].nunique()}')

    # observation keys interested in
    observation_fields = observation_fields.split(',')

    # fields to cleanse
    cleanse_fields = cleanse_fields.split(',')
    for cleanse in cleanse_fields:
        df_gen[cleanse] = df_gen[cleanse].apply(cleanse_value)

    # just want the two observation fields
    df_gen.set_index(join_field,inplace=True,drop=True)
    df_gen = df_gen[observation_fields]

    # rename the observation fields
    for field in observation_fields:
        df_gen.rename(columns={field:f'metadata-{field}'},inplace=True)

    # join the new metadata on
    print("Joining Data")
    df = df.join(df_gen,on="context-context_id")
    # df["metadata-context_id"] = df["context-context_id"]
    df = df.fillna("")
    print("Joined Columns")
    print(df.columns.to_list())
    print(f'Final df has shape: {df.shape}')
    print(f'Unique context-context_ids in final is: {df["context-context_id"].nunique()}')

    # make a dict again
    workspace_dict = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "examples": back_to_hf_unlabelled.df_to_formatted_json(df,sep="-")
    }
    print(f'Len of examples is {len(workspace_dict["examples"])}')

    # write output
    output_path = filename.replace(".json","_gen_metadata.json")
    with open(output_path,mode="w",encoding="utf8") as file_out:
        json.dump(workspace_dict,file_out,indent=2)
        print(f'Wrote to: {output_path}')


def cleanse_value(text:str) -> str:
    text = text.lower()
    text = text.strip('.')
    return text

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
