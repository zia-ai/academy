"""
python csv_to_entites.py

accept in a UTF8 csv file and a delimiter and converts it to entities
your csv should be of the format
no headers
filename without .csv is entity name
col0 = key_value
col1 = onward are synonyms
for example this is the way Dialogflow ES exports an entity

"""
# *********************************************************************************************************************

# standard imports
import json
import os

# 3rd party imports
import pandas
import click


@click.command()
@click.option('-f', '--filename', type=str, required=True,
              help='Input File Path')
@click.option('-d', '--delimiter', type=str, required=False, default=",",
              help='Delimiter for the csv file')
@click.option('-l', '--language', type=str, required=False, default="en",
              help='Language of entities default: en')
def main(filename: str,
         delimiter: str,
         language:str
         ) -> None:
    """Main Function"""

    # read the input csv with columns 0,1,2 etc
    assert filename.endswith(".csv")
    df = pandas.read_csv(filename, encoding='utf8',header=None, delimiter=delimiter)

    # assert
    assert df.shape[0] >= 1
    assert df.shape[1] >= 2

    # values
    values = []

    # check if column containing keys is having only unique values
    assert df[0].is_unique

    # iterate through dataframe
    for i in range(df.shape[0]):

        # work out how many synonyms we have (2nd onward until NaN)
        synonyms = []
        for j in range(1,df.shape[1],1):
            # exit when come to first NaN value
            if pandas.isna(df.loc[i,j]):
                break
            synonym = {
                "value": df.loc[i,j]
            }
            synonyms.append(synonym.copy())

        # add a value to the entity with those synonyms (colunn 1)
        value = {
            "id":f'entity-value-{df.loc[i,0]}',
            "key_value": df.loc[i,0],
            "language": language,
            "synonyms": synonyms
        }
        values.append(value.copy())

    # create the entity add the values
    entity_name = os.path.basename(filename).replace(".csv","")
    entity = {
        "id": f'entity-{entity_name}',
        "name": entity_name,
        "values": values
    }

    # add all entities to workspace as a list
    workspace = {
        "$schema": "https://docs.humanfirst.ai/hf-json-schema.json",
        "entities": [entity]
    }

    # write to json
    output_filename = filename.replace(".csv",".json")
    assert output_filename != filename

    print(df)
    print(json.dumps(workspace,indent=2))

    with open(output_filename,mode="w",encoding="utf8") as output_file:
        json.dump(workspace,output_file,indent=2)
        print(f"Wrote to: {output_filename}")

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
