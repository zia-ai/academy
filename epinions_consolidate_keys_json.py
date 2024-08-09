"""
python epinions_consolidate

"""
# ******************************************************************************************************************120

# standard imports
import os
import json

# 3rd party imports
import click
import pandas

SCHEMA_KEYS = ["review_id","item_code","date_drafted","stars_rating", 
               "amount_paid","review","category","manufacturer","model","title"]

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=False,
              default='./data/epinions/epinions_data.json', help='Input File')
@click.option('-s', '--sample', type=int, required=False,
              default=0, help='Sampling ')
def main(filename: str, sample: int) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # list the files
    file_in = open(filename,mode="r",encoding="utf8")
    dict_in = json.load(file_in)
    file_in.close()
    df = pandas.json_normalize(dict_in["examples"])
    print(df)

    # work out valid invalid
    gb = df[["context.context_id","metadata.key"]].groupby("context.context_id",as_index=True).count()
    gb.rename(columns={"metadata.key":"count_metadata_keys"},inplace=True)

    # for each key transform the column
    for key in SCHEMA_KEYS:
        df_slice = df.loc[df["metadata.key"] == key,["context.context_id","text"]].copy()
        df_slice.rename(columns={"text":key},inplace=True)
        df_slice.set_index("context.context_id",inplace=True)
        gb = gb.join(df_slice)

    # Summarise which ones we lose
    print("Distribution of keys")
    print(gb[["count_metadata_keys","review_id"]].groupby("count_metadata_keys").count())

    # mark the ones we lose as invalid
    gb["valid"] = gb["count_metadata_keys"] == 10
    print(gb)

    gb = gb[gb["valid"] == True]
    print(gb)

    print("Categories")
    print(gb["category"].unique())
    print(gb[["category","review_id"]].groupby("category").count().sort_values("review_id",ascending=False))

    print("Manufacturers")
    print(gb["manufacturer"].unique())
    print(gb[["manufacturer","review_id"]].groupby("manufacturer").count())

    # output
    output_filename = filename.replace(".json","_output.csv")
    assert output_filename != filename
    gb.to_csv(output_filename,header=True,index=False)
    print(f'Wrote to: {output_filename}')

def validate_row(row: pandas.Series) -> pandas.Series:
    return row


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
