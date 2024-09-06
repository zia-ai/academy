"""
python extract_cols.py

"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import pandas
import numpy

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-i', '--index_cols', type=str, required=True, help='Comma Delimited string of columns withotu metadata reason_run_16')
@click.option('-c', '--cols', type=str, required=True, help='Comma Delimited string of columns withotu metadata reason_run_16')

def main(filename: str, cols: str, index_cols: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # get the metadata cols
    cols = cols.split(",")
    assert len(cols) >= 1
    for i,col in enumerate(cols):
        cols[i] = f'metadata.{col}'.strip()
    print(cols)

    # get the index cols
    index_cols = index_cols.split(",")
    assert len(index_cols) >= 1
    for i,index_col in enumerate(index_cols):
        index_cols[i] = f'metadata.{index_col}'.strip()

    # read the df
    file_in = open(filename,mode="r",encoding="utf8")
    dict_in = json.load(file_in)["examples"]
    file_in.close()
    df = pandas.json_normalize(dict_in)
    assert isinstance(df,pandas.DataFrame)

    # set blanks to blank 
    df = df.fillna("")

    # check cols
    print("Columns are")
    print(df.columns.to_list())

    # work out blanks
    df = df.apply(these_are_not_blank,args=[cols],axis=1)

    # group by to summarise
    gb = df[["is_data","id"]].groupby("is_data").count()
    print(gb)

    # take non blanks
    df = df[df["is_data"] == True]
    print(df)

    # keep only the wanted cols
    all_cols = index_cols + cols
    df = df[all_cols]
    print(df)

    # write output
    output_filename = filename.replace(".json","_output.csv")
    assert filename != output_filename
    df.to_csv(output_filename,index=False,header=True)
    print(f'Wrote to output: {output_filename}')

def these_are_not_blank(row:pandas.Series, cols: list) -> pandas.Series:
    row["is_data"] = False
    for col in cols:
        if row[col] != "":
            row["is_data"] = True
            break
    return row

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
