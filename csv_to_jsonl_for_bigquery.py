"""
python sort_escaping.py

"""
# ******************************************************************************************************************120

# standard imports
import csv
import json
import jsonlines
from dateutil import parser

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-d', '--date_col', type=str, required=False, default="", 
              help='Optional date_col to fix to isoformat')
@click.option('-t', '--timezone', type=str, required=False, default="", 
              help='Optional Time Zone for Dates')
def main(filename: str, date_col: str, timezone: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # Read the CSV
    df = pandas.read_csv(filename,dtype=str)

    # Fill NA with blank strings
    df.fillna(value="",inplace=True)

    # Fix the date if required with optional timezone
    if date_col != "":
        df[date_col] = df[date_col].apply(format_date, args=[timezone])
    print(df)

    # List the columns
    cols = df.columns.to_list()
    print(cols)

    # build the objects
    json_out = []
    for i,row in df.iterrows():
        row_obj = {}
        for c in cols:
            row_obj[c] = row[c]
        json_out.append(row_obj)
    
    # Write to JSONL
    output_filename = filename.replace(".csv","_jsonlified.json")
    assert filename != output_filename
    df.to_csv(output_filename,header=True,index=False,quoting=csv.QUOTE_ALL)
    with open(output_filename,encoding="utf8",mode="w") as file_out:
        jsonlines.Writer(file_out).write_all(json_out)
        print(f'Wrote to: {output_filename}')
    
def format_date(datestring: str, timezone: str = "") -> str:
    """Format a datestring to an isoformat with optional timezone"""
    return f'{parser.parse(datestring).isoformat()}{timezone}'

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
