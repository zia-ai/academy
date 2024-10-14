"""
python split_csv_to_multiple_parts.py

Takes a CSV and splits it into n parts
Currently has no distribution logic - could sample
Fix a date column if provided to isodate

"""
# *********************************************************************************************************************

# standard imports
from dateutil import parser

# third Party imports
import click
import numpy
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input file')
@click.option('-p', '--parts', type=int, required=False, default=2, 
              help='Number of parts to split to - default is 2')
@click.option('-d', '--date_col', type=str, required=False, default='', 
              help='Date col to fix to isodate')
@click.option('-t', '--text_col_clean_empty', type=str, required=False, default='', 
              help='Column name of text to clean empty')
def main(filename: str, parts: int, date_col: str, text_col_clean_empty: str) -> None:
    """Main Function"""

    # Read file
    assert filename.endswith(".csv")
    df = pandas.read_csv(filename,encoding="utf8")
    print(df)

    # fix date if necessary
    if date_col != '':
        df[date_col] = df[date_col].apply(fix_date)

    # fix empty if necessary
    if text_col_clean_empty != '':
        print(f'Before removing blanks: {df.shape}')
        df = df[~(df[text_col_clean_empty]=="")]
        print(f'After  removing blanks: {df.shape}')

    # TODO: Currently no sort order or sampling options
    # Should offer weighted sampling across a predicted intent maybe

    # divide large dataframe into multiple parts
    dfs = numpy.array_split(df, parts)

    # parse through every part of the divided dataframe
    for i,sub_df in enumerate(dfs):

        # make sure every sub dataframe is actually a dataframe
        assert isinstance(sub_df, pandas.DataFrame)

        # suffixing the output file path with the part_no
        output_path = filename.replace(".csv",f"_{i}.csv")
        assert output_path != filename

        # Write output
        sub_df.to_csv(output_path,index=False,header=True)
        print(f'Wrote to: {output_path}')

def fix_date(datestring: str) -> str:
    """Fix date"""
    return f'{parser.parse(datestring).isoformat()}Z'

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
