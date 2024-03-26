"""
ftest.py
"""
# ******************************************************************************************************************120

# standard imports
import os

# 3rd party imports
import click
import pandas

# custom imports
import csv_to_json_unlabelled

@click.command()
@click.option('-f', '--filepath', type=str, required=True, help='Input the directory where files exist')
@click.option('-m', '--metadata_keys', type=str, required=False, default='',
              help='<metadata_col_1,metadata_col_2,...,metadata_col_n>')
@click.option('-u', '--utterance_col', type=str, required=True,
              help='Column name containing utterances')
@click.option('-d', '--delimiter', type=str, required=False, default=",",
              help='Delimiter for the csv file')
@click.option('-c', '--convo_id_col', type=str, required=False, default='',
              help='If conversations which is the id otherwise utterances and defaults to hash of utterance_col')
@click.option('-t', '--created_at_col', type=str, required=False, default='',
              help='If there is a created date for utterance otherwise defaults to now')
@click.option('-x', '--unix_date', is_flag=True, type=bool, required=False, default=False,
              help='If created_at column is in unix epoch format')
@click.option('-r', '--role_col', type=str, required=False, default='',
              help='Which column the role in ')
@click.option('-p', '--role_mapper', type=str, required=False, default='',
              help='If role column then role mapper in format "source_client:client,source_expert:expert,*:expert"')
@click.option('-e', '--encoding', type=str, required=False, default='utf8',
              help='Input CSV encoding')
@click.option('--filtering', type=str, required=False, default='',
              help='column:value,column:value;column:value,column:value')
@click.option('-h', '--striphtml', is_flag=True, default=False,
              help='Whether to strip html tags from the utterance col')
@click.option('-b', '--drop_blanks', is_flag=True, type=bool, default=False,
              help='Whether to drop blanks')
def main(filepath: str, metadata_keys: str, utterance_col: str, delimiter: str,
         convo_id_col: str, created_at_col: str, unix_date: bool, role_col: str,
         role_mapper: str, encoding: str, filtering: str, striphtml: bool, drop_blanks: bool) -> None:
    """Main Function"""

    assert os.path.isdir(filepath)
    list_filenames = os.listdir(filepath)
    for file_name in list_filenames:
        if file_name.endswith(".csv"):
            df = pandas.read_csv(os.path.join(filepath,file_name),delimiter=delimiter)
            print(df)

            csv_to_json_unlabelled.process(
                filename=os.path.join(filepath,file_name),
                metadata_keys=metadata_keys,
                created_at_col=created_at_col,
                utterance_col=utterance_col,
                delimiter=delimiter,
                convo_id_col=convo_id_col,
                role_col=role_col,
                role_mapper=role_mapper,
                unix_date=unix_date,
                encoding=encoding,
                filtering=filtering,
                striphtml=striphtml,
                drop_blanks=drop_blanks
            )

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter