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
@click.option('-p', '--path', type=str, default='',help="Dir to read")
@click.option('-d', '--delimiter', type=str, default='',help="file to read")
def main(path: str, delimiter: str):
    """Main Function"""

    assert os.path.isdir(path)
    list_filenames = os.listdir(path)
    for file_name in list_filenames:
        if file_name.endswith(".csv"):
            df = pandas.read_csv(os.path.join(path,file_name),delimiter=delimiter)
            print(df)
            metadata_keys = df.columns.to_list()
            metadata_keys.remove("body")
            metadata_keys=",".join(metadata_keys)

            csv_to_json_unlabelled.process(
                filename=os.path.join(path,file_name),
                metadata_keys=metadata_keys,
                created_at_col="initial_created_at",
                utterance_col="body",
                delimiter=delimiter,
                convo_id_col="content_thread_id",
                role_col="creator_name",
                role_mapper=":client,Rob Bot | Chatbot | Agent conversationnel Bot:expert,*:expert",
                unix_date=False,
                encoding='utf8',
                filtering='',
                striphtml=False
            )

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
