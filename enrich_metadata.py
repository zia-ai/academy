"""
python enrich_metadata.py

takes a .csv.fmt1 or .csv of data uploaded by csv uploader, joins it on a unique key to more metadata and
writes an output back to be uploaded by gui csv uploader.

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Location of HF CSV FMT unlabelled file')
@click.option('-m', '--metadata', type=str, required=True, help='Location of a CSV with IDs matching contextid')
@click.option('-i', '--index_col', type=str, required=True, default="SESSION_ID",
              help='Column name in CSV of the context.contextid index col')
@click.option('-r', '--remove_columns', type=str, default="",
              help='Provide list of columns names to be removed from output CSV')
def main(filename: str, metadata: str, index_col: str, remove_columns: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # Get metadata and set index
    df_metadata = pandas.read_csv(metadata,encoding="utf8")

    # drop duplicated rows
    df_metadata = df_metadata.drop_duplicates()

    # convert all columns into string type
    df_metadata = df_metadata.astype(str)

    # have to build the filename as we have up to 6 files per session
    df_metadata["filename"] = df_metadata["start_ts"].str[0:10] + "T" + df_metadata["t"] + ".wav"

    # set index
    df_metadata.set_index(["SESSION_ID","filename"],inplace=True)
    print(df_metadata)

    # Read the existing data
    df_existing = pandas.read_csv(filename,encoding="utf8")

    # format of path is like
    # <root_folder_name>---recordings---<session_id>---<file_name>
    # so split by "---"
    df_existing[["root_folder",
                 "sub_folder",
                 "SESSION_ID",
                 "filename"]] = df_existing["recording_file"].str.split("---",expand=True)

    # index by session_id, timestamp as it will be uploaded
    df_existing.set_index(["SESSION_ID","filename","timestamp"],inplace=True)
    print(df_existing)

    # join the new metadata on SESSION_ID
    df_existing = df_existing.join(df_metadata,on=["SESSION_ID","filename"],how="left")
    print(df_existing)

    # remove unwanted column names
    remove_columns_list = remove_columns.split(",")
    if remove_columns_list:
        for i,col in enumerate(remove_columns_list):
            remove_columns_list[i] = col.strip()

        df_existing.drop(columns=remove_columns_list,inplace=True)

    # output the file
    inter_filename = filename.replace(".fmt1","")
    output_filename = inter_filename.replace(".csv","_output.csv")
    assert output_filename != filename
    df_existing.to_csv(output_filename,header=True,index=True)
    print(f'Wrote to: {output_filename}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
