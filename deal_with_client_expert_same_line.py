"""
python deal_with_client_expert_same_line.py

separates out client and expert utterances
allows a switch to determine if one goes first or second
optionally lets strip a particular string for instance any utterances which are "WELCOME"

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='CSV Filename to operate on')
@click.option('-c', '--convo_id_col', type=str, required=True, help='Conversation ID column')
@click.option('-t', '--timestamp_col', type=str, required=True, help='Name column has the timestamp in')
@click.option('-i', '--client_col', type=str, required=True, help='Name of column with the client utterance in')
@click.option('-e', '--expert_col', type=str, required=True, help='Name of column with the expert utterance in')
@click.option('-d', '--drop_this', type=str, required=False, default='', help='Drop any utterances matching this.')
@click.option('-b', '--begin_client', is_flag=True, required=False, default=False, 
              help='Whether client or expert is expected to speak first')
def main(filename: str, 
         convo_id_col: str,
         timestamp_col: str,
         client_col: str, expert_col: str,
         drop_this: str,
         begin_client: bool
         ) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # read df and check cols    
    df = pandas.read_csv(filename,dtype=str)
    cols = df.columns.to_list()
    for col in [convo_id_col,timestamp_col,client_col,expert_col]:
        try:
            assert col in cols
        except AssertionError as e: # pylint: disable=unused-variable
            print(f'Couldn\'t find col {col}')
            print(cols)
            quit()
    print('All columns found')
    print(df)

    # baseline expert timestamp
    df["expert_timestamp"] = pandas.to_datetime(df[timestamp_col])

    # add or takeaway 1 ms based on expert of client first
    if begin_client:
        delta = -1
    else:
        delta = 1
    df["client_timestamp"] = df["expert_timestamp"] + pandas.to_timedelta(delta,"ms") # ms = microseoncds

    # separate out client
    df_client = df.copy(deep=True)
    df_client.drop(columns=["expert_timestamp",timestamp_col,expert_col],inplace=True)
    df_client.rename(columns={"client_timestamp":timestamp_col,client_col:"text"},inplace=True)
    df_client["input_role"] = "client"
    print(df_client)

    # separate out expert
    df_expert = df.copy(deep=True)
    df_expert.drop(columns=["client_timestamp",timestamp_col,client_col],inplace=True)
    df_expert.rename(columns={"expert_timestamp":timestamp_col,expert_col:"text"},inplace=True)
    df_expert["input_role"] = "expert"
    print(df_expert)

    # concatenate overwriting df
    df = pandas.concat([df_expert,df_client],axis=0)
    df.sort_values([convo_id_col,timestamp_col],inplace=True)

    # delete any strip utterances
    if drop_this != '':
        df = df[~(df["text"]==drop_this)]

    print(df)

    # write to output
    output_filename = filename.replace(".csv", "_output.csv")
    assert filename != output_filename
    df.to_csv(output_filename,index=False,header=True)
    print(f'Wrote to: {output_filename}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
