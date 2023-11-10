"""

python csv_column_to_expert_response.py
-f input filename of a csv
-c which the expert utterance is in

Will copy the expert utterance to a new row with +1 ms timestamp

"""
#********************************************************************************************************************120

# standard imports
import datetime
from dateutil import parser

# 3rd party imports
import pandas
import click

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input filename of a csv')
@click.option('-c', '--convo_id_col', type=str, required=True, help='Conversation ID Column - primary sort key')
@click.option('-t', '--created_at_col', type=str, required=False, default='',
              help='Created date for utterance secondary sort key')
@click.option('-u', '--utterance_col', type=str, required=True,
              help='Column name containing client utterances')
@click.option('-r', '--response_col', type=str, required=True, help='Column with the expert response in')
@click.option('-e', '--encoding', type=str, default="utf8", help='Column to turn into client rows')
@click.option('-d', '--delimiter', type=str, default=",", help='Column to turn into client rows')
@click.option('-s', '--sample', type=str, default="", help='Do only this convoid')
def main(filename: str, convo_id_col: str, created_at_col: str,
         utterance_col: str, response_col: str,
         delimiter: str, encoding: str,
         sample: str) -> None:
    """Main Function"""

    # check it's a csv
    assert filename.endswith(".csv")

    # read DF
    df_client = pandas.read_csv(filename,delimiter=delimiter,encoding=encoding)

    # sample down if necessary
    if sample != "":
        print(f'Before sample shape: {df_client.shape}')
        df_client = df_client[df_client[convo_id_col]==sample]
        print(f'After  sample shape: {df_client.shape}')
        print(df_client)

    # index the records in case of timestamps that have the same value if low precision is used
    # this is a bit crappy as it assumes they are in the right order to start with!
    df_client['idx'] = df_client.groupby(convo_id_col).cumcount()

    # create response df move response col into utterance col
    df_expert = df_client.copy(deep=True)
    df_expert.drop(columns=[utterance_col],inplace=True)
    df_expert.rename(columns={response_col:utterance_col},inplace=True)

    # drop expert column from client
    df_client.drop(columns=[response_col],inplace=True)

    # add 1ms for expert none for client
    df_expert[created_at_col] = df_expert.apply(add_milliseconds,args=[created_at_col,1,False],axis=1)
    df_client[created_at_col] = df_client.apply(add_milliseconds,args=[created_at_col,0,False],axis=1)

    # create roles
    df_client["csv_role"] = "client"
    df_expert["csv_role"] = "expert"

    # concatenate
    df = pandas.concat([df_client,df_expert],axis=0)

    # sort by primary and secondary key
    df.sort_values([convo_id_col,"idx",created_at_col],inplace=True)

    # write to output
    output_filename = filename.replace(".csv","_output.csv")
    assert output_filename != filename
    df.to_csv(output_filename,index=False,header=True,encoding=encoding)
    print(f'Wrote to: {output_filename}')

    # debugging if sample
    if sample != "":
        print(df)


def add_milliseconds(row: pandas.Series, created_at_col: str,
                     milliseconds: int, dayfirst: bool = True) -> datetime.datetime:
    '''Add milliseconds based on idx to timestamp and produce datetime object'''
    milliseconds = milliseconds + row["idx"]
    candidate_datetime = parser.parse(row[created_at_col],dayfirst=dayfirst)
    candidate_datetime = candidate_datetime + datetime.timedelta(milliseconds=milliseconds)
    return candidate_datetime.isoformat(timespec="milliseconds")

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
