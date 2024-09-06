"""
python template.py

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    df = pandas.read_csv(filename)
    print(df)
    df = df[["text","metadata:call_id","metadata:conversation_id"]]
    df.rename(columns={"metadata:call_id":"call_id"},inplace=True)
    df.rename(columns={"metadata:conversation_id":"conversation_id"},inplace=True)
    print(df)
    print(f'Unique call_ids:          {df["call_id"].nunique()}')
    print(f'Unique conversation_id:   {df["conversation_id"].nunique()}')
    print(f'Unique intent utterances: {df["text"].nunique()}')
    print(f'Total  intent utterances: {df["text"].count()}')
    output_filename = filename.replace(".csv","_output.csv")
    assert filename != output_filename
    df.to_csv(output_filename,index=False)
    print(f'Wrote to: {output_filename}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
