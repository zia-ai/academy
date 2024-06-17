"""
python ./add_date_to_conversation_csv.py

HF ensures the conversation flow stays intact by using utterance created at date and time
This script helps to add date and time to every utterances in all conversations in a CSV 
"""
# *********************************************************************************************************************

# standard imports
import os
import datetime

# third party imports
import click
import pandas

@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-c', '--convo_id', type=str, required=True, help='Conversation ID column name')
def main(input_filename: str, convo_id: str) -> None:
    """Main Function"""

    if not os.path.exists(input_filename):
        print("Couldn't find the dataset at the file location you provided:")
        print(input_filename)
        quit()

    df = pandas.read_csv(input_filename, encoding='utf8')
    assert isinstance(df, pandas.DataFrame)

    # Initialize the datetime column
    df['created_at'] = pandas.NaT

    # Get the current datetime
    current_time = datetime.datetime.now()

    # Apply the function to each group and assign it to the datetime column
    df['created_at'] = df.groupby(convo_id).cumcount().apply(
        lambda x: current_time + datetime.timedelta(seconds=x))
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')


    # Save the updated dataframe back to csv
    output_filename = input_filename.replace(".csv","_output.csv")
    df.to_csv(output_filename, index=False, header=True)
    print(f"Dates are added to the CSV and saved at {output_filename}")


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
