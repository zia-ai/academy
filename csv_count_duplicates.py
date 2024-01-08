"""
python csv_count_duplicates.py

Approximates the number of duplicates in a provided csv file to humanfirst upload

assumes humanfirst simple utterance upload format for model of
no header line
utterance,intent_name

"""
# *********************************************************************************************************************

# standard imports

# 3rd party imports
import pandas
import click

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None:
    """Main Function"""

    # read the csv into a dataframe
    assert filename.endswith(".csv")
    df = pandas.read_csv(filename, encoding="utf8", names=["utterance","intent_name"], dtype=str,delimiter=",")

    # create a lowercase version
    df["lowercase"] = df["utterance"].str.lower()

    # replace punctuation with space
    punct = r'[_\-\!$£%^&\"\'*@#\[\]\{\}\(\)\+\=\\\/¬`<>,.\?]'
    df["no_punctuation"] = df["lowercase"].str.replace(punct,' ',regex=True)

    # remove multiple white space
    df["single_white_space"] = df["no_punctuation"].replace(r'[ ]+',' ',regex=True)

    # stri[] whitespace
    df["stripped"] = df["single_white_space"].str.strip()

    # group by
    df = df[["intent_name","stripped","utterance"]].groupby(["intent_name","stripped"]).count()
    print(df)
    print(df.sort_values("utterance",ascending=False).head(50))

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
