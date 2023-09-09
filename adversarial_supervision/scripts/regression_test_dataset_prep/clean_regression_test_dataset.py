#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./adversarial_supervision\
#         /scripts\
#         /regression_test_dataset_prep\
#         /clean_regression_test_dataset.py                                              # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# 3rd party imports
import click
import pandas

@click.command()
@click.option('-f', '--file_path', type=str, required=True,help='Regression Test CSV file path')
def main(file_path: str) -> None:
    '''Main Function'''

    process(file_path)


def process(file_path: str) -> None:
    '''Clean Dataset'''

    df = pandas.read_csv(file_path,sep=",",encoding="utf8")
    df["cleaned_phrases"] = df["phrases"].apply(clean_phrase)
    print(df["cleaned_phrases"])

    output_file_path = file_path.replace(".csv","_cleaned.csv")

    df["cleaned_phrases"].to_csv(output_file_path,encoding="utf-8",sep=",",header=None,index=False)
    print(f"Cleaned phrases are stored at {output_file_path}")

def clean_phrase(text: str) -> str:
    """Clean phrase"""

    return text.strip("-").strip(" ")


if __name__=="__main__":
    main() # pylint: disable=no-value-for-parameter
