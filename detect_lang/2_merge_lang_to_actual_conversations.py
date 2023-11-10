#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# *********************************************************************************************************************
#
# python 2_merge_lang_to_actual_conversations.py
# This script populates the detected language of a conversation across all utterances of that conversation
#
# *********************************************************************************************************************

# standard imports
import json

# 3rd party imports
import pandas
import click


@click.command()
@click.option('-l', '--lang_filename', type=str, required=True,
              help='Input CSV File Path containing detected languages')
@click.option('-h', '--hf_filename', type=str, required=True,
              help='Input HF JSON File Path')
def main(lang_filename: str, hf_filename: str) -> None:
    """Main Function"""

    # load input data
    with open(hf_filename, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-",)

    # enforce id is string
    df["context-context_id"] = df["context-context_id"].astype(str)

    # give a sequence number to each utterance
    df = df.sort_values(["context-context_id", "created_at"])
    df['seq'] = df.groupby("context-context_id").cumcount()

    print(f"Total number of conversations: {len(df['context-context_id'].unique())}")

    # set context-context_id and seq as index
    df.set_index(["context-context_id", "seq"], drop=True, inplace=True)

    df_convo = pandas.read_csv(lang_filename,sep=",",encoding="utf8")
    df_convo.set_index(["context-context_id"], drop=True, inplace=True)

    print(f"Languages detected:{df_convo['lang'].unique()}")

    print(df_convo.groupby(["lang"]).count())

    df = df.apply(merge_lang,args=[df_convo],axis=1)

    file_output = lang_filename.replace(".csv","_populated.csv")
    df.to_csv(file_output,sep=",",encoding="utf8",index=True)
    print(f"Wrote the output to {file_output}")


def merge_lang(row:pandas.Series, df_convo: pandas.DataFrame ) -> pandas.Series:
    """Merges the laguage detected with the original dataframe"""

    row["lang"] = df_convo.loc[row.name[0]]["lang"]
    return row


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
