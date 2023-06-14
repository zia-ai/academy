#!/usr/bin/env python
# -*- coding: utf-8 -*-
# *******************************************************************************************************************120
#  Code Language:   python
#  Script:          dataset_info.py
#  Imports:         click, requests, pandas, humanfirst_apis
#  Functions:       main(), get_source_id()
#  Description:     Produces a CSV contaning dataset information
#
# **********************************************************************************************************************

# third party imports
import requests
import click
import pandas
from typing import Union
# custom import
import humanfirst_apis


@click.command()
@click.option('-u', '--username', type=str, required=True, help='HumanFirst username')
@click.option('-p', '--password', type=str, required=True, help='HumanFirst password')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-o', '--output_path', type=str, required=True, help='Output CSV Path')
def main(username: str, password: str, namespace: str, output_path: str) -> None:
    """Main function"""

    headers = humanfirst_apis.process_auth(username=username, password=password)
    conversation_set_list = humanfirst_apis.get_conversion_set_list(headers, namespace)
    df = pandas.json_normalize(data=conversation_set_list, sep="-")
    df.rename(columns={"id": "conversation_set_id"}, inplace=True)
    df["conversation_source_id"] = df["sources"].apply(get_source_id)

    df.drop(columns=["sources"], inplace=True)

    df.to_csv(output_path, encoding="utf-8", sep=",", index=False)
    print(df)
    print(f"CSV is stored at {output_path}")


def get_source_id(source: Union[list, float]) -> Union[str, float]:
    '''Extracts the conversation source id if present'''

    if not isinstance(source, float):
        if not pandas.isna(source).all():
            for i, obj in enumerate(source):
                if 'conversationSourceId' in obj:
                    return obj['conversationSourceId']
            return pandas.NA
        else:
            # returning null value
            return source[0]
    else:
        # returning null value
        return source


if __name__ == "__main__":
    main()
