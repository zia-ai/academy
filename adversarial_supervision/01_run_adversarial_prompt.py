#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./adversarial_outbound_supervision/01_run_adversarial_prompt.py                 # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# standard imports
from multiprocessing import Pool
import re
from os.path import join

# 3rd party imports
import openai
import pandas
import numpy
import click


@click.command()
@click.option('-r', '--results', type=str, default="./adversarial_outbound_supervision/results/",
              help='folder path where results are stored')
@click.option('-a', '--openai_api_key', type=str, required=True, help='OpenAI API key')
@click.option('-p', '--prompt', type=str, required=True, help='location of prompt file to read')
@click.option('-t', '--tokens', type=int, default=500, help='Tokens to reserve for output')
@click.option('-n', '--num_cores', type=int, default=2, help='Number of cores for parallelisation')
@click.option('-c', '--count', type=int, default=5, help='Number of conversations to sample')
def main(results: str,
         openai_api_key: str,
         num_cores: int,
         prompt: str,
         tokens: int,
         count: str) -> None:
    '''Main Function'''
    process(results, openai_api_key, num_cores, prompt, tokens, count)


def process(results: str,
            openai_api_key: str,
            num_cores: int,
            prompt: str,
            tokens: int,
            count: str) -> None:
    '''Run prompt'''

    openai.api_key = openai_api_key

    with open(prompt, mode="r",encoding="utf8") as f:
        prompt_text = f.read()

    print(f"Prompt: {prompt_text}")

    prompt_list = []

    i = 0
    while i<count:
        prompt_list.append({
            "prompt": prompt_text, 
            "max_tokens": tokens 
        })
        i = i+1

    df = pandas.json_normalize(data=prompt_list)

    print(df)

    # parallelization
    pool = Pool(num_cores)
    dfs = numpy.array_split(df, num_cores)
    pool_results = pool.map(parallelise_calls, dfs)
    pool.close()
    pool.join()
    df = pandas.concat(pool_results)

    # enforce this column is string
    df["completion"] = df["completion"].astype(str)

    output= prompt.split("/")[-1]
    output= output.replace(".txt", "_results.csv")
    output= join(results,output)

    print(df["completion"])
    df.to_csv(output, sep=",", encoding="utf8", index=False)


def parallelise_calls(df: pandas.DataFrame) -> pandas.DataFrame:
    '''Parallelise dataframe processing'''
    return df.apply(call_api, axis=1)


def call_api(row: pandas.Series) -> pandas.Series:
    '''Call OpenAI API for summarization'''

    row["completion"], row["total_tokens"] = summarize(row["prompt"], row["max_tokens"])

    row["completion"] = re.sub(r'^"*','',row["completion"])
    row["completion"] = re.sub(r'"*$','',row["completion"])
    return row


def summarize(prompt: str, tokens: int) -> str:
    '''Summarizes single conversation using prompt'''

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0301",
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=1.0,
        max_tokens=tokens,
        top_p=1,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message.content, response.usage.total_tokens


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
