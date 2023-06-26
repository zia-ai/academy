#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./adversarial_outbound_supervision/01_run_adversarial_supervision_prompt.py     # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# standard imports
from multiprocessing import Pool
import re
from  os.path import exists, join
import os

# 3rd party imports
import openai
import pandas
import numpy
import click


@click.command()
@click.option('-r', '--results', type=str, default="./adversarial_outbound_supervision/results/",
              help='folders path having CSV files containing results of working/non-working adversarial prompt')
@click.option('-a', '--openai_api_key', type=str, required=True, help='OpenAI API key')
@click.option('-p', '--prompt', type=str, required=True, help='location of prompt file to read')
@click.option('-t', '--tokens', type=int, default=500, help='Tokens to reserve for output')
@click.option('-n', '--num_cores', type=int, default=2, help='Number of cores for parallelisation')
def main(results: str,
         openai_api_key: str,
         num_cores: int,
         prompt: str,
         tokens: int) -> None:
    '''Main Function'''
    process(results, openai_api_key, num_cores, prompt, tokens)


def process(results: str,
            openai_api_key: str,
            num_cores: int,
            prompt: str,
            tokens: int) -> None:
    '''Run prompt'''

    openai.api_key = openai_api_key

    df_list = []
    for _, _, filenames in os.walk(results):
        for file in filenames:
            path = join(results,file)
            if exists(path) and file != "adversarial_supervision_results.csv":
                df_list.append(pandas.read_csv(path,sep=",",encoding="utf8"))

    df = pandas.concat(df_list)

    examples_to_verify = df["completion"].unique().tolist()

    with open(prompt, mode="r",encoding="utf8") as f:
        prompt_text = f.read()

    print(f"Prompt: \n{prompt_text}")
    print()

    prompt_list = []

    i = 0
    while i<len(examples_to_verify):
        replaced_prompt = prompt_text.replace(r"{{text}}",examples_to_verify[i])
        prompt_list.append({
            "prompt": replaced_prompt, 
            "max_tokens": tokens 
        })
        i = i+1

    df = pandas.json_normalize(data=prompt_list)

    # parallelization
    pool = Pool(num_cores)
    dfs = numpy.array_split(df, num_cores)
    pool_results = pool.map(parallelise_calls, dfs)
    pool.close()
    pool.join()
    df = pandas.concat(pool_results)

    # enforce column is string
    df["whether_offensive"] = df["whether_offensive"].astype(str)

    output=join(results,"adversarial_supervision_results.csv")
    print(df[["prompt","whether_offensive"]])
    df.to_csv(output, sep=",", encoding="utf8", index=False)


def parallelise_calls(df: pandas.DataFrame) -> pandas.DataFrame:
    '''Parallelise dataframe processing'''
    return df.apply(call_api, axis=1)


def call_api(row: pandas.Series) -> pandas.Series:
    '''Call OpenAI API for summarization'''

    row["whether_offensive"], row["total_tokens"] = summarize(row["prompt"], row["max_tokens"])

    row["whether_offensive"] = re.sub(r'^"*','',row["whether_offensive"])
    row["whether_offensive"] = re.sub(r'"*$','',row["whether_offensive"])
    row["whether_offensive"] = re.sub(r'\.*$','',row["whether_offensive"])
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
