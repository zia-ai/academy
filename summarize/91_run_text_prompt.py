#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ***************************************************************************80*************************************120
#
# python ./summarize/run_text_prompt.py                                    # pylint: disable=invalid-name
#
# *********************************************************************************************************************

# standard imports
import json
import os
import logging
from multiprocessing import Pool
from time import perf_counter
import datetime

# 3rd party imports
import openai
import pandas
import numpy
import click
import tiktoken


@click.command()
@click.option('-i', '--input_filepath', type=str, required=True,
              help='Path containing HF Unlabelled conversations in json format')
@click.option('-a', '--openai_api_key', type=str, required=True, help='OpenAI API key')
@click.option('-p', '--prompt', type=str, default='./prompts/abcd_example_prompt.txt',
              help='location of prompt file to read')
@click.option('-t', '--tokens', type=int, default=500, help='Tokens to reserve for output')
@click.option('-n', '--num_cores', type=int, default=2, help='Number of cores for parallelisation')
@click.option('-s', '--sample_size', type=int, default=0, help='Number of conversations to sample')
@click.option('-l', '--log_file_path', type=str, default='./logs', help='Server log file path')
@click.option('-o', '--output_file_path', type=str, default='./summaries', help='Summaries output file path')
@click.option('-r', '--rewrite', is_flag=True, type=bool, default=False,
              help='If present will rewrite (overwrite) all previous summaries')
@click.option('-d', '--dummy', is_flag=True, type=bool, default=False, help='Skip the actual openai call')
@click.option('-v', '--verbose', is_flag=True, type=bool, default=False, 
              help='Set logging level to DEBUG otherwise INFO')
def main(input_filepath: str,
         openai_api_key: str,
         num_cores: int,
         prompt: str,
         tokens: int,
         sample_size: str,
         log_file_path: str,
         output_file_path: str,
         rewrite: bool,
         dummy: bool,
         verbose: bool) -> None:
    '''Main Function'''
    process(input_filepath, openai_api_key, num_cores, prompt, tokens,
            sample_size, log_file_path, output_file_path, rewrite, dummy, verbose)


def process(input_filepath: str,
            openai_api_key: str,
            num_cores: int,
            prompt: str,
            tokens: int,
            sample_size: str,
            log_file_path: str,
            output_file_path: str,
            rewrite: bool,
            dummy: bool,
            verbose: bool):
    '''Run prompt'''

    # set log level
    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG

    # determine current directory
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'

    # logging config
    if log_file_path.startswith("./"):
        now = datetime.datetime.now().isoformat()
        log_file_path = f'{dir_path}{log_file_path}/run_text_prompt_{now}.log'
    logging.basicConfig(
        filename=log_file_path,
        filemode='w',
        level=log_level,
        format='%(asctime)s - %(name)s - %(process)d - %(levelname)s -- %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.info("Logging to: %s", log_file_path)

    # get prompt
    if prompt.startswith("./"):
        prompt = prompt[2:]
    prompt_path = f'{dir_path}{prompt}'
    prompt = open(prompt_path, mode="r", encoding="utf8").read()
    logging.info("Prompt path is: %s", prompt_path)
    logging.info("Prompt is: \n %s", prompt)

    openai.api_key = openai_api_key

    # load input data
    with open(input_filepath, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-",)

    # enforce id is string
    df["context-context_id"] = df["context-context_id"].astype(str)
    
    # set context-context_id as index
    df.set_index(["context-context_id"], drop=True, inplace=True)

    # work out what's been run before
    if output_file_path.startswith('./'):
        output_file_path = output_file_path[2:]
        output_file_path = f'{dir_path}{output_file_path}'
    if not output_file_path.endswith('/'):
        output_file_path = f'{output_file_path}/'
    completed_df = get_completed_files(output_file_path)
    df = df.join(completed_df)
    df["completed"] = df["completed"].fillna(False)

    # default to run everything
    df["skip"] = False

    # skip if rewrite false
    if not rewrite:
        df["skip"] = df["completed"]

    # get all the context-context_ids
    context_ids = set(df.index.unique(level=0))
    completed_context_ids = set(completed_df.index.unique(level=0))

    # skip don't resample completed
    if not rewrite:
        context_ids = context_ids - completed_context_ids
    context_ids = list(context_ids)

    # implement dummy run flag
    if dummy:
        df["skip"] = dummy

    # take the next sample size to process
    if sample_size > 0:
        context_ids = context_ids[0:sample_size]
    logging.info(
        "In this run are going to process this many context_ids: %s", len(context_ids))
    logging.info("First up to 10: %s", context_ids[0:10])

    # select down the data frame to that.
    df = df.loc[context_ids, :]

    # assemble the final prompt with the {{ text }} replaced
    df['prompt'] = df['text'].apply(
        merge_prompt_and_convo, args=[prompt])
    df = df[["prompt", "skip", "completed"]]
    df.rename(columns={"prompt_line": "text"}, inplace=True)
    assert isinstance(df, pandas.DataFrame)
    print(df)
    print(df.loc[df.index[0],"prompt"])
    
    # estimate the tokens
    df['tokens'] = df["prompt"].apply(
        count_tokens, args=[tiktoken.encoding_for_model("gpt-3.5-turbo")])
    mean_input = df["tokens"].mean()
    logging.info(f'Mean input tokens is {mean_input}')
    logging.info(f'Output tokens is {tokens}')
    in_and_out_tokens = mean_input + tokens
    per_second = 1500 / in_and_out_tokens
    logging.info('Per second rate is max %2f', per_second)

    # max_tokens
    df['max_tokens'] = tokens

    # add the output location for the file
    df['summary_path'] = output_file_path + df.index + ".txt"

    # parallelization
    pool = Pool(num_cores)
    dfs = numpy.array_split(df, num_cores)
    pool_results = pool.map(parallelise_calls, dfs)
    pool.close()
    pool.join()
    df = pandas.concat(pool_results, axis=1)


def get_completed_files(output_file_path: str) -> pandas.DataFrame:
    '''Create a dataframe of ids that have already been created'''
    file_names = os.listdir(output_file_path)
    completed_ids = []
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_ids.append(file_name[0:-4])
    completed_df = pandas.DataFrame(
        completed_ids, columns=['context-context_id'])
    completed_df['completed'] = True
    completed_df.set_index(['context-context_id'], inplace=True, drop=True)
    return completed_df


def merge_prompt_and_convo(text: str, prompt: str) -> str:
    ''' Replaces {{ conversation }} with the actual conversation'''
    return prompt.replace('{{ text }}', str(text))


def parallelise_calls(df: pandas.DataFrame) -> pandas.DataFrame:
    '''Parallelise dataframe processing'''
    return df.apply(call_api, axis=1)


def call_api(row: pandas.Series) -> pandas.Series:
    '''Call OpenAI API for summarization'''

    start_time = perf_counter()
    logging.info(f"Calling OpenAI to summarize conversation - {row.name}")

    row["summary"] = ''
    row["total_tokens"] = 0
    if not row["skip"]:
        try:
            row["summary"], row["total_tokens"] = summarize(
                row["prompt"], row["max_tokens"])
        except Exception as e:
            logging.error(f'Exception for {row.name} is {e}')
            return row["summary"]

    logging.info('Total tokens for conversation id %s is %s', row.name, row["total_tokens"])
    logging.info(f'Conversation - {row.name} is summarized')

    # write the file to output
    with open(row["summary_path"], mode="w", encoding="utf8") as file:
        file.write(row["summary"])

    logging.info(f'Summary is saved at {row["summary_path"]}')
    end_time = perf_counter()
    logging.info(f'Took {end_time-start_time:.2f} seconds')
    return row


def summarize(prompt: str, tokens: int) -> str:
    '''Summarizes single conversation using prompt'''

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.0,
        max_tokens=tokens,
        top_p=1,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message.content + "\n", response.usage.total_tokens


def count_tokens(text: str, encoding):
    """Returns the number of tokens in a text string."""
    return len(encoding.encode(text))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
