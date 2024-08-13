# pylint: disable=invalid-name
"""
python ./compare_metadata_results.py
-f source filename
-l lhs column
-r rhs column
-a openai apikey

Run a pipeline with version a of the prompt and save it's results as lhs_col
Run a pipeline with version b of the prompt and save it's results as rhs_col

This script then accepts a downloaded csv lhs and rhs column names 
then embedds each result using openai in batch mode
and does a semantic simiarlity between the results

"""
# *********************************************************************************************************************
# standard imports
import math

# third party imports
import click
import tiktoken
import pandas
import openai
import numpy
import math

# custom imports


@click.command()
@click.option('-f', 'input_filename', type=str, required=True, help='Filename')
@click.option('-l', '--lhs_col', type=str, required=True, help='LHS metadata column name no metadata prefix')
@click.option('-r', '--rhs_col', type=str, required=True, help='RHS metadata column name no metadata prefix')
@click.option('-s', '--sample', type=int, required=False, default=0, help='Sampling')
@click.option('-a', '--apikey', type=str, required=True, help='OpenAI api key')
@click.option('-c', '--cosine_clip', type=float, required=True, help='Cosine Clip to show')
@click.option('-b', '--bucket_granularity', type=int, required=False, default=5, help='Division to bucket into')
@click.option('-t', '--trim_to', type=int, required=False, default=8190, help='How Many Tokens Wanted')
@click.option('-d', '--delimiter', type=str, required=False, default=':', help='Metadata Context Delimiter')
@click.option('-n', '--number_per_api_call', type=int, required=False, default=1024, help='How many to send to openai at once')
def main(input_filename: str, lhs_col: str, rhs_col: str, sample: int, apikey: str, cosine_clip: float,
         bucket_granularity: int, trim_to: int, delimiter: str, number_per_api_call: int):
    """Main"""

    # fix names
    lhs_origin = lhs_col
    lhs_col = f'metadata{delimiter}{lhs_col}'
    rhs_origin = rhs_col
    rhs_col = f'metadata{delimiter}{rhs_col}'

    # Read CSV
    df = pandas.read_csv(input_filename)

    # connect openai
    openai.api_key = apikey

    # Subsample if need.
    if sample > 0:
        df = df.head(sample)

    # encode_columns
    for col_name in [lhs_col,rhs_col]:
        df = encode_column(col_name, trim_to, df, number_per_api_call)
    print('Encoded columns')

    # get similarlity
    df["cosine_similarity"] = df.apply(calculate_cosine_sim,
                                       args=[
                                           f'{lhs_col}_embeddings',
                                           f'{rhs_col}_embeddings'
                                        ],
                                        axis=1
                                       )
    print('Calculated similarlity')

    # print a list of things over the clip
    df_filtered = df[df["cosine_similarity"]<cosine_clip]
    print("Values above clip")
    pandas.set_option('max_colwidth', 75)
    print(df_filtered[["cosine_similarity",lhs_col,rhs_col]])

    # bucket similarity
    df["bucketed"] = df["cosine_similarity"].apply(bucket_similarity,args=[bucket_granularity])
    print(df[["bucketed","id"]].groupby("bucketed").count())

    # overall
    print(f'\nAverage similarity cosine rounded down: {math.floor(df["cosine_similarity"].mean()*100)/100}')

    # write it to csv - droping columns excel doesn't like
    output_filename = input_filename.replace(".csv",f"_{lhs_origin}_{rhs_origin}.csv")
    assert input_filename != output_filename
    df[[lhs_col,rhs_col,"cosine_similarity","bucketed"]].to_csv(output_filename,index=False,header=True,encoding="utf8")
    print(f'Wrote to: {output_filename}')

def bucket_similarity(sim: float, division: int) -> float:
    """buckets a float into divisions treating 0.999+ as 1 but otherwise
    rounding down."""
    if sim >= 0.999:
        return 1.00
    else:
        return math.floor(sim * 100 / division) * division / 100

def calculate_cosine_sim(row: pandas.Series, embeddings_col_lhs: str, embeddings_col_rhs:str) -> float:
    "Calculate cosine"
    # calculate similarity with simple dot product which openai embeddings support
    return numpy.inner(row[embeddings_col_lhs],row[embeddings_col_rhs])

def encode_column(column_name: str, trim_to: int, df: pandas.DataFrame, chunk_size: int) -> pandas.DataFrame:
    """encode and embed a column"""

    # names are
    tokens_col = f'{column_name}_tokens_trimmed'
    tokens_trimmed_col = f'{column_name}_tokens_trimmed'
    embeddings_col = f'{column_name}_embeddings'
   
    # encode the tokens locally so we can send the maximum
    print(f'Converting: {column_name} to tokens')
    df[tokens_col] = df[column_name].apply(encode_this)

    # trim to the maximum number of tokens
    print(f'Trimming:   {column_name} to {trim_to} tokens')
    df[tokens_trimmed_col] = df[tokens_col].apply(slice_this,args=[trim_to])

    # ok so get an array of the trimmed tokens and parcel up into chunks for openi
    embed_this = df[tokens_trimmed_col].to_list()
    chunk_start = 0
    chunk_end = chunk_start + chunk_size
    chunk_estimate = math.ceil(len(embed_this)/chunk_size)
    print(f'Chunk estimate: {chunk_estimate}')
    
    embeddings_result = []
    while chunk_start < len(embed_this):
        print(f'Processing: {chunk_start:>10} {chunk_end:>10}')
        if chunk_end >= len(embed_this):
            chunk_end = len(embed_this)
            print(f'Adjusted final chunk {chunk_start:>10} {chunk_end:>10}')
        embeddings_result.extend(get_batch_embedding(embed_this[chunk_start:chunk_end],"text-embedding-3-large"))
        chunk_start = chunk_end
        chunk_end = chunk_start + chunk_size
    print(f'Embedded total number: {len(embeddings_result)}')
    df[embeddings_col] = embeddings_result
    return df

def encode_this(text:str,model:str = "gpt-4") -> str:
    "encode the column to tokens"
    enc = tiktoken.encoding_for_model(model)
    return enc.encode(str(text),disallowed_special=False)

def slice_this(a_list:list, trim_to: int) -> list:
    "slice a list like"
    return a_list[0:trim_to]

def get_embedding(text: str, model: str):
    "Get a single embedding"
    # https://platform.openai.com/docs/api-reference/embeddings/create
    return openai.Embedding.create(input = [text], model=model).data[0].embedding

def get_batch_embedding(text_list: list, model: str):
    "Get a batch embedding"
    # https://platform.openai.com/docs/api-reference/embeddings/create
    if isinstance(text_list, str):
        text_list = [text_list]
    assert isinstance(text_list,list)
    output_embeddings = openai.Embedding.create(input = text_list, model=model).data
    output_list = []
    for embedding in output_embeddings:
        output_list.append(embedding.embedding)
    return output_list

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
