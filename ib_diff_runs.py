# pylint: disable=invalid-name
"""
python ./ib_diff_runs.py

Assumes same keys

"""
# *********************************************************************************************************************
# standard imports
import math

# third party imports
import click
import tiktoken
import pandas
import tqdm
import openai
import numpy

# custom imports


@click.command()
@click.option('-f', 'input_filename', type=str, required=True, help='Filename')
@click.option('-l', '--lhs_col', type=str, required=True, help='LHS metadata column name no metadata prefix')
@click.option('-r', '--rhs_col', type=str, required=True, help='RHS metadata column name no metadata prefix')
@click.option('-s', '--sample', type=int, required=False, default=0, help='Sampling')
@click.option('-a', '--apikey', type=str, required=True, help='OpenAI api key')
@click.option('-c', '--cosine_clip', type=float, required=True, help='Cosine Clip to show')
@click.option('-b', '--bucket_granularity', type=int, required=False, default=5, help='Division to bucket into')
@click.option('-t', '--trim_to', type=int, required=False, default=3000, help='How Many Tokens Wanted')
@click.option('-d', '--delimiter', type=str, required=False, default=',', help='Metadata Context Delimiter')
def main(input_filename: str, lhs_col: str, rhs_col: str, sample: int, apikey: str, cosine_clip: float,
         bucket_granularity: int, trim_to: int, delimiter: str):
    """Main"""

    # fix names
    lhs_col = f'metadata{delimiter}{lhs_col}'
    rhs_col = f'metadata{delimiter}{rhs_col}'

    # Read CSV
    df = pandas.read_csv(input_filename)

    # connect openai
    openai.api_key = apikey

    # tqdm
    tqdm.tqdm.pandas()

    # Subsample if need.
    if sample > 0:
        df = df.head(sample)

    # encode_columns
    for col_name in [lhs_col,rhs_col]:
        df = encode_column(col_name, trim_to, df)
    print('Encoded columns')

    # get similarlity
    print(df.columns)
    df["cosine_similarity"] = df.apply(calculate_cosine_sim,
                                       args=[
                                           f'{lhs_col}_embeddings',
                                           f'{rhs_col}_embeddings'
                                        ],
                                        axis=1
                                       )
    print('Calculated similarlity')

    print(df.columns)
    df_filtered = df[df["cosine_similarity"]<cosine_clip]
    pandas.set_option('max_colwidth', 75)
    print(df_filtered[["cosine_similarity",lhs_col,rhs_col]])

    # bucket similarity
    df["bucketed"] = df["cosine_similarity"].apply(bucket_similarity,args=[bucket_granularity])
    print(df[["bucketed","id"]].groupby("bucketed").count())

    # overall
    print()
    print(f'Average similarity cosine rounded down: {math.floor(df["cosine_similarity"].mean()*100)/100}')

    # write it to csv - droping columns excel doesn't like
    output_filename = input_filename.replace(".csv","_output.csv")
    assert input_filename != output_filename
    df[[lhs_col,rhs_col,"cosine_similarity","bucketed"]].to_csv(output_filename,index=False,header=True,encoding="utf8")
    print(f'Wrote to: {output_filename}')

def bucket_similarity(sim: float, division: int) -> float:
    """buckets a float into divisions rounding down."""
    return math.floor(sim * 100 / division) * division / 100

def calculate_cosine_sim(row: pandas.Series, embeddings_col_lhs: str, embeddings_col_rhs:str) -> float:
    "Calculate cosine"
    # calculate similarity with simple dot product which openai embeddings support
    return numpy.inner(row[embeddings_col_lhs],row[embeddings_col_rhs])

def encode_column(column_name: str, trim_to: int, df: pandas.DataFrame) -> pandas.DataFrame:
    """encode and embed a column"""

    # names are
    tokens_col = f'{column_name}_tokens_trimmed'
    tokens_trimmed_col = f'{column_name}_tokens_trimmed'
    embeddings_col = f'{column_name}_embeddings'

    print(f'Encoding: {column_name}')
    df[tokens_col] = df[column_name].apply(encode_this)
    print(f'Trimming: {column_name}')
    df[tokens_trimmed_col] = df[tokens_col].apply(slice_this,args=[trim_to])
    print(f'Embedding: {column_name}')
    df[embeddings_col] = df[tokens_trimmed_col].progress_apply(get_embedding,args=["text-embedding-3-small"])
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
    # TODO: work out batch
    return openai.Embedding.create(input = [text], model=model).data[0].embedding

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
