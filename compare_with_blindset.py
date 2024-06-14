# pylint: disable=invalid-name
"""
python ./compare_metadata_results.py
-f source filename
-s blindset
-i index_col (to be unique and same titled across blind and filename - defaults to 0
-l blindset_col (defaults to 1)
-r target_col ()

This 

"""
# *********************************************************************************************************************
# standard imports
import math

# third party imports
import click
import pandas
import openai
import math

# custom imports
import compare_metadata_results


@click.command()
@click.option('-f', 'input_filename', type=str, required=True, help='Filename')
@click.option('-s', 'blindset_filename', type=str, required=True, help='Blindset filenmae')
@click.option('-i', '--index_col', type=str, required=False, default='0', 
              help='index column to join on - defaults to blindset column 0')
@click.option('-l', '--lhs_col', type=str, required=False, default='1',
              help='LHS defaults to blindset column 1')
@click.option('-r', '--rhs_col', type=str, required=True, help='RHS metadata column name with metadata if prefix')
@click.option('-a', '--apikey', type=str, required=True, help='OpenAI api key')
@click.option('-c', '--cosine_clip', type=float, required=True, help='Cosine Clip to show')
@click.option('-b', '--bucket_granularity', type=int, required=False, default=5, help='Division to bucket into')
@click.option('-t', '--trim_to', type=int, required=False, default=8190, 
              help='How Many Tokens Wanted')
@click.option('-n', '--number_per_api_call', type=int, required=False, default=1024, 
              help='How many to send to openai at once')
def main(input_filename: str, blindset_filename: str, 
         index_col: str, lhs_col: str, rhs_col: str, 
         apikey: str, cosine_clip: float,
         bucket_granularity: int, trim_to: int, 
         number_per_api_call: int):
    """Main"""

    # Read filenames CSV
    df_blindset = pandas.read_csv(blindset_filename)
    df = pandas.read_csv(input_filename)

    # fix names
    lhs_origin = lhs_col
    rhs_origin = rhs_col
    if index_col == '0':
        print('here')
        index_col = df_blindset.columns.to_list()[0]
    if lhs_col == '1':
        lhs_col = df_blindset.columns.to_list()[1]
    
    # check that index col present
    try:
        assert index_col in df_blindset.columns.to_list()
        assert lhs_col in df_blindset.columns.to_list()
        assert index_col in df.columns.to_list()
        assert rhs_col in df.columns.to_list()
    except Exception as e:
        print(df.columns.to_list())
        print(df_blindset.columns.to_list())
        print(index_col)
        print(lhs_col)
        print(rhs_col)
        print(e)
        quit()

    # join and filter
    df_blindset.set_index(index_col,drop=True,inplace=True)
    df = df.join(df_blindset,on=index_col,how='inner')

    # connect openai
    openai.api_key = apikey

    # encode_columns
    df = compare_metadata_results.encode_column(rhs_col, trim_to, df, number_per_api_call)
    df_blindset = compare_metadata_results.encode_column(lhs_col, trim_to, df, number_per_api_call)
    print('Encoded columns')

    # get similarlity
    df["cosine_similarity"] = df.apply(compare_metadata_results.calculate_cosine_sim,
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
    df["bucketed"] = df["cosine_similarity"].apply(compare_metadata_results.bucket_similarity,args=[bucket_granularity])
    print(df[["bucketed","id"]].groupby("bucketed").count())

    # overall
    print(f'\nAverage similarity cosine rounded down: {math.floor(df["cosine_similarity"].mean()*100)/100}')

    # write it to csv - droping columns excel doesn't like
    output_filename = input_filename.replace(".csv",f"_{lhs_origin}_{rhs_origin}.csv")
    assert input_filename != output_filename
    df[[lhs_col,rhs_col,"cosine_similarity","bucketed"]].to_csv(output_filename,index=False,header=True,encoding="utf8")
    print(f'Wrote to: {output_filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
