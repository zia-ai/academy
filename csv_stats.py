"""
python template.py

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas
import tiktoken

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-t', '--text_col', type=str, required=True, help='Which has the utterances in')
@click.option('-c', '--convo_id_col', type=str, required=True, help='ConvoID to group on')
def main(filename: str,
         text_col: str,
         convo_id_col: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # Read the CSV
    df = pandas.read_csv(filename,encoding="utf8",dtype=str)
    
    # Count the tokens on the text field protecting against NaN
    embeddings = tiktoken.encoding_for_model("gpt-4o")
    df = df.fillna("")
    df["text_token_count"] = df[text_col].apply(count_tokens,args=[embeddings])
    
    # group by sum
    df_groupby = df[[convo_id_col,"text_token_count"]].groupby(convo_id_col).sum()
    assert isinstance(df_groupby,pandas.DataFrame)
    print(f'Average tokens per convo:     {df_groupby["text_token_count"].mean():.2f}')
    print(f'Max tokens per convo:         {df_groupby["text_token_count"].max():.2f}')
    
    # group by count
    df_groupby = df[[convo_id_col,"text_token_count"]].groupby(convo_id_col).count()
    print(f'Average utterances per convo: {df_groupby["text_token_count"].mean():.2f}')
    print(f'Max utterances per convo:     {df_groupby["text_token_count"].max():.2f}')
    
    # just average
    print(f'Average tokens per utterance: {df["text_token_count"].mean():.2f}')
    print(f'Max tokens per utterance:     {df["text_token_count"].max():.2f}')

def count_tokens(text: str, embeddings) -> int:
    try:
        return len(embeddings.encode(text))
    except Exception as e:
        print(e)
        print(text)
        return 0
    

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
