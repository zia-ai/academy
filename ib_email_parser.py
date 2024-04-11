# pylint: disable=invalid-name
"""
python ./ib_m.py
       -f <YOUR FILENAME>

"""
# *********************************************************************************************************************
# standard imports
import json

# third party imports
import click
import tiktoken
import pandas


# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-s', '--sample', type=int, required=False, default=0, help='Sampling')
@click.option('-t', '--trim_to', type=int, required=False, default=0, help='How Many Tokens Wanted')
def main(input_filename: str, sample: int, trim_to: int):
    """Main"""
    file_in =  open(input_filename,mode='r',encoding='utf8')
    dict_json = json.load(file_in)
    file_in.close()
    list_examples = dict_json["examples"]
    if sample > 0:
        list_examples = list_examples[0:sample]

    enc = tiktoken.encoding_for_model("gpt-4")
    df = pandas.json_normalize(list_examples)
    df["tokens"] = df["text"].apply(enc.encode)
    df["metadata.tokens_start"] = df["tokens"].apply(len)
    df["tokens_trimmed"] = df["tokens"].apply(slice_this,args=[trim_to])
    df["metadata.tokens_end"] = df["tokens_trimmed"].apply(len)
    df["text"] = df["tokens_trimmed"].apply(enc.decode)
    df.drop(columns=["tokens","tokens_trimmed"],inplace=True)
    print(df.columns)

    # calculate mapper
    all_columns = df.columns.to_list()
    mapper = {}
    for col in all_columns:
        assert isinstance(col,str)
        for replacer in ['metadata.','context.']:
            if col.startswith(replacer):
                mapper[col] = col.replace(replacer,"")
    df.rename(columns=mapper,inplace=True)
    df.rename(columns={'created_at':'old_created_at'},inplace=True)
    print(df.columns)

    output_filename = input_filename.replace(".json","_output.csv")
    assert input_filename != output_filename
    df.to_csv(output_filename)
    print(f'Wrote to: {output_filename}')

def slice_this(a_list:list,trim_to: int) -> list:
    """Slice a list like"""
    return a_list[0:trim_to]


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
