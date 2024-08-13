"""
python mbox_make_csv.py

Recursively scan a directory and load the json and turn into a CSV which can then be
csv_json_to_unlabelled

"""
# ******************************************************************************************************************120

# standard imports
import os
import json
import datetime

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-d', '--directory', type=str, required=True, help='Directory')
@click.option('-r', '--reverse', type=bool, required=False, default=False,
              help='String sort ascheding or descending')
def main(directory: str,
         reverse: bool) -> None: # pylint: disable=unused-argument
    """Main Function"""

    all_dicts = []
    process_dir(directory,reverse,all_dicts)
    df = pandas.json_normalize(all_dicts)
    df["timestamp"].fillna(datetime.datetime.now().isoformat())
    df.loc[df["timestamp"]=='',"timestamp"] = datetime.datetime.now().isoformat()
    print("Why do these error?")
    print(df.shape)
    print(df.loc[df["content"]=='',["filename","content"]])
    df = df[~(df["content"]=='')]
    print(df.loc[df["content"]=='',["filename","content"]])
    print(df.shape)
    output_filename = os.path.join(directory,"output.csv")
    df.to_csv(output_filename,index=False,header=True)
    print(f'Wrote to: {output_filename}')
    print(df)
    tokens = df["tokens"].sum()

    # lets bucket tokens
    df["tokens_bucket"] = df["tokens"].apply(bucket_tokens)

    #gb
    gb = df[["filename","tokens_bucket"]].groupby("tokens_bucket").count()
    print(gb)


    print(f'Total tokens: {tokens}')
    if 'tokens_shrunk' in df.columns.to_list():
        tokens_shrunk = df["tokens_shrunk"].sum()
        print(f'Shrunk tokens: {tokens_shrunk}')
        print(f'Diff:          {tokens - tokens_shrunk}')


def bucket_tokens(tokens: int) -> int:
    """Bucket tokens"""
    if tokens >= 8191:
        return 9000
    elif tokens >= 4095:
        return 4095
    elif tokens >= 2047:
        return 2047
    elif tokens >= 1024:
        return 1024
    elif tokens >= 7:
        return 7
    elif tokens >= 1:
        return 1
    else:
        return 0


def process_dir(directory:str, reverse: bool, all_dicts: list):
    """Cycle through the directories"""
    dir_count = 0
    assert os.path.isdir(directory)
    list_files = os.listdir(directory)
    list_files.sort(reverse=reverse)
    for fn in list_files:
        fqp = os.path.join(directory,fn)
        if os.path.isfile(fqp):
            if fqp.endswith('.json'):
                with open(fqp,mode='r',encoding='utf8') as file_in:
                    try:
                        json_dict = json.load(file_in)
                        json_dict['filename'] = fn
                        all_dicts.append(json_dict)
                    except Exception as e:
                        print(f'Can\'t read {fqp}')
                        print(e)
                    dir_count = dir_count + 1
        elif os.path.isdir(fqp):
            sub_count = process_dir(fqp,reverse,all_dicts)
            dir_count=dir_count + sub_count
        else:
            print(fqp)
            raise RuntimeError("WTF?")
    print(f'{dir_count:>10}    {directory}')
    return dir_count

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
