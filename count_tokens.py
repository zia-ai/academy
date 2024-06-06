"""
python count_tokens.py

Recursively scan a directory and count the tokens in each file

"""
# ******************************************************************************************************************120

# standard imports
import os

# 3rd party imports
import click
import tiktoken

# custom imports

@click.command()
@click.option('-d', '--directory', type=str, required=True, help='Directory')
@click.option('-r', '--reverse', type=bool, required=False, default=False,
              help='String sort ascheding or descending')
def main(directory: str,
         reverse: bool) -> None: # pylint: disable=unused-argument
    """Main Function"""

    embeddings = tiktoken.encoding_for_model("gpt-4o")

    process_dir(directory,reverse,embeddings)

def process_dir(directory:str, reverse: bool, embeddings):
    dir_count = 0
    dir_tokens = 0
    assert(os.path.isdir(directory))
    list_files = os.listdir(directory)
    list_files.sort(reverse=reverse)
    for fn in list_files:
        fqp = os.path.join(directory,fn)
        if os.path.isfile(fqp):
            with open(fqp,mode='r',encoding='utf8') as file_in:
                tokens = len(embeddings.encode(file_in.read()))
                dir_tokens = dir_tokens + tokens
                dir_count = dir_count + 1
        elif os.path.isdir(fqp):
            sub_tokens,sub_count = process_dir(fqp,reverse,embeddings)
            dir_tokens=dir_tokens+sub_tokens
            dir_count=dir_count + sub_count
        else:
            print(fqp)
            raise RuntimeError("WTF?")
    print(f'tokens: {dir_tokens:>20} count: {dir_count:>10} {directory}')
    return dir_tokens,dir_count

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
