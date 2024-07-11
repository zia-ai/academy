"""
python count_tokens.py

"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import tiktoken

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    file_in = open(filename,mode="r",encoding="utf8")
    text = file_in.read()
    file_in.close()
    print(len(text))
    encoding = tiktoken.encoding_for_model("gpt-4-turbo")
    tokens = len(encoding.encode(text))
    print(tokens)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
