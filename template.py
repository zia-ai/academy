"""
python template.py

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    print("Hello World")

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
