"""
python template.py

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    df = pandas.read_csv(filename)
    print(df)

    gb = df[["text","context_id"]].groupby(["text"]).count()
    print(gb)
    
    gb["%"] = gb["context_id"] / gb["context_id"].sum() * 100
    print(gb)
    print(f'Checksum: {gb["%"].sum()}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
