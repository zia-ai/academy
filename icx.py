"""
python icx.py

"""
# *********************************************************************************************************************

# standard imports
import csv
import re

# 3rd party imports
import pandas
import click

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None:
    """Main Function"""

    re_fixit = re.compile(', ([0-9]{13}), (client|expert), ')

    with open(filename,mode='r',encoding='utf8') as file_in:
        text = file_in.read()
        text = re_fixit.sub(replacement_is,text)
        output_filename=filename.replace(".csv","_output.csv")
        assert output_filename != filename
        with open(output_filename,mode='w',encoding='utf8') as file_out:
            file_out.write(text)
            print(f'Wrote to: {output_filename}')

    df = pandas.read_csv(output_filename,names=["id","unixdate","role","text"],
                         encoding='utf8',dtype=str,sep=",")



    print(df)

def replacement_is(match:re.Match) -> str:
    assert match.group(1) is not None and match.group(2) is not None
    return f',{match.group(1)},{match.group(2)},'

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
