# pylint: disable=invalid-name
"""
python ./ib_m.py
       -f <YOUR FILENAME>

"""
# *********************************************************************************************************************
# standard imports

# third party imports
import click
from bs4 import BeautifulSoup

# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
def main(input_filename: str):
    """Main"""
    file = open(input_filename, mode = "r", encoding = "utf8")
    soup = BeautifulSoup(file.read(), 'html.parser')
    reviews = soup.findAll("div", { "class" : "paper__bd" })
    print(f'Len of reviews: {len(reviews)}')
    for i, r in enumerate(reviews):
        print(f'Working on {i}')
        output_filename = input_filename.replace(".html",f'_{i:04}.html')
        with open(output_filename,mode='w',encoding='utf8') as file_out:
            file_out.write(str(r))

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
