# pylint: disable=invalid-name
"""
python ./diff_csv_col_id.py

Diff two outputs from HF pipelines based on two columns.
Example testing unlabelled v generated output result numbers.

"""
# *********************************************************************************************************************
# standard imports

# third party imports
import click
import pandas

# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-c', '--convoid_col', type=str, required=True, help='Convo ID Column')
@click.option('-d', '--diff_filename', type=str, required=True, help='Generated File')
@click.option('-s', '--source_convoid_col', type=str, required=True, help='Source Convo ID Column')
def main(input_filename: str, diff_filename: str, convoid_col: str, source_convoid_col: str):
    """Main function"""
    df1 = read_file(input_filename)
    df2 = read_file(diff_filename)
    convo_ids1 = set(df1[convoid_col].to_list())
    print(f'First file {input_filename} contains: {len(convo_ids1)}')
    convo_ids2 = set(df2[source_convoid_col].to_list())
    print(f'Second file {diff_filename} contains: {len(convo_ids2)}')
    print('Symetric Difference is:')
    print(convo_ids1.symmetric_difference(convo_ids2))
    # print(convo_ids2 - convo_ids1)

def read_file(filename: str) -> pandas.DataFrame:
    """Read either an excel or csv detecting filename type"""
    if filename.endswith('.csv'):
        return pandas.read_csv(filename)
    elif filename.endswith('.xlsx'):
        return pandas.read_excel(filename)
    else:
        raise RuntimeError(f'Unknown file type: {filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
