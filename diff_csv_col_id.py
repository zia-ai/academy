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
    df1 = pandas.read_csv(input_filename)
    df2 = pandas.read_csv(diff_filename)
    convo_ids1 = set(df1[convoid_col].to_list())
    print(convo_ids1)
    print(len(convo_ids1))
    convo_ids2 = set(df2[source_convoid_col].to_list())
    print(len(convo_ids2))
    print(convo_ids1.symmetric_difference(convo_ids2))
    # print(convo_ids2 - convo_ids1)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
