# pylint: disable=invalid-name
"""
python ./diff_runs_col_id.py

Diff two json outputs from HF pipelines based on two columns.
Gives same, present in left and present in right

"""
# *********************************************************************************************************************
# standard imports
import json

# third party imports
import click
import pandas

# custom imports


@click.command()
@click.option('-l', '--lhs', type=str, required=True, help='LHS Json')
@click.option('-r', '--rhs', type=str, required=True, help='RHS Json')
def main(lhs: str, rhs: str):
    """Main function"""

    # read files
    df1 = read_file(lhs,"lhs","metadata.sourceConversationId")
    df2 = read_file(rhs,"rhs","metadata.sourceConversationId")

    # join files
    df = df1.join(df2,how="outer")

    # Same
    df_same = df[df["text_lhs"] == df["text_rhs"]]
    print(f'\nSame    : {df_same.shape[0]}')
    print(df_same)

    # LHS
    df_present_in_left = df[df["text_rhs"].isna()]
    print(f'\nlhs only: {df_present_in_left.shape[0]}')
    print(df_present_in_left)

    # RHS
    df_present_in_right = df[df["text_lhs"].isna()]
    print(f'\nrhs only: {df_present_in_right.shape[0]}')
    print(df_present_in_right)

    # TDifferent
    df_different = df[~df["text_lhs"].isna() & ~df["text_rhs"].isna() & (df["text_lhs"] != df["text_rhs"])]
    print(f'\ndifferent only: {df_different.shape[0]}')
    print(df_different)

def read_file(filename: str, suffix: str, join_field: str) -> pandas.DataFrame:
    """Read either an excel or csv detecting filename type"""
    assert filename.endswith(".json")
    file_obj = open(filename,mode='r',encoding='utf8')
    file_dict = json.load(file_obj)
    df = pandas.json_normalize(file_dict["examples"])
    df = df[[join_field,"text"]]
    df.rename(columns={"text":f'text_{suffix}'},inplace=True)
    df.set_index(join_field,inplace=True)
    return df


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
