"""
l.py

Adding initial question as first user utterance in conversations
"""
# *********************************************************************************************************************

# standard imports
import re

# custom imports
import pandas
import click

# 3rd party imports


@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str):
    """main"""

    # read whole data frame
    df = pandas.read_csv(filename, encoding="utf8",)

    # get the first utterances
    group_by_cols = ["conversation_id","conversation_startdatetime","initial_question","company_name","reply_author"]
    df_initial_question = df.groupby(group_by_cols).count().reset_index()
    df_initial_question["reply_author"] = "user"
    df_initial_question = df_initial_question[["conversation_id","conversation_startdatetime","company_name",
                                               "reply_author","initial_question"]]
    rename_mapper = {
        "conversation_startdatetime":"reply_datetime",
        "initial_question":"reply",
        }
    df_initial_question.rename(inplace=True,columns=rename_mapper)

    print(df_initial_question)

    # reduce original df
    df = df[["conversation_id","reply_datetime","company_name","reply_author","reply"]]
    print(df)

    # add a to b
    df = pandas.concat([df,df_initial_question],axis=0)
    df = df.sort_values(["conversation_id","reply_datetime"],ignore_index=True)


    # cleanse html
    re_strip_html_tags = re.compile(r'<[ A-Za-z0-9\-\"\'\\\/=]+>')
    df["reply"] = df["reply"].apply(execute_regex,args=[re_strip_html_tags])
    print(df)

    # delete duplicates
    print('Dupes')
    print(f'Before: {df.shape}')
    df = df.drop_duplicates()
    print(f'After:  {df.shape}')
    print(df)

    # delete empty utterances
    print('Empty')
    print(f'Before: {df.shape}')
    df = df[~(df["reply"]=="")]
    df = df[~(df["reply"].isna())]
    df = df[~(df["reply"]=="nan")]
    print(f'After:  {df.shape}')

    # workout an output
    output_filename = filename.replace(".csv","_output.csv")
    assert output_filename != filename
    df.to_csv(output_filename,header=True,index=False)

def execute_regex(text_to_run_on: str, re_to_run: re) -> str:
    """Executes a compiled regex on a text"""
    return re_to_run.sub('',str(text_to_run_on))



if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
