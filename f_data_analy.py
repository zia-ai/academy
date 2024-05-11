"""
python f_data_anal.py

"""
# ******************************************************************************************************************120

# standard imports
import os
import re

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-d', '--directory', type=str, required=True, help='Dir')
@click.option('-m', '--match_this', type=str, required=True, help='Match')
@click.option('-b', '--break_after', type=int, required=False,default=0, help='Break after')
def main(directory: str, match_this: str, break_after: int) -> None:
    """Main Function"""
    re_fn = re.compile(match_this)
    filenames = os.listdir(directory)
    df = pandas.DataFrame()
    found = 0
    for filename in filenames:
        if re_fn.match(filename):
            print(f'Processing filename: {filename}')
            df_temp = pandas.read_csv(os.path.join(directory,filename),encoding="utf8",low_memory=False,delimiter=";")
            df = pandas.concat([df,df_temp],axis=0)
            found = found + 1
        if break_after > 0 and found == break_after:
            break

    # total messages
    print(f'Total messages: {df.shape}')

    # total conversations
    print(f'Unique content threads: {df["content_thread_id"].nunique()}')

    # create date_day
    df["date_day"] = pandas.to_datetime(df["created_at"],dayfirst=True).dt.date

    # messages a day
    gb1 = df[["date_day","content_thread_id"]].groupby(["date_day"]).count()
    print(gb1)
    print(f'Average messages a day: {gb1["content_thread_id"].mean()}')

    # channels
    gb2 = df[["source_type","content_thread_id"]].groupby(["source_type"]).count().reset_index()
    print(gb2)
    print(gb2["source_type"].unique())

    # converations a day
    gb3 = df[["date_day","content_thread_id","id"]].groupby(["date_day","content_thread_id"]).count().reset_index()
    gb3 = gb3[["date_day","content_thread_id"]].groupby(["date_day"]).count()
    print(gb3)
    print(f'Average convos a day: {gb3["content_thread_id"].mean()}')
    print(f'Sum convos a day: {gb3["content_thread_id"].sum()}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
