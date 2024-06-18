"""
python visualise_p6.py

"""
# ******************************************************************************************************************120

# standard imports
import json
import os
import datetime

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst

@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-o', '--output_dir', type=str, default="./data/", help='Output directory')
def main(username: str,
         password: int,
         namespace: bool,
         playbook: str,
         output_dir: str):
    """Main Function"""

    # check which authorization method using
    hf_api = humanfirst.apis.HFAPI(username, password)
    print('Connected')

    res = hf_api.export_unlabelled_conversations_view(
        namespace=namespace,
        playbook_id=playbook,
        dedup_by_hash=False,
        dedup_by_convo=False)
    print('Download complete')

    df = pandas.json_normalize(data=res.json()["examples"],sep="-")
    print(df.columns)

    # Workout the day the conversation started
    df_date = df[["context-context_id","created_at"]]
    df_date["created_at"] = pandas.to_datetime(df["created_at"])
    df_date = df_date.groupby("context-context_id").min().reset_index()
    df_date.set_index("context-context_id",inplace=True,drop=True)

    # group up convos
    df = df[["context-context_id","metadata-total_score","metadata-issues","metadata-reasoning","id"]].groupby(["context-context_id","metadata-total_score","metadata-issues","metadata-reasoning"]).count()
    df.reset_index(inplace=True)
    df.rename(inplace=True,columns={"id":"count_of_utterances_in_convo"})
    df["metadata-issues"] = df["metadata-issues"].str.split(",")
    df = df.explode(["metadata-issues"],ignore_index=True)
    df["metadata-issues"] = df["metadata-issues"].str.strip(" ")
    df.sort_values(["context-context_id","metadata-issues"],inplace=True,ascending=True,ignore_index=True)
    df.reset_index(inplace=True)
    df.drop(columns="index",inplace=True)

    # set all blank issues to none
    df["metadata-issues"] = df["metadata-issues"].fillna("none")
    df["metadata-issues"] = df["metadata-issues"].replace("","none")

    # join on started at date
    df = df.join(df_date,on="context-context_id")
    df["convo_started_date"] = df["created_at"].dt.date

    print(df)
    print(f'Unique Convos are: {df["context-context_id"].nunique()}')

    output_filename=os.path.join(output_dir,f"{namespace}_{playbook}_{datetime.datetime.now().isoformat()}.csv")
    df.to_csv(output_filename,index=False,header=True)
    print(f'Wrote to: {output_filename}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
