"""
python google_melt_timesheet.py

Takes a timesheet consolidation and melts it and then performs checks

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Filename to melt and analyse')
def main(filename: str):
    """
    melt a timesheet based on filename
    """

    # read dataframe
    df = pandas.read_csv(filename,encoding="utf8")

    # drop total column if exists
    if "total" in df.columns.to_list():
        df.drop(columns="total",inplace=True)

    # melt the days into rows.
    id_vars = ["client_code","task_code","name"]
    value_vars = set(df.columns.to_list()) - set(id_vars)
    df = df.melt(id_vars=id_vars,value_vars=value_vars,var_name="day",value_name="hours_booked")

    # set str day to be int
    df["day"] = df["day"].astype(int)

    # sort
    df.sort_values(["day"]+id_vars,inplace=True)

    # fill na
    df.fillna(0,inplace=True)

    # set the index
    df.set_index(["day"]+id_vars,inplace=True,drop=True)


    # write it
    output_filename = filename.replace(".csv","-melted.csv")
    assert output_filename != filename
    df.to_csv(output_filename,index=True,header=True)
    print(f'Wrote to {output_filename}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
