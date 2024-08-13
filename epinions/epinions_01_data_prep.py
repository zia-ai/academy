"""
python epinions.py

Uses file from here
https://cseweb.ucsd.edu/~jmcauley/datasets.html#social_data

Citation:

SPMC: Socially-aware personalized Markov chains for sparse sequential recommendation
Chenwei Cai, Ruining He, Julian McAuley
IJCAI, 2017

Improving latent factor models via personalized feature projection for one-class recommendation
Tong Zhao, Julian McAuley, Irwin King
Conference on Information and Knowledge Management (CIKM), 2015

Conversion to json from output.

python csv_to_json_unlabelled.py \
-f ./data/epinions/epinions_output.csv \
-m "id,item,user,paid,timestamp,stars" \
-u "words" \
-c "id" \
-t "timestamp" \
-r "item" \
-p "*:client" \
-z

"""
# ******************************************************************************************************************120

# standard imports
import datetime
import numpy

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst

@click.command()
@click.option('-f', '--filename', type=str, required=False, default='./data/epinions/epinions.txt',
              help='Input File Path')
@click.option('-n', '--noskip', is_flag=True, required=False, default=False, help='Input File Path')
def main(filename: str, noskip: bool) -> None: # pylint: disable=unused-argument
    """Process epinions text"""

    dtypes = {
        "item": str,
        "user": str,
        "paid": numpy.float64,
        "timestamp": datetime.datetime,
        "stars": numpy.float64,
        "words": str
    }

    # parse each line in the format expected - frigged file to remove "/n " with " " using Textpad
    # note - unclear why won't parse original file - haven't done byte level check
    file_in = open(filename,mode='r',encoding="utf8")

    short = 0
    errors = 0
    completed = 0
    rows = []
    for i,line in enumerate(file_in):
        # skip header "#item, user, paid, time, stars, words"
        if i == 0:
            continue

        # do split
        split_line = line.split(' ')

        # skip short lines
        if len(split_line) < 5:
            short = short + 1
            print(line)
            continue

        # try and get the data from lines with enouh cols.
        row = {}
        try:

            for j,col in enumerate(dtypes.keys()):
                if j in [0,1,2,4]:
                    row[col] = split_line[j]
                elif j == 3:
                    row[col] = datetime.datetime.fromtimestamp(int(split_line[j]))
                else:
                    row[col] = ' '.join(split_line[5:]).strip()
            assert isinstance(row[col],dtypes[col]) # pylint: disable=undefined-loop-variable
            rows.append(row.copy())
            completed = completed + 1
        except Exception as e: # pylint: disable=broad-exception-caught
            print("EXCEPTION")
            print(f'row:           {i}')
            print(f'split_line[j]: {split_line[j]}')
            print(f'Exception:     {e}')
            errors = errors + 1
            print("Line:")
            print(line)
            if noskip:
                quit()
    file_in.close()

    # make dataframe
    df = pandas.DataFrame(rows)

    # approximate paid to cleaner ints
    df["paid"] = df["paid"].str.extract('([0-9]+)')
    df["paid"].fillna(0)
    df.loc[df["paid"].isna(),"paid"] = 0
    df["paid"] = df["paid"].astype(int)

    # stars to ints
    df["stars"] = df["stars"].astype(float).astype(int)

    # drop any blank lines
    df = df[~(df["words"]=="")]

    # generate unique_id
    df["id"] = df["words"].apply(humanfirst.objects.hash_string)

    # print the Dataframe
    print(df)

    # write it somewhere
    output_filename = filename.replace('.txt','_output.csv')
    assert filename != output_filename
    df.to_csv(output_filename,index=False,header=True)
    print(f'Wrote to: {output_filename}')

    print(f'Too short:         {short}')
    print(f'Errors:            {errors}')
    print(f'Completed:         {completed}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
