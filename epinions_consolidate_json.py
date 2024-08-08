"""
python epinions_consolidate

"""
# ******************************************************************************************************************120

# standard imports
import os
import json

# 3rd party imports
import click
import pandas

SCHEMA_KEYS = ["review_id","item_code","date_drafted","stars_rating", 
               "amount_paid","review","category","manufacturer","model","title"]

# custom imports

@click.command()
@click.option('-d', '--directory', type=str, required=False,
              default='./data/epinions/json', help='Input Directory')
@click.option('-s', '--sample', type=int, required=False,
              default=0, help='Sampling ')
def main(directory: str, sample: int) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # list the files
    file_list = os.listdir(directory)

    # process one by one
    json_objs = []
    counter = 0
    for file_name in file_list:

        # exit if sample size reached
        if sample > 0 and counter >= sample:
            break

        # get FQFN check is JSON and open it.
        fqfn = os.path.join(directory,file_name)
        if fqfn.endswith(".json") and not fqfn.endswith("error.json"):
            with open(fqfn,mode='r',encoding='utf8') as file_in:

                # load the file
                json_in = json.load(file_in)
                assert isinstance(json_in, dict)
                json_out = {}

                # "" from this below is then completely matching based on symetric_difference from sets
                # anything else shows the differences
                json_out["schema_analysis"] = ",".join(list(set(json_in.keys()).symmetric_difference(set(SCHEMA_KEYS))))

                # Test the result
                if json_out["schema_analysis"] == "":
                    json_out["valid"] = True
                else:
                    json_out["valid"] = False
                for key in SCHEMA_KEYS:
                    if key in json_in.keys():
                        json_out[key] = json_in[key]
                    else:
                        json_out[key] = None
                json_objs.append(json_out.copy())
        counter = counter + 1

    df = pandas.json_normalize(json_objs)
    print(df.columns)
    print(df)

    gb = df[["valid","review_id"]].groupby("valid").count()
    print(gb)

    # output
    output_filename = os.path.join(directory,"output.csv")
    df.to_csv(output_filename,header=True,index=False)
    print(f'Wrote to: {output_filename}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
