"""
python csv_validate_json.py

"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import pandas

# custom imports

SHOULD_START_WITH = "```json"
SHOULD_END_WITH = "```"

@click.command()
@click.option('-f', '--filename', type=str, required=True, 
              help='Input csv of expected ```json objects from generated tab')
@click.option('-s', '--sample', type=int, required=False, default=0, 
              help='Number to randomly sample, defaults to 0 = no sampling include everything')
def main(filename: str,
         sample: int) -> None: # pylint: disable=unused-argument
    """Produces a report on whether all the JSON objects are valid from a download of generated data"""

    # Read the CSV
    df = pandas.read_csv(filename,encoding="utf8")
    print(df)

    # sample if required
    if sample:
        df = df.sample(sample,random_state=sample)

    # Check if valid
    df = df.apply(validate_json,axis=1)

    # Summarise results
    gb = df[["id","valid","error"]].groupby(["valid","error"]).count()
    with pandas.option_context('display.max_colwidth', 150,):
        print(gb)

def validate_json(row: pandas.Series) -> pandas.Series:
    """Checks for ```json start
    ``` end and then checks rest parses to JSON
    appends text_dict with None if invalid, error type and 
    valid bool"""
    text = row["text"]
    assert isinstance(text,str)
    try:
        if not text.startswith(SHOULD_START_WITH):
            raise RuntimeError(f"Doesn't start with {SHOULD_START_WITH}")
        text = text.replace(SHOULD_START_WITH,"").strip()
        if not text.endswith(SHOULD_END_WITH):
            raise RuntimeError(f"Doesn't end with {SHOULD_END_WITH}")
        text = text.replace(SHOULD_END_WITH,"").strip()
        text_dict = json.loads(text)
        assert isinstance(text_dict,dict)
        row["text_dict"] = text_dict
        row["error"] = ""
        row["valid"] = True
    except RuntimeError as e:
        row["text_dict"] = None
        row["error"] = f'{e}'
        row["valid"] = False
    except json.decoder.JSONDecodeError as e:
        row["text_dict"] = None
        row["error"] = f'JSONDecodeError: {str(e).split(": ")[0]}'
        row["valid"] = False
    return row

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
