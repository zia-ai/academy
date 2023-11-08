"""
Set of pytest humanfirst.py tests

"""
# ***************************************************************************80**************************************120

# standard imports
import os
import json

# 3rd party imports
import pandas

# custom imports
import simple_json_labelled

# locate where we are
here = os.path.abspath(os.path.dirname(__file__))

def test_load_testdata():
    """test_load_testdata"""

    dtypes = {
        'external_id': str,
        'timestamp': str,
        'utterance': str,
        'speaker': str,
        'nlu_detected_intent': str,
        'nlu_confidence': str,
        'overall_call_star_rating': int
    }

    # read the input csv
    path_to_file=os.path.join(here,'examples','simple_example.csv')
    df = pandas.read_csv(path_to_file,
                         encoding='utf8', dtype=dtypes)
    assert isinstance(df, pandas.DataFrame)
    assert df.shape == (5, 7)

def test_write_json():
    """test_write_json"""

    # delete output file so can sure we are testing fresh each time
    output_file = os.path.join(here, "examples", "json_model_example_output.json")
    if os.path.exists(output_file):
        os.remove(output_file)
    input_file = os.path.join(here, "examples", "json_model_example.json")
    input_json = json.loads(open(input_file, 'r', encoding='utf8').read())

    simple_json_labelled.process(input_json, input_file, create_date=True)
    assert os.path.isfile(output_file)
    output_json = json.loads(open(output_file, 'r', encoding='utf8').read())
    examples = output_json["examples"]
    assert examples[0]["created_at"] == "2023-06-05T14:26:07+00:00"
    assert examples[1]["created_at"] == "2023-06-05T14:26:08+00:00"
