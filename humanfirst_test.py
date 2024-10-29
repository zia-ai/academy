"""
Set of pytest humanfirst.py tests

"""
# ***************************************************************************80**************************************120

# standard imports
import os
import json

# 3rd party imports
import pandas
import humanfirst
import requests

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

def test_override_timeouts():
    """Test that we can override some timeouts"""

    # start with very small timeout
    hf_api = humanfirst.apis.HFAPI(timeout=0.5)
    assert isinstance(hf_api,humanfirst.apis.HFAPI)
    
    # this should fail then
    try:
        response = hf_api.list_playbooks(namespace='humanfirst')
        # raise RuntimeError("Didn't time out")
    except requests.exceptions.ReadTimeout as e:
        print("Correctly timed out")
        print(e)
    
    # then should pass when we override at function level TODO: need to parameterize into a constant
    response = hf_api.list_playbooks(namespace='humanfirst',timeout=20)
    assert len(response) > 0
    
    # start with large timeout
    hf_api = humanfirst.apis.HFAPI(timeout=360)
    assert isinstance(hf_api,humanfirst.apis.HFAPI)
    assert len(response) > 0
    
    # TODO: turn logging on and off here with the variables