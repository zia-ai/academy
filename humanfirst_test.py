"""
Set of pytest humanfirst.py tests

"""
# ***************************************************************************80**************************************120

# standard imports
import os
import json
import sys
import time


# 3rd party imports
import pandas
import humanfirst
import requests
import pytest

# custom imports
import simple_json_labelled

# This is needed to run some tests - export it a sa ENV variable
TEST_NAMESPACE = os.environ.get("HF_TEST_NAMESPACE")
HF_LOG_FILE_ENABLE= os.environ.get("HF_LOG_FILE_ENABLE")
HF_LOG_CONSOLE_ENABLE =   os.environ.get("HF_LOG_CONSOLE_ENABLE")
HF_LOG_DIR = os.environ.get("HF_LOG_DIR")
HF_LOG_LEVEL = os.environ.get("HF_LOG_LEVEL")
HF_PASSWORD = os.environ.get("HF_PASSWORD")
HF_USERNAME = os.environ.get("HF_USERNAME")

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

@pytest.mark.skipif(not TEST_NAMESPACE, 
                    reason="requires HF_TEST_NAMESPACE set to a valid namespace in the environment with some playbooks/workspaces")
def test_override_timeouts():
    """Test that we can override some timeouts
    requires HF_TEST_NAMESPACE env variable to be set
    for instance to humanfirst or the default namespace
    of your humanfirst organisation""" 
        
    # start with very small timeout
    hf_api = humanfirst.apis.HFAPI(timeout=0.5)
    assert isinstance(hf_api,humanfirst.apis.HFAPI)
    
    # this should fail then
    try:
        response = hf_api.list_playbooks(namespace=TEST_NAMESPACE)
        # raise RuntimeError("Didn't time out")
    except requests.exceptions.ReadTimeout as e:
        print("Correctly timed out")
        print(e)
    
    # then should pass when we override at function level TODO: need to parameterize into a constant
    response = hf_api.list_playbooks(namespace=TEST_NAMESPACE,timeout=20)
    assert len(response) > 0
    
    # start with large timeout
    hf_api = humanfirst.apis.HFAPI(timeout=360)
    assert isinstance(hf_api,humanfirst.apis.HFAPI)
    assert len(response) > 0
    
# requires as env variables
@pytest.mark.skipif(not TEST_NAMESPACE # eg humanfirst or your org namespace
                    or not HF_LOG_CONSOLE_ENABLE # TRUE 
                    or not HF_LOG_LEVEL # DEBUG
                    or not HF_USERNAME # your username
                    or not HF_PASSWORD, # your password 
                    reason="requires HF_TEST_NAMESPACE, HF_USERNAME, HF_PASSWORD, HF_LOG_CONSOLE_ENABLE and HF_LOG_LEVEL to generate logs")
def test_console_logging():
    """Run test with -s to see a demonstration of console logging"""
    # Demo only - no asserts.
    
    # authorise
    hf_api = humanfirst.apis.HFAPI()   
    
    # run a call
    hf_api.list_playbooks(namespace=TEST_NAMESPACE)

    
# requires as env variables
@pytest.mark.skipif(not TEST_NAMESPACE # eg humanfirst or your org namespace
                    or not HF_LOG_FILE_ENABLE # TRUE 
                    or not HF_LOG_LEVEL # DEBUG
                    or not HF_LOG_DIR # some directory to check empty for instance ./data/logs
                    or not HF_USERNAME # your username
                    or not HF_PASSWORD, # your password 
                    reason="requires HF_TEST_NAMESPACE, HF_USERNAME, HF_PASSWORD, HF_LOG_FILE_ENABLE, HF_LOG_DIR and HF_LOG_LEVEL to generate logs")
def test_file_logging():
    """Run test to demonstrate file logging
    Warning will delete any pre-existing logs in the log dir passed"""
    
    hf_api = humanfirst.apis.HFAPI()   
    hf_api.list_playbooks(namespace=TEST_NAMESPACE)
    
    # check files after
    list_all_files_after = os.listdir(HF_LOG_DIR)
    list_log_files_after = []
    for f in list_all_files_after:
        if f.endswith(".log"):
            list_log_files_after.append(os.path.join(HF_LOG_DIR,f))
    assert len(list_log_files_after) > 0
       
    # test contents
    for f in list_log_files_after:
        with open(f,mode="r",encoding="utf8") as log_file_in:
            contents = log_file_in.read()
            print(f"\n\nFile: {f}")
            if len(contents) > 0:
                print(contents)
            else:
                print("empty")    
        
        # cleanup
        os.remove(f)