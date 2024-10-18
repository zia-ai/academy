"""
Test key script csv_to_json_unlabelled_test.py

"""
# ***************************************************************************80**************************************120

# standard imports
import os
import json
import shutil
import datetime
from dateutil import parser

# 3rd party imports
import pytest
import jsondiff 

# custom imports
import csv_to_json_unlabelled # file under test

CLOCK_TICK_ZERO = "1970-01-01T00:00:00Z"
assert isinstance(parser.parse(CLOCK_TICK_ZERO),datetime.datetime)

@pytest.fixture
def source_files():
    """A pytest fixture to create a dict of the files in use for test and 
    clean them up after yielding"""
    # locate where we are
    here = os.path.abspath(os.path.dirname(__file__))
    
    # test csv input file
    source=os.path.join(here,'examples','ExampleUpload2.csv')
    # expected output file we don't want to overwrite as a json
    expected_result=os.path.join(here,'examples','ExampleUpload2.json')
    # Where we copy our test data to
    test_file=os.path.join(here,'examples','ExampleUpload2_pytest.csv')
    # Where the output will be generated.
    actual_output=os.path.join(here,'examples','ExampleUpload2_pytest.json')
    
    shutil.copy(source,test_file)
    
    source_files = {
        "test_file": test_file,
        "expected_result": expected_result,
        "actual_output": actual_output
    }
    
    yield source_files
    
    # cleanup
    for f in [test_file,actual_output]:
        if os.path.isfile(f):
            os.remove(f)

def test_csv_to_json_unlabelled_no_role_mapper_with_perf_test(source_files):
    """test_load_testdata  equiv to:
python csv_to_json_unlabelled.py \
-f ./examples/ExampleUpload2.csv \
-m "myid,scenario" \
-u "text" \
-c "myid" \
-t "somedate" \
-r "rolehere" \
-e "utf8" \
-y"""

    # see keys in test file
    process_response = csv_to_json_unlabelled.process(
        filename=source_files["test_file"],
        metadata_keys="myid,scenario",
        utterance_col="text",
        convo_id_col="myid",
        created_at_col="somedate",
        # unix_date
        role_col="rolehere",
        # role_mapper
        encoding="utf8",
        #filtering
        # striphtml
        # drop+blanks
        # minimize_meta
        why_so_long=True
    )
    # Should have got an integer
    assert isinstance(process_response, int)
    assert process_response > 0
    print(process_response)
    assert(_verify_output_as_expected(source_files["expected_result"],
                                      source_files["actual_output"]))
    


def _check_and_reset_datetimes(json_as_dict: dict, key_name: str) -> dict:
    """Recurisvely Checks every object for the key_name and if found:
    - checks it parses to a valid date
    - sets it to clock tick 1 in isoformat string
    
    This may be useful for letting you compare jsons with recent date fields."""
    for key in json_as_dict.keys():
        if isinstance(json_as_dict[key],dict):
            json_as_dict[key] = _check_and_reset_datetimes(json_as_dict[key],key_name)
        if key == key_name:
            # check it's a date and parses
            assert isinstance(parser.parse(json_as_dict[key],datetime.datetime))
            json_as_dict[key] = CLOCK_TICK_ZERO
            

def _verify_output_as_expected(expected_output: str, actual_output: str):
    """Normalise datefield sand compare the output json to expected"""
    expected_json = json.load(open(expected_output,mode="r",encoding="utf8"))
    expected_json = _check_and_reset_datetimes(expected_json,"loaded_date")
    actual_json = json.load(open(actual_output,mode="r",encoding="utf8"))
    actual_json = _check_and_reset_datetimes(actual_json,"loaded_date")
    
    if expected_json == actual_json:
        return True
    else:
        print(jsondiff.diff(expected_json,actual_json))
        return False