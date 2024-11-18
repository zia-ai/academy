# pylint: disable=invalid-name
"""
Checks for a single json or a folder of JSON and assuming humanfirst json format 
Assembles them into a single CSV contains the correct "examples" information

Optionally search through for a set of keys from a single column - i.e 
if trying to find certain call numbers

python ./json_to_csv.py
       -f <YOUR FILENAME or FOLDER>



"""
# *********************************************************************************************************************

# standard imports
import os

# third party imports
import click
import pandas
import json

# custom imports


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File or Folder')
@click.option('-m', '--max_files', type=int, required=False, default=0, help='Maximum number of files to run on')
@click.option('-s', '--search_col', type=str, required=False, default="", help='Optional column to build a separate CSV of')
@click.option('-v', '--search_values', type=str, required=False, default="", help='Comma delimited set of values to run on')
def main(input_filename: str, max_files: int,
         search_col: str, search_values: str):
    """In the case of a folder will run for all files in that folder"""
    
    # check if file or folder and process loop round
    if os.path.isdir(input_filename):
        list_files = os.listdir(input_filename)
        json_files = []
        for f in list_files:
            if f.endswith(".json"):
                json_files.append(os.path.join(input_filename,f))
        if len(json_files) == 0:
            raise RuntimeError(f"No json files to process in {input_filename}")
        else:
            print(f"JSON number of files to process: {len(json_files)}")
        df = pandas.DataFrame()
        for i,jf in enumerate(json_files):
            if max_files > 0 and i >= max_files:
                print(f'Max files reached')
                break
            print(f'{i:03} Beginning work on {jf}')
            df = pandas.concat([df,process_file(jf)])            
        output_filename = os.path.join(input_filename,"collated_output.csv")
    elif os.path.isfile(input_filename):
        df = process_file(input_filename)
        output_filename = input_filename.replace(".json", "_output.csv")

    else:
        raise RuntimeError(f"This string does not appear to be a file or folder: {input_filename}")
    
    # print 
    print(df)
    
    # Do any searches
    if search_col != "":
        if search_values == "":
            raise RuntimeError(f'If search_col is provided at least one search value must be passed')
        
        list_cols = df.columns.to_list()
        assert search_col in list_cols
        
        # get search values
        search_values = search_values.split(",")
        assert isinstance(search_values,list)
        assert len(search_values) >= 1
        print(f'Search values contains this many values to search for: {len(search_values)}')
        
        
        # do the search
        df_values = search_for_values(df,search_col,search_values)

        # check the results
        if df_values.shape[0] == len(search_values):
            print("Found all values")
        else:
            print(f"Found: {df_values.shape[0]} out of: {search_values}")
        print(df_values)
        
        write_output(df_values,input_filename,output_filename.replace(".csv","_searched.csv"))    
    write_output(df,input_filename,output_filename)    
    
    
    
def search_for_values(df: pandas.DataFrame, search_col: str, search_values: list) -> pandas.DataFrame:
    return df[df[search_col].isin(search_values)].copy(deep=True)   
    
def process_file(input_filename: str) -> pandas.DataFrame:
    file = open(input_filename, mode = "r", encoding = "utf8")
    workspace_json = json.load(file)
    file.close()
    return pandas.json_normalize(workspace_json["examples"])

def write_output(df: pandas.DataFrame, input_filename: str, output_filename: str):
    assert input_filename != output_filename
    df.to_csv(output_filename,index=False, header=True)
    print(f'Wrote to: {output_filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter