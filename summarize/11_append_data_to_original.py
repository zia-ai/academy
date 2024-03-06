# pylint: disable=invalid-name
"""
python ./summarize/11_append_data_to_original.py       # pylint: disable=invalid-name

Adds summarises to an output file version
"""
# ********************************************************************************************************************

# standard imports
import os
import json

# 3rd party imports
import click
import pandas

JSON_START = '```json'
JSON_END = '```'

@click.command()
@click.option('-j', '--jointo', type=str, required=True, help='Path of original data to join to')
@click.option('-i', '--index_col', type=str, required=True,
              help='index column containing keys matching filenames in summaries')
@click.option('-s', '--summaries_dir', type=str, required=True, help='Summaries input file path')
@click.option('-c', '--column_target_name', type=str, required=True, help='Column name to add to original')
@click.option('-f', '--filter_json', is_flag=True, required=False, default=False, help='Whether to extract json')
@click.option('-g', '--index_is_integer', is_flag=True, required=False,
              default=False, help='If index column is an integer')
def main(jointo:str,
         index_col: str,
         summaries_dir: str,
         column_target_name: str,
         filter_json: bool,
         index_is_integer: bool):
    '''Main function'''

    # read original workspace
    if jointo.endswith(".xlsx"):
        print("XLSX mode")
        df = pandas.read_excel(jointo,dtype=str)
        output_file_name = jointo.replace(".xlsx","_output.csv")
    elif jointo.endswith(".csv"):
        print("CSV mode")
        df = pandas.read_csv(jointo,encoding='utf8',dtype=str)
        output_file_name = jointo.replace(".csv","_output.csv")
    else:
        raise RuntimeError(f"Unsupported file type {jointo}")
    if index_is_integer:
        df = df[~df[index_col].isna()]
        print(df[index_col].unique())
        df[index_col] = pandas.to_numeric(df[index_col])

    df.set_index(index_col,inplace=True,drop=True)
    df.sort_index(inplace=True)
    df = df.fillna("")

    # read summaries
    assert os.path.isdir(summaries_dir)
    file_names = os.listdir(summaries_dir)
    print(f'Total filenames: {len(file_names)}')
    completed_ids = []
    summaries = []
    explanations = []
    output_formats = []
    for file_name in file_names:
        if file_name.endswith(".txt"):

            # extract ID
            completed_id = file_name.replace(".txt","")
            completed_ids.append(completed_id)

            # read file
            file_name = os.path.join(summaries_dir,file_name)
            file = open(file_name, mode='r', encoding='utf8')

            # strip text
            text = file.read().strip("\n")

            # determine whether there is a json subsection or just read the whole thing.
            output_format = ""
            if filter_json:
                # JSON starts at the beginning
                if text.find(JSON_START) == 0:
                    # And finishes at the end then the whole thing is JSON
                    if text.endswith(JSON_END):
                        summary = text[len(JSON_START):-len(JSON_END)]
                        summaries.append(json.dumps(json.loads(summary)))
                        explanation = ''
                        output_format = "JSON_WHOLE_THING"
                    # otherwise it is followed by explanation.
                    else:
                        summary = summary_candidate.split(JSON_END)[0]
                        summaries.append(json.dumps(json.loads(summary)))
                        explanation = explanation + summary_candidate.split(JSON_END)[1]
                        output_format = "ADDITIONAL_EXPLANATION_AT_END"
                # test for multiple json sections
                elif len(text.split(JSON_START)) > 2:
                    summaries.append("")
                    explanation = text
                    output_format = "TOO_MANY_JSON_EXAMPLES"
                # JSON Starts later on
                elif text.find(JSON_START) > 0:
                    # explanation at the start
                    explanation,summary_candidate = text.split(JSON_START)
                    # if json goes to end
                    if summary_candidate.endswith(JSON_END):
                        summary = summary_candidate[0:-len(JSON_END)]
                        summaries.append(json.dumps(json.loads(summary)))
                        explanation = ""
                        output_format = "EXPLANATION_AT_FRONT_JSON_TILL_END"
                    else:
                        summary = summary_candidate.split(JSON_END)[0]
                        try:
                            summaries.append(json.dumps(json.loads(summary)))
                            explanation = explanation + summary_candidate.split(JSON_END)[1]
                            output_format = "EXPLANATION_AT_FRONT_AND_AT_END"
                        except json.decoder.JSONDecodeError:
                            summaries.append("")
                            explanation = summary_candidate
                            output_format = "JSON_DECODE_ERROR"

                # Else we didn't find a JSON start
                else:
                    summary = ''
                    summaries.append(summary)
                    explanation = text
                    output_format = "NO_JSON_START"
            # Just take everything
            else:
                summaries.append(text)
                output_format = "EVERYTHING"
            explanations.append(explanation)
            output_formats.append(output_format)
            print(output_format)
            file.close()

    # check lists
    print(f"explanations:   {len(explanations)}")
    print(f"output_formats: {len(output_formats)}")
    print(f"summaries:      {len(summaries)}")
    assert len(explanations) == len(output_formats)
    assert len(explanations) == len(summaries)

    # turn that into new dataframe
    if filter_json:
        print('DF Generationa for Filter JSON')
        df_newdata = pandas.DataFrame(zip(completed_ids,output_formats,explanations,summaries),
                                      columns=[index_col,
                                               f'{column_target_name}_output_format',
                                               f'{column_target_name}_explanation',
                                               f'{column_target_name}_json']
                                      )

        df_newdata["json_to_json"] = df_newdata[f'{column_target_name}_json'].apply(json_to_json)
        df_newcols = pandas.DataFrame.from_records(df_newdata["json_to_json"])
        df_newdata.drop(columns="json_to_json",inplace=True)
        df_newdata = df_newdata.join(df_newcols)
        df_newdata = df_newdata.fillna("")
    else:
        print('DF Generation for for normal mode')
        df_newdata = pandas.DataFrame(zip(completed_ids,summaries),columns=[index_col,column_target_name])
        print(df)
    # Set index
    if index_is_integer:
        print('Transforming index')
        df_newdata[index_col] = pandas.to_numeric(df_newdata[index_col])
    print(f'Setting index on new data: {index_col}')
    df_newdata.set_index(index_col,inplace=True)
    df_newdata.sort_index(inplace=True)

    print("\nNEWDATA:")
    print(df_newdata)

    print("\nEXISTING_DATA:")
    print(df)

    # join that to original
    df = df.join(df_newdata)
    df = df.fillna("")

    # write to output
    assert jointo != output_file_name
    df.to_csv(output_file_name, header=True,index=True)
    print(f'Wrote to: {output_file_name}')

def json_to_json(json_string: str) -> str:
    """Converts list of dicts to single big dict"""
    if json_string == '':
        return {}
    try:
        json_dict = json.loads(json_string)
    except json.decoder.JSONDecodeError:
        return ["JSON_DECODE_ERROR"]
    return_dict = {}
    if isinstance(json_dict,list):
        for i,obj in enumerate(json_dict):
            assert isinstance(obj,dict)
            for key in obj.keys():
                return_dict[f'{i}_{key}'] = obj[key]
    elif isinstance(json_dict,dict):
        return json_dict
    else:
        return ["UNKNOWN_OBJECT_TYPE"]
    return return_dict

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
