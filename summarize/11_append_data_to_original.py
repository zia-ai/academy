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

    # read summaries
    assert os.path.isdir(summaries_dir)
    file_names = os.listdir(summaries_dir)
    print(f'Total filenames: {len(file_names)}')
    completed_ids = []
    summaries = []
    explanations = []
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            completed_ids.append(completed_id)
            file_name = os.path.join(summaries_dir,file_name)
            file = open(file_name, mode='r', encoding='utf8')
            text = file.read().strip("\n")
            if filter_json:
                if text.find(JSON_START) == 0:
                    if text.endswith(JSON_END):
                        summary = text[len(JSON_START):-len(JSON_END)]
                        summaries.append(json.dumps(json.loads(summary)))
                        explanation = ''
                        print("YOOOOO")
                elif text.find(JSON_START) > 0:
                    explanation,summary_candidate = text.split(JSON_START)
                    if summary_candidate.endswith(JSON_END):
                        summary = summary_candidate[0:-len(JSON_END)]
                        summaries.append(json.dumps(json.loads(summary)))
                    else:
                        summary = summary_candidate.split(JSON_END)[0]
                        summaries.append(json.dumps(json.loads(summary)))
                        explanation = explanation + summary_candidate.split(JSON_END)[1]
                else:
                    summary = ''
                    summaries.append(summary)
                    explanation = text
                explanations.append(explanation)
            else:
                summaries.append(text)
            file.close()

    # turn that into new dataframe
    if filter_json:
        print('DF Generationa for Filter JSON')
        df_newdata = pandas.DataFrame(zip(completed_ids,summaries,explanations),
                                      columns=[index_col,column_target_name,f'{column_target_name}_explanation'])
    else:
        print('DF Generationa for for normal mode')
        df_newdata = pandas.DataFrame(zip(completed_ids,summaries),columns=[index_col,column_target_name])
        print(df)
    # Set index
    if index_is_integer:
        print('Transforming index')
        df_newdata[index_col] = pandas.to_numeric(df_newdata[index_col])
    print(f'Setting index on new data: {index_col}')
    df_newdata.set_index(index_col,inplace=True)
    df_newdata.sort_index(inplace=True)

    print("NEWDATA:")
    print(df_newdata)

    print("EXISTING_DATA:")
    print(df)

    # join that to original
    df = df.join(df_newdata)
    print(df[~df["PPE"].isna()])

    # write to output
    assert jointo != output_file_name
    df.to_csv(output_file_name, header=True,index=True)
    print(f'Wrote to: {output_file_name}')

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
