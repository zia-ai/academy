"""
python csv_to_json_unlabelled_hb.py

"""
# *********************************************************************************************************************

# standard imports
import re
from dateutil import parser

# 3rd party imports
import pandas
import click
import codepage

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-b', '--botname', type=str, required=True, help='Input botname')
@click.option('-u', '--username', type=str, required=False,
              default='Firstname Lastname', help='Input botname')
def main(filename: str,
         botname: str,
         username: str) -> None:
    """Main Function"""

    # read excel
    assert filename.endswith('xlsx')
    df = pandas.read_excel(filename, dtype=str)
    assert isinstance(df, pandas.DataFrame)
    df.fillna('', inplace=True)

    # fix codepage errors from excel
    df["codepage"] = df["Transcript"].apply(codepage.fixit)

    # replace all new lines
    df["codepage_nl"] = df["codepage"].str.replace("\n","")

    # add newlines from html
    df["codepage_nl_br"] = df["codepage_nl"].str.replace("<br>","\n")

    # strip HTML

    re_strip_html_tags = re.compile(r'<[ A-Za-z0-9\-\"\'\\\/=]+>')
    df["codepage_nl_br_html"] = df["codepage_nl_br"].apply(execute_strip,args=[re_strip_html_tags])

    # Split
    initial_date_defintion = '[ ]*\[[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}\]:[ ]*'
    regex_string = f'(^{botname}|^{username}|\|\|{botname}|\|\|{username}|{initial_date_defintion})'
    print(regex_string)
    re_split = re.compile(regex_string)
    df["split"] = df["codepage_nl_br_html"].apply(execute_split,args=[re_split])

    # Consolidate
    second_date_definition = '\[([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\]:'
    re_get_date = re.compile(second_date_definition)
    re_is_name = re.compile(f'{botname}|{username}')
    df["consolidate"] = df["split"].apply(consolidate,args=[re_get_date,re_is_name])

    # Explode rows
    df = df.explode("consolidate",ignore_index=True)

    # Split three col
    df['role_name'], df['isodate_string'], df['cleansed_text'] = zip(*list(df['consolidate'].values))

    # strip back
    df = df[['Conversation Id','isodate_string','role_name','cleansed_text']]

    # output file
    print(df)
    output_file_name = filename.replace(".xlsx",".csv")
    assert output_file_name != filename
    df.to_csv(output_file_name,index=False)
    print(f'Wrote to {output_file_name}')

def consolidate(split_list: list, re_get_date: re, re_is_name: re):
    "Consolidates name, role, text"
    output_list = []
    for split in split_list:
        if re_is_name.match(split):
            name = split
            continue
        if re_get_date.match(split):
            date = re_get_date.match(split)[1]
            date = parser.parse(date).isoformat()
            continue
        text = split
        output_list.append([name,date,text])
        name = None
        date = None
        text = None
    return output_list




def execute_split(text_to_run_on: str, re_to_run: re) -> list:
    """Executes a compiled regex on a text"""

    candidate = re_to_run.split(text_to_run_on)
    assert isinstance(candidate,list)
    output = []
    for c in candidate:
        try:
            if c is None:
                continue
            assert isinstance(c,str)
            if c in ['','||']:
                continue
            if c.startswith('||'):
                c = c.replace('||','')
            output.append(c.strip())
        except AssertionError:
            print(c)
            quit()
    return output

def execute_strip(text_to_run_on: str, re_to_run: re) -> str:
    """Executes a compiled regex sub on a text replacing with nothing"""
    return re_to_run.sub('',text_to_run_on)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
