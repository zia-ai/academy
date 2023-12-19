# pylint: disable=invalid-name
"""
python ./summarize/01_summarize_transcripts_generic_directory.py

Looks through the summaries directory and turns any *.txt files there
into an unlabelled workspace with a metadata field of the original id

"""
# ********************************************************************************************************************

# standard imports
import os
import datetime

# 3rd party imports
import click
import pandas
import humanfirst


@click.command()
@click.option('-s', '--summaries_dir', type=str, default='./summaries', help='Summaries input file path')
@click.option('-w', '--workspaces_dir', type=str, default='./workspaces', help='Workspace output file path')
@click.option('-e', '--explode', is_flag=True, type=bool, default=False,
              help='Explode a - bulletted list stripping bullets')
@click.option('-m', '--mapper', type=str, default='',
              help='CSV File to lookup ID and produce an intent for that ID from it')
@click.option('-i', '--id_col_name', type=str, default='', help='Column in csv file with context-id in')
@click.option('-c', '--map_col_name', type=str, default='', help='Column in csv file to map to from context-id')
def main(summaries_dir: str, workspaces_dir: str, explode: bool, mapper: str, id_col_name: str, map_col_name: str):
    '''Main function'''

    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not dir_path.endswith("/"):
        dir_path = f'{dir_path}/'

    # find summary directory
    summaries_dir = check_directory(dir_path, summaries_dir)

    # read all the files
    file_names = os.listdir(summaries_dir)
    completed_ids = []
    summaries = {}
    for file_name in file_names:
        if file_name.endswith(".txt"):
            completed_id = file_name[0:-4]
            completed_ids.append(completed_id)
            file_name = f'{summaries_dir}{file_name}'
            file = open(file_name, mode='r', encoding='utf8')
            summaries[completed_id] = file.read()
            file.close()

    # declare an unlabelled workspace
    unlabelled = humanfirst.objects.HFWorkspace()

    # get mapper
    if mapper != '':
        assert id_col_name != ''
        assert map_col_name != ''
        df = pandas.read_csv(mapper, index_col=id_col_name, usecols=[
                             id_col_name, map_col_name], encoding='utf8')
        mapper = df.to_dict(orient='dict')
        mapper = mapper["intent_name_text"]
        assert isinstance(mapper, dict)
        # print(f'Using mapper with {len(list(mapper.keys()))} values')

    # create an example for each file
    for c in completed_ids:

        # explode data
        if explode:
            examples = str(summaries[c]).split('\n')
        else:
            examples = [summaries[c]]

        # build examples
        for i, e in enumerate(examples):

            # different formats if exploding
            if explode:
                if e.startswith('- '):
                    e = e[2:]
                example_id = f'{c}-{i}'
                context = humanfirst.objects.HFContext(
                    context_id=c, type='conversation', role='client')
            else:
                example_id = c
                context = {}

            # skip blanks
            if e == '':
                continue

            # deal with mapper
            if mapper != '' and isinstance(mapper, dict):
                intents = [unlabelled.intent(f'{c}_{mapper[c]}')]
            else:
                intents = []

            example = humanfirst.objects.HFExample(
                text=e,
                id=example_id,
                created_at=datetime.datetime.now(),
                intents=intents,
                tags=[],
                metadata={
                    "id": c
                },
                context=context
            )

            # add example to workspace
            unlabelled.add_example(example)

    print(f'Processed {len(unlabelled.examples)} examples')

    # work out a file name
    output_file_candidate = summaries_dir.strip("./")
    output_file_candidate = "_".join(output_file_candidate.split("/"))
    workspaces_dir = check_directory(dir_path, workspaces_dir)
    output_file_name = f'{workspaces_dir}{output_file_candidate}.json'
    print(output_file_name)

    # write to filename
    file_out = open(f'{output_file_name}', mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    print(f'Wrote to {output_file_name}')
    file_out.close()


def check_directory(dir_path: str, dir_string: str) -> str:
    '''Check directory local or absolute properties'''
    if dir_string.startswith('./'):
        dir_string = dir_string[2:]
        dir_string = f'{dir_path}{dir_string}'
    if not dir_string.endswith('/'):
        dir_string = f'{dir_string}/'
    return dir_string


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
