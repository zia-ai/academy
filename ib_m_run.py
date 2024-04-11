# pylint: disable=invalid-name
"""
python ./ib_m.py
       -f <YOUR FILENAME>

"""
# *********************************************************************************************************************
# standard imports

# third party imports
import click
import os
import datetime

# custom imports
import humanfirst


@click.command()
@click.option('-d', '--directory', type=str, required=True, help='Input Dir')
@click.option('-f', '--start_file', type=str, required=True, help='MatchFormat')
@click.option('-t', '--file_type', type=str, required=False, default='.html',
              help='.format default .html')
def main(directory: str, start_file: str, file_type: str):
    """main"""

    # list and filter files
    list_files = os.listdir(directory)
    print(f'Length files: {len(list_files)}')
    to_process = []
    for f in list_files:
        assert isinstance(f,str)
        if f.startswith(start_file):
            to_process.append(f)
    print(f'Length filtered: {len(to_process)}')

    # get a workspace
    unlabelled = humanfirst.objects.HFWorkspace()



    # fill it up.
    for f in to_process:
        file_in = open(os.path.join(f'{directory}',f),mode='r',encoding='utf8')

        # context
        context = humanfirst.objects.HFContext(type="conversation",role="client",context_id=humanfirst.objects.hash_string(f,"convo"))

        example = humanfirst.objects.HFExample(text=file_in.read(),
                                               id=humanfirst.objects.hash_string(f,"example"),
                                               created_at=datetime.datetime.now().isoformat(),
                                               )
        unlabelled.add_example(example)

    # write it
    unlabelled.write_json(open(os.path.join(f'{directory}','output_file.json'),mode='w',encoding='utf8'))

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
