"""
tagging labelled utterances

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# ******************************************************************************************************************120

# standard imports
import os
import datetime

# third part imports
import click
import humanfirst


@click.command()
@click.option('-t', '--tag_name', type=str, required=True,
              help='Tag name')
@click.option('-d', '--delimiter', type=str, default="-",
              help='Delimiter used in intent name')
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True,
              help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True,
              help='HumanFirst playbook id')
def main(tag_name: str, delimiter: str,
         username: str, password: str,
         namespace: str, playbook: str):
    '''Main function'''

    hf_api = humanfirst.apis.HFAPI(username=username, password=password)
    playbook_dict = hf_api.get_playbook(namespace, playbook)
    labelled_workspace = humanfirst.objects.HFWorkspace.from_json(playbook_dict,delimiter=delimiter)
    hf_tag = labelled_workspace.tag(tag=tag_name)
    for idx,_ in labelled_workspace.examples.items():
        labelled_workspace.examples[idx].tags.append(hf_tag)

    now = datetime.datetime.now().isoformat()
    filepath = os.path.join("./data",f"tagging_all_labelled_utterances_{namespace}_{playbook}_{now}.json")
    file_out = open(filepath,
                    mode='w',
                    encoding='utf8')
    labelled_workspace.write_json(file_out)
    print(f"The file is written at {filepath}")
    file_out.close()


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
