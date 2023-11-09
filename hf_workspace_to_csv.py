"""
python write_csv.py

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
import json

# third part imports
import click
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-o', '--output_dir', type=str, default="./data", help='Output file path')
@click.option('-l','--delimiter',type=str,default="-",help='Intent name delimiter')
@click.option('--include_intent_tags', type=str, default="",
              help='Comma delimited list of include intent tags to filter the output by')
@click.option('--exclude_intent_tags', type=str, default="",
              help='Comma delimited list of exclude intent tags to filter the output by')
@click.option('--include_utterance_tags', type=str, default="",
              help='Comma delimited list of include utterance tags to filter the output by')
@click.option('--exclude_utterance_tags', type=str, default="",
              help='Comma delimited list of exclude utterance tags to filter the output by')
def main(username: str, password: int, namespace: bool, playbook: str, output_dir: str,
         include_intent_tags: str,
         exclude_intent_tags: str,
         include_utterance_tags: str,
         exclude_utterance_tags: str,
         delimiter: str
         ):
    """Main Function"""

    tag_filters = humanfirst.objects.HFTagFilters()
    tag_filters.set_tag_filter("intent", "include", include_intent_tags)
    tag_filters.set_tag_filter("intent", "exclude", exclude_intent_tags)
    tag_filters.set_tag_filter("utterance", "include", include_utterance_tags)
    tag_filters.set_tag_filter("utterance", "exclude", exclude_utterance_tags)
    print(tag_filters)
    write_csv(username, password, namespace, playbook, output_dir, tag_filters, delimiter=delimiter)


def write_csv(username: str,
              password: int,
              namespace: bool,
              playbook: str,
              output_dir: str,
              tag_filters: list,
              delimiter: str) -> None:
    """Writes the HF workspace to a CSV file and stores it in the output path
    CSV will contain intent_id, intent_name, for every example
    along with any intent level and utterance level metadata as columns"""

    if not output_dir.endswith('/'):
        output_dir = output_dir + '/'

    # Download playbook as json
    hf_api = humanfirst.apis.HFAPI(username, password)
    playbook_dict = hf_api.get_playbook(namespace, playbook)
    labelled_workspace = humanfirst.objects.HFWorkspace.from_json(playbook_dict,delimiter=delimiter)
    assert isinstance(labelled_workspace, humanfirst.objects.HFWorkspace)
    output_path_json = f'{output_dir}{namespace}-{playbook_dict["name"]}.json'

    # Write json version
    with open(output_path_json, mode="w", encoding="utf8") as f:
        json.dump(playbook_dict, f, indent=3)
    print(f'Wrote json version of playbook: {output_path_json}')

    # Write csv version
    output_path_csv = f'{output_dir}{namespace}-{playbook_dict["name"]}.csv'
    labelled_workspace.write_csv(output_path_csv, tag_filters=tag_filters)
    print(f"Wrote CSV file to: {output_path_csv}")


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
