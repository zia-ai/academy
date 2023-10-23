"""
python csv_to_json_labelled.py

"""
# *********************************************************************************************************************

# standard imports
import uuid
from datetime import datetime
from typing import Union

# 3rd party imports
import pandas
import click
import humanfirst


@click.command()
@click.option('-f', '--filename', type=str, required=True,
              help='Input File Path')
@click.option('-d', '--delimiter', type=str, required=False, default=",",
              help='Delimiter for the csv file')
@click.option('-m', '--metadata_col', type=str, required=False, default='',
              help='Utterance Metadata Colums: "metadata_col_1,metadata_col_2,...,metadata_col_n"')
@click.option('-u', '--utterance_col', type=str, required=True,
              help='Column name containing utterances')
@click.option('-i', '--intent_col', type=str, required=True,
              help='Column name containing the label name for the intent')
@click.option('-r', '--response_col', type=str, required=False, default="",
              help='Intent Metadta Columns (often response) "metadata_col_1,metadata_col_2,...,metadata_col_n"')
@click.option('-s', '--strip', type=str, required=False, default='',
              help='Strip char and replace with single space')
@click.option('-o', '--sort_col', type=str, required=False, default='',
              help='Sort by this column')
@click.option('-t', '--tag_col', type=str, required=False, default='',
              help='Intent Tag Columns for instance status "tag_col_1,tag_col_2,...,tag_col_n"')
@click.option('-h', '--hierarchical', type=str, required=False, default='',
              help='If to split intent_col by what')
def main(filename: str,
         delimiter: str,
         metadata_col: str,
         utterance_col: str,
         intent_col: str,
         response_col: str,
         strip: str,
         sort_col: str,
         tag_col: str,
         hierarchical: str
         ) -> None:
    """Main Function"""

    # read the input csv
    df = pandas.read_csv(filename, encoding='utf8', delimiter=delimiter)

    # sort if necessary
    if sort_col != '':
        df.sort_values([sort_col, utterance_col], inplace=True)
        df.reset_index(inplace=True)
        print(df)

    utterance_metadata_keys = []
    if metadata_col != '':
        utterance_metadata_keys = metadata_col.split(",")
    intent_metadata_keys = []
    if response_col != '':
        intent_metadata_keys = response_col.split(",")

    # create metadata object per utterance
    df['utterance_metadata'] = df.apply(
        create_metadata, args=[utterance_metadata_keys], axis=1)
    df['intent_metadata'] = df.apply(
        create_metadata, args=[intent_metadata_keys], axis=1)

    # see if tags to do
    if tag_col == '':
        tag_col = list()
    else:
        tag_col = tag_col.split(',')

    # A workspace is used to upload labelled or unlabelled data
    labelled = humanfirst.objects.HFWorkspace()

    # build examples adding the intents as we go to labelled
    df = df.apply(build_examples, args=[
                  labelled, utterance_col, intent_col, strip, tag_col,hierarchical], axis=1)

    # add the examples to workspace
    for example in df['example']:
        labelled.add_example(example)

    # write to output
    filename_out = filename.replace('.csv', '.json')
    file_out = open(filename_out, mode='w', encoding='utf8')
    labelled.write_json(file_out)
    print(f'Wrote to {filename_out}')
    file_out.close()


def build_examples(row: pandas.Series,
                   labelled: humanfirst.objects.HFWorkspace,
                   utterance_col: str,
                   intent_col: str,
                   strip: str,
                   tag_col: list,
                   hierarchical: str
    ) -> pandas.Series:
    '''Build the examples'''

    tags = []
    for tag in tag_col:
        tags.append(labelled.tag(row[tag]))

    # work out whether splitting the intent name on a delimiter
    if hierarchical == '':
        name_or_hier = [row[intent_col]]
    else:
        name_or_hier = str(row[intent_col]).split(hierarchical)

    intent = labelled.intent(
        name_or_hier=name_or_hier, metadata=row["intent_metadata"] ,tags=tags)

    # strip any strip chars if we are turning labels into utterances for instance
    utterance = str(row[utterance_col])
    if strip != '':
        utterance = utterance.replace(strip, ' ')

    # build examples
    example = humanfirst.objects.HFExample(
        text=utterance,
        id=f'example-{uuid.uuid4()}',
        created_at=datetime.now().isoformat(),
        intents=[intent],
        tags=[],
        metadata=row['utterance_metadata']
    )
    row['example'] = example
    return row


def create_metadata(row: Union[pandas.Series, dict], metadata_keys_to_extract: list) -> dict:
    '''Build the HF metadata object for the pandas line using the column names passed'''

    metadata = {}
    for key in metadata_keys_to_extract:
        metadata[key] = str(row[key])

    return metadata


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
