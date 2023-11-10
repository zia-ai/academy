"""
python liu_process.py -n liuetal

download the input file first using liu_download.sh into ./data

Uses the data set published in this paper
Liu, X., Eshghi, A., Swietojanski, P. and Rieser, V., 2019.
Benchmarking natural language understanding services for building conversational agents.
arXiv preprint arXiv:1903.05566."

https://arxiv.org/pdf/1903.05566.pdf

https://github.com/xliuhw/NLU-Evaluation-Data


"""
# *********************************************************************************************************************

# standard imports
import datetime
import copy
import re

# third party imports
import pandas
import numpy
import click
import humanfirst


# define regex
re_deannotate = re.compile(r"\[\s*([A-Za-z0-9-_]+)\s*:\s*([A-Za-z0-9@-_’'\. ]+)\]")

@click.command()
@click.option('-n','--name',type=str,default='liuetal',help='Name used for all files')
@click.option('-s','--sample',type=int,default=0,help='n conversations to sample from dataset')
@click.option('-w','--workspace',type=bool,default=False,help='whether to reload workspace')
def main(name: str, sample: int, workspace: str):
    """Main Function"""
    # TODO - test workspace function

    # read file
    dtypes = {
        'userid': str,
        'answerid': str,
        'scenario': str,
        'status': str,
        'intent': str,
        'answer_annotation': str,
        'notes': str,
        'suggested_entities': str,
        'answer_normalised': str,
        'answer': str,
        'question': str
    }

    df = pandas.read_csv(f'./data/{name}.csv', encoding='utf8', sep=';', dtype=dtypes, keep_default_na=False)
    assert isinstance(df,pandas.DataFrame)
    print(f'Read file of df.shape: {df.shape}')
    print(df.columns)

    print("user id summary")
    print(df[["userid","answerid"]].groupby("userid").count())

    # need to drop from original set items that the paper decided where irrelevant/wrong
    drop_statuses = ['IRR','IRR_XL','IRR_XR']
    # full list of statuses kept ADD (addition by annotator),
    # MOD (see notes for modification),
    # MOD_XL (see notes for modication), null, blank)
    df = df[~df['status'].isin(drop_statuses)]
    print(f'After dropping {drop_statuses} shape is {df.shape}')

    # have many duplicates on answer id.
    df["uid"] = df["answer_annotation"].apply(humanfirst.objects.hash_string)

    # we have music as an intent and as a scenario.  We have intent query identically named in many scenarios.
    # TODO: look at humanfirst.py and decide if need update on how taxonmy managed.
    # in meantime workaround is makeing intent scenario_intent under parent scenario.
    df['scenario_intent'] = df['scenario'] + '_' + df['intent']
    print(df['scenario_intent'].unique())

    # allow sampling for a smaller subset
    if sample > 0:
        df = df.sample(sample)
        print(f'Sampled down to {sample} conversations')
    else:
        print('No sampling down, full set')

    # create workspaces
    labelled_workspace = humanfirst.objects.HFWorkspace()
    unlabelled_workspace = humanfirst.objects.HFWorkspace()

    # if we have already created intents reload them from source control
    # we are reading from CSV to represent a cleansed source control dataset without tags etc.
    # i.e like a user might have before starting to use humanfirst
    if workspace:
        df_intents = pandas.read_csv(f'./workspaces/{workspace}/{workspace}-intents.csv',
                                     delimiter=',',
                                     names=['utterance','slash_sep_hier'])
        assert isinstance(df_intents,pandas.DataFrame)
        df_intents.apply(build_intents_from_file, args=[labelled_workspace],axis=1)

    # Liu set has no dateimtes on the data
    # Shard the data across 30 days of an arbitary month for performance in HumanFirst studio
    df['day'] = numpy.random.randint(0, 30, df.shape[0])
    start_datetime = datetime.datetime(2022,5,1,0,0,0)
    df["created_at"] = df["day"].apply(add_datetimes,args=[start_datetime])
    print("Distribution by day is")
    print(df[['created_at','uid']].groupby('created_at').count())

    # create deannoated text
    df['deannotated_text'] = df['answer_annotation'].apply(deannotate)

    # check for duplicates when lowercased
    before = df.shape[0]
    print(f'Before deduplication {before}')
    df["deannotated_lower"] = df['deannotated_text'].str.lower()
    df.drop_duplicates(subset="deannotated_lower",inplace=True)
    after = df.shape[0]
    print(f'After deduplication {after}')

    # create example for each row.
    df = df.apply(create_example,axis=1,args=[unlabelled_workspace,labelled_workspace])

    # write unlabelled
    with open(f'./data/{name}_unlabelled.json', 'w', encoding='utf8') as file_out:
        unlabelled_workspace.write_json(file_out)

    # write labelled
    with open(f'./data/{name}_labelled.json', 'w', encoding='utf8') as file_out:
        labelled_workspace.write_json(file_out)

def create_example(row: pandas.Series,
                   unlabelled_workspace: humanfirst.objects.HFWorkspace,
                   labelled_workspace: humanfirst.objects.HFWorkspace) -> list:
    '''Parse a single utterance to an example'''

    # HFMetadata just dict[str,all]
    # extract these keys into the metadata dict
    keys_to_extract = ['userid', 'answerid','scenario','intent','status','notes','suggested_entities','question']
    convo_metadata_dict = {}
    for key in keys_to_extract:
        if row[key]:
            convo_metadata_dict[key] = str(row[key])
        else:
            convo_metadata_dict[key] = ''

    # Tags
    tags=[]
    tags_to_label = ['scenario'] # 'scenario_subflow' - removed as too noisy with speaker role and seq
    for tag in tags_to_label:
        tags.append(unlabelled_workspace.tag(row[tag]))
        labelled_workspace.tag(row[tag])

    # Context with the conversation_id linking utterances (here none just role: client
    context = humanfirst.objects.HFContext(f'convo-{row["answerid"]}','conversation', 'client')

    # Create the unlabelled example without intents
    example = humanfirst.objects.HFExample(
        text=row['deannotated_text'],
        id=f'example-unlabelled-{row["uid"]}',
        created_at=row['created_at'],
        intents=[],
        tags=tags,
        metadata=copy.deepcopy(convo_metadata_dict),
        context=context
    )

    # add to the unlabelled_workspace
    unlabelled_workspace.add_example(example)

    # get or create the intent in the labelled_workspace using parent as scenario and child as intent
    # i.e alarm/query, alarm/remove with no metadata
    intents = [labelled_workspace.intent(name_or_hier=[row['scenario_intent']])]

    labelled_example = humanfirst.objects.HFExample(
        text=row["deannotated_text"],
        id=f'example-labelled-{row["uid"]}',
        created_at=row['created_at'],
        intents=intents,
        tags=[],
        metadata={},
        context={}
    )
    labelled_workspace.add_example(labelled_example)

def deannotate(text: str) -> str:
    """Deannotates a given string"""

    findalls = re_deannotate.findall(text)
    output_text = ''
    for _ in findalls:
        matches = re_deannotate.search(text)
        assert isinstance(matches, re.Match)
        output_text = output_text + text[0:matches.start()] + matches.group(2)
        text = text[matches.end():]
    output_text = output_text + text
    return output_text

# TODO: move into humanfirst
def build_intents_from_file(row: pandas.Series, labelled_workspace: humanfirst.objects.HFWorkspace):
    """Build intents"""

    hierarchy = str(row['slash_sep_hier']).split('/')
    labelled_workspace.example(row['utterance'],intents=[labelled_workspace.intent(hierarchy)])

def add_datetimes(days: int, start_datetime: datetime.datetime):
    """creates a timestamp n days after the start_datetime"""
    return start_datetime + datetime.timedelta(days=days)

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
