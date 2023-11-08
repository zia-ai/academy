"""
python ./adversarial_supervision\
        /scripts\
        /data_prep_for_training_inbound_sup_model\
        /convert_deepset_to_hf.py

"""
# *********************************************************************************************************************

# Core imports
import uuid
from os.path import join

# 3rd party imports
import pandas
import click
from googletrans import Translator
import humanfirst

class UnrecognisedEnvironmentException(Exception):
    """This happens when entered environmenis neither dev nor prod"""

class UnscuccessfulAPICallException(Exception):
    """This happens when an API call goes unsuccessful"""

class EmptyResponseException(Exception):
    """This happens when a response generated is empty"""

@click.command()
@click.option('-f', '--text_folder_path', type=str, default="./adversarial_supervision/dataset",
              help='folder containing both train and test dataset in parquet format')
def main(text_folder_path: str) -> None:
    '''Main Function'''

    process(text_folder_path)


def process(text_folder_path: str) -> None:
    '''Evaluate adversarial examples'''

    # combine both train and test data into single dataframe
    train_filepath = join(text_folder_path,"train.parquet")
    test_filepath = join(text_folder_path,"test.parquet")

    df_train = pandas.read_parquet(train_filepath)
    df_test = pandas.read_parquet(test_filepath)

    df = pandas.concat([df_train,df_test],ignore_index=True)

    # prints basic info about the data
    print(df.columns)
    print(df.shape)

    print(df.groupby("label").count())

    translator = Translator()

    df["translated_text"] = df["text"].apply(translate_to_english,args=[translator])

    df = df.loc[df["label"] == 1].reset_index(drop=True)

    print(df[["translated_text","label"]])

    # build examples
    df = df.apply(build_examples, axis=1)

    # A workspace is used to upload labelled or unlabelled data
    # unlabelled data will have no intents on the examples and no intents defined.
    unlabelled = humanfirst.objects.HFWorkspace()

    # add the examples to workspace
    for example in df['example']:
        unlabelled.add_example(example)

    # write to output
    filename_out = join(text_folder_path,"deepset_prompt_attack_hf_load.json")
    file_out = open(filename_out, mode='w', encoding='utf8')
    unlabelled.write_json(file_out)
    file_out.close()
    print(f"Dataset is saved at {filename_out}")


def build_examples(row: pandas.Series):
    '''Build the examples'''

    # build examples
    example = humanfirst.objects.HFExample(
        text=row['translated_text'].strip(),
        id=f'example-{str(uuid.uuid4())}',
        created_at="",
        intents=[],  # no intents as unlabelled
        tags=[],  # recommend uploading metadata for unlabelled and tags for labelled
        metadata={"prompt_attack":"True"},
        # this links the individual utterances into their conversation
        context=humanfirst.objects.HFContext()
    )
    row['example'] = example
    return row

def translate_to_english(text: str, translator: Translator) -> str:
    """Translates text to english"""

    return translator.translate(text).text


if __name__=="__main__":
    main() # pylint: disable=no-value-for-parameter
