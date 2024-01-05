"""
confusion_matrix.py

Accepts phrases.csv from evaluation results and produces a CSV containing confusion matrix

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
from os.path import isfile, isdir, join
import zipfile
import io

# third party imports
import pandas
import click
import humanfirst
from sklearn.metrics import confusion_matrix


# Typical phrases.csv from HF contains
# Labelled Phrase,Detected Phrase,Intent Id,Intent Name,Top Match Intent Id,Top Match Intent Name,Top Match Score,
# Entropy,Uncertainty,Margin Score,Result Type

class FileDirException(Exception):
    """This happens when a file directory does not exist"""

class InvalidCSVException(Exception):
    """This happens when a file path does not exist or if it is not a csv file"""

@click.command()
@click.option('-f', '--filedir', type=str, default='./data',
              help='All the files from evaluations gets extracted at this directory')
@click.option('-i', '--input_filepath', type=str, default='', help='Input filepath - phrases.csv')
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-e', '--evaluation_id', type=str, required=True, help='HumanFirst evaluation id')
def main(filedir: str, input_filepath: str,
         username: str, password: int, namespace: bool,
         playbook: str, evaluation_id: str) -> None:
    '''Main function'''

    if not isfile(input_filepath):
        if not isdir(filedir):
            raise FileDirException(f"{filedir} is not a directory")
        hf_api = humanfirst.apis.HFAPI(username, password)
        response = hf_api.get_evaluation_report(namespace, playbook, evaluation_id)
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        zip_file.extractall(filedir)
        phrases_filename = join(filedir, "phrases.csv")
    else:
        phrases_filename = input_filepath

    process(phrases_filepath=phrases_filename)


def process(phrases_filepath: str) -> None:
    '''Creates a confusion matrix'''

    # validate file path
    if (not isfile(phrases_filepath)) or (phrases_filepath.split('.')[-1] != 'csv'):
        raise InvalidCSVException("Incorrect file path or not a CSV file")

    # read phrases file
    df = pandas.read_csv(phrases_filepath)
    labels = sorted(list(set(df["Intent Name"])))
    print(f"Total Number of intents: {len(labels)}")

    # summarise
    print('Summary of df')
    print(df[["Labelled Phrase", "Result Type"]
             ].groupby(["Result Type"]).count())

    # create confusion matrix
    confusion_mat = confusion_matrix(
        df["Intent Name"], df["Top Match Intent Name"], labels=labels)

    # Convert the confusion matrix to a DataFrame for better visualization
    confusion_df = pandas.DataFrame(confusion_mat,index=labels,columns=labels)

    # Save the confusion matrix to a CSV file
    output_filepath = phrases_filepath.replace(".csv","_confusion_matrix.csv")
    confusion_df.to_csv(output_filepath, index_label="Actual/Predicted")

    print("Confusion Matrix:")
    print(confusion_df)
    print(f"Confusion matrix saved as {output_filepath}")

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
