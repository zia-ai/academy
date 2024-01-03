"""
python ./batch_predict.py

This script predicts the intents of utterances in batches
The input CSV file should contain a column containing utterances for prediction
The output CSV contains fully_qualified_intent_name, confidence, parent_intent, and child_intent columns additionally 

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *********************************************************************************************************************

# standard imports
import os

# third party imports
import click
import pandas
import humanfirst

@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-o', '--output_filename', type=str, default='', help='Output File')
@click.option('-r', '--uttr_col', type=str, required=True, help='Utterance column name')
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-c', '--chunk', type=int, default=500, help='size of maximum chunk to send to batch predict')
def main(input_filename: str, output_filename: str, uttr_col: str,
         username: str, password: int, namespace: bool, playbook: str, chunk: int) -> None:
    """Main Function"""

    if not os.path.exists(input_filename):
        print("Couldn't find the dataset at the file location you provided:")
        print(input_filename)
        quit()

    df = pandas.read_csv(input_filename, encoding='utf8')
    assert isinstance(df, pandas.DataFrame)

    # drop all rows which don't have any meaningful data
    print(f'Shape with all lines:                {df.shape}')
    df = df[~df[uttr_col].isna()]
    print(f'Shape with only non-blank verbatims: {df.shape}')

    rename_col = {
        uttr_col: "utterance"
    }

    df.rename(columns=rename_col,inplace=True)

    print(df)

    hf_api = humanfirst.apis.HFAPI(username=username, password=password)

    fully_qualified_intent_name = []
    confidence = []
    parent_intents = []
    child_intents = []
    num_processed = 0
    for i in range(0, df['utterance'].size, chunk):
        utterance_chunk = list(df['utterance'][i: i + chunk])
        response_dict = hf_api.batchPredict(
                                            sentences=utterance_chunk,
                                            namespace=namespace,
                                            playbook=playbook)

        for j in range(len(utterance_chunk)):
            confidence.append(response_dict[j]['matches'][0]['score'])
            hierarchy = response_dict[j]['matches'][0]['hierarchyNames']
            intent_name = hierarchy[0]
            for i in range(1, len(hierarchy)):
                intent_name = f'{intent_name}-{hierarchy[i]}'
            fully_qualified_intent_name.append(intent_name)
            predicted_intent = response_dict[j]['matches'][0]['name']
            if predicted_intent != fully_qualified_intent_name[-1]:
                child_intents.append(predicted_intent)
            else:
                child_intents.append(None)
            parent_intents.append(hierarchy[0])
        num_processed = num_processed + len(utterance_chunk)
        print(f'Completed: {num_processed} utterances')
    df['fully_qualified_intent_name'] = fully_qualified_intent_name
    df['confidence'] = confidence
    df['parent_intent'] = parent_intents
    df['child_intent'] = child_intents

    if output_filename == '':
        output_filename = input_filename.replace(".csv","_predictions.csv")

    df.to_csv(output_filename, index=False, encoding='utf8')
    print(f'Predictions CSV is saved at {output_filename}')


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
