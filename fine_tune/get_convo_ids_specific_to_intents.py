"""
python ./fine_tune/get_convo_ids_specific_to_intents.py

"""
# *********************************************************************************************************************

# standard imports
import json
import os

# third party imports
import click
import pandas
import nltk
import humanfirst

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')


@click.command()
@click.option('-f', '--input_filename', type=str, required=True, help='Input File')
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-c', '--chunk', type=int, default=500, help='size of maximum chunk to send to batch predict')
@click.option('-i', '--intent', type=str, required=True, help='HumanFirst playbook id')
def main(input_filename: str,
         username: str,
         password: int,
         namespace: bool,
         playbook: str,
         bearertoken: str,
         chunk: int,
         intent: str) -> None:
    '''Main Function'''

    process(input_filename, username, password, namespace, playbook, bearertoken, chunk, intent)


def process(input_filename: str,
            username: str,
            password: int,
            namespace: bool,
            playbook: str,
            _: str,
            chunk: int,
            intent: str) -> None:
    '''Predicts the utterances'''

    output_filename = input_filename.replace(".json","_predicted.csv")
    if os.path.exists(output_filename):
        df = pandas.read_csv(output_filename, encoding="utf8", delimiter=",")
        df["context-context_id"] = df["context-context_id"].astype(str)
        print(df.columns)
    else:
        with open(input_filename, mode="r", encoding="utf8") as file_obj:
            data = json.load(file_obj)

        df = pandas.json_normalize(data=data["examples"],sep="-")

        print(f"Size of dataframe: {df.shape}")

        # enforce id is string
        df["context-context_id"] = df["context-context_id"].astype(str)

        df = df.loc[df["context-role"]=="client"]
        print(f"Size of dataframe containing only client utterances: {df.shape}")


        # df = df.sample(100)

        headers = humanfirst.apis.process_auth(username=username,
                                            password=password)

        fully_qualified_intent_name = []
        confidence = []
        parent_intents = []
        child_intents = []
        num_processed = 0
        for i in range(0, df['text'].size, chunk):
            utterance_chunk = list(df['text'][i: i + chunk])
            response_dict = humanfirst.apis.batchPredict(headers=headers,
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
            num_processed = num_processed + chunk
            print(f'Completed: {num_processed} utterances')
        print(f'Completed: {df["text"].size} utterances')
        df['fully_qualified_intent_name'] = fully_qualified_intent_name
        df['confidence'] = confidence
        df['parent_intent'] = parent_intents
        df['child_intent'] = child_intents

    df = df.loc[df["confidence"] > 0.35]
    df = df.loc[df["parent_intent"] == intent]

    ids = df["context-context_id"].unique().tolist()

    print(ids)
    print(len(ids))

    intent_specific_id_file = input_filename.replace(".json",f"_{intent}_specific_ids.txt")
    with open(intent_specific_id_file, mode="w", encoding="utf8") as f_obj:
        f_obj.write("\n".join(ids))

    df.to_csv(output_filename, index=False, encoding='utf8')
    print(f'Predictions CSV is saved at {output_filename}')

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
