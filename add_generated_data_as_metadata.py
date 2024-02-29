"""
python add_generated_data_as_metadata.py

Doesn't work for merge stash as there won't be source conversation id metadata in the result
"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import pandas
import click

# custom imports
import back_to_hf_unlabelled

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input HF JSON File Path')
@click.option('-g', '--generated_data', type=str, required=True, help='Generated data')
def main(filename: str, generated_data: str) -> None:
    """Main Function"""

    # load input data
    with open(filename, mode="r", encoding="utf8") as file:
        data = json.load(file)
    df = pandas.json_normalize(data=data["examples"], sep="-")


    # load input data
    with open(generated_data, mode="r", encoding="utf8") as file:
        gen_data = json.load(file)
    df_gen = pandas.json_normalize(data=gen_data["examples"], sep="-")


    if "context-context_id" not in df.columns:
        raise RuntimeError("Context id column is not present in the unlabelled dataset")

    if "metadata-sourceConversationId" not in df_gen.columns:
        raise RuntimeError("Source Conversation ID metadata is not present in the generated dataset")


    all_gen_metadata = [
        'metadata-generationRunId',
        'metadata-generationTime',
        'metadata-integrationId',
        'metadata-llmModelName',
        'metadata-prompt',
        'metadata-promptId',
        'metadata-sourceConversationId',
        'metadata-pipelineId',
        'metadata-pipelineStepId'
    ]

    unlabelled_columns = df.columns

    for met in all_gen_metadata:
        if met in unlabelled_columns:
            err_msg1 = f"{met} from generated data conflicts with the {met} from unalbelled data."
            err_msg2 = f"{met} should not exist in unlabelled data"
            raise RuntimeError(f"{err_msg1} {err_msg2}")


    df_gen.rename(columns={'text': 'metadata-gen_text'}, inplace=True)
    gen_columns = df_gen.columns
    gen_metadata = []
    for met in gen_columns:
        if met.startswith("metadata-"):
            if met not in unlabelled_columns and met not in ['metadata-exampleId','metadata-exampleText']:
                gen_metadata.append(met)

    df_gen = df_gen[gen_metadata]

    merged_df = pandas.merge(df,
                            df_gen,
                            how="inner",
                            left_on="context-context_id",
                            right_on="metadata-sourceConversationId").reset_index(drop=True)

    merged_df.drop(columns=["metadata-sourceConversationId"],inplace=True)

    output_path = filename.replace(".json","_gen_metadata.json")
    back_to_hf_unlabelled.back_to_hf(merged_df,output_path,[])

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
