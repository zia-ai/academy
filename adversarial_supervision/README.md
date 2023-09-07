# Pre-requisite
## Get OpenAI API key

[Sign up](https://auth0.openai.com/u/signup/identifier?state=hKFo2SAyeGY1Q1lCaE5GakU4d1ZCdml6RE92WGN5UkM3M19WRaFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIE1fTE50N1l3N3hkcEVsQ0g1NUx1c1g3SUtmWnZFRF9po2NpZNkgRFJpdnNubTJNdTQyVDNLT3BxZHR3QjNOWXZpSFl6d0Q) for OpenAI

If already have an account [sign in](https://auth0.openai.com/u/login/identifier?state=hKFo2SAyeGY1Q1lCaE5GakU4d1ZCdml6RE92WGN5UkM3M19WRaFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIE1fTE50N1l3N3hkcEVsQ0g1NUx1c1g3SUtmWnZFRF9po2NpZNkgRFJpdnNubTJNdTQyVDNLT3BxZHR3QjNOWXZpSFl6d0Q)

Once signed in find API keys under [User Settings](https://beta.openai.com/account/api-keys)

This API key is required to perform prompt attacks against gpt-3.5-turbo model

## Train inbound supervision model to detect prompt attacks

Convert the dataset train.parquet and test.parquet to HumanFirst(HF) JSON format using the script convert_deepset_to_hf.py

`python ./adversarial_supervision/scripts/data_prep_for_training_inbound_sup_model/convert_deepset_to_hf.py -f <folder containing both train and test dataset in parquet format>`

Load the converted dataset to the HF tool.

Link the newly uploaded dataset to the existing inbound supervision model workspace.

Cluster the data samples and find new intents.

Create new intents using the data samples.

## Build initial outbound supervision prompt

Generate responses for 10 manually crafted adversarial inputs

`python ./adversarial_supervision/scripts/initial_outbound_sup/1_run_example_adversarial_prompt/1_run_adversarial_prompt.py -a <OpenAI API key>`

Since the number of utterances is low, the reponses are manually labelled as appropriate or inappropriate

Evaluate the responses generated above using multiple versions of outbound supervision prompts

`python ./adversarial_supervision/scripts/initial_outbound_sup/2_evaluate_initial_outbound_supervision_prompt/02_1_non_redacted.py -a <OpenAI API key> -m <model version>`

`python ./adversarial_supervision/scripts/initial_outbound_sup/2_evaluate_initial_outbound_supervision_prompt/02_2_redacted.py -a <OpenAI API key> -m <model version>`

`python ./adversarial_supervision/scripts/initial_outbound_sup/2_evaluate_initial_outbound_supervision_prompt/02_3_completion.py -a <OpenAI API key> -m <model version>`

`python ./adversarial_supervision/scripts/initial_outbound_sup/2_evaluate_initial_outbound_supervision_prompt/02_4_prompt_completion.py -a <OpenAI API key> -m <model version>`

Choose the one providing the best results and it is found to be second version. Hence it is used further in the project.

# Perform prompt attacks

## Perform prompt attacks in Charlie

Request for Charlie's username and password to stephen@humanfirst.ai

create a empty folder in the results folder

`mkdir ./adversarial_supervision/results/<folder name - AAAA>`

Run different variations of prompt attacks on Charlie using the following command

`python ./adversarial_supervision/scripts/1_attack/adversarial_attack.py -a <OpenAI API key> -b ./adversarial_supervision/dataset/adv_suffix.txt -r ./adversarial_supervision/results/<folder name - AAAA>/ -e <dev | prod> -u <Charlie's username> -p <Charlie's password> -d ./adversarial_supervision/dataset/<jaiklbreak_dan_prefix.txt | data_leakage_dan_prefix.txt> -s <sample size> -n <number of cores> -g charlie [--use_dan_attack_prefix] [--use_adv_suffix] -f <input dataset containing harmful insructions> -m <gpt-3.5-turbo-0301 | gpt-3.5-turbo-0613>`

## Perform prompt attacks in OpenAI instruct model gpt-3.5-turbo

create a empty folder in the results folder

`mkdir ./adversarial_supervision/results/<folder name - AAAA>`

Run different variations of prompt attacks on openai using the following command

`python ./adversarial_supervision/scripts/1_attack/adversarial_attack.py -a <OpenAI API key> -b ./adversarial_supervision/dataset/adv_suffix.txt -r ./adversarial_supervision/results/<folder name - AAAA>/ -d ./adversarial_supervision/dataset/<jaiklbreak_dan_prefix.txt | data_leakage_dan_prefix.txt> -s <sample size> -n <number of cores> -g openai [--use_dan_attack_prefix] [--use_adv_suffix] -f <input dataset containing harmful insructions> -m <gpt-3.5-turbo-0301 | gpt-3.5-turbo-0613>`

# Label prompt attack responses

Convert the dataset contqaining responses of prompt attacks into HF JSON format using the following command

`python ./adversarial_supervision/scripts/2_labelling/general_way_for_labelling_both_jailbreak_and_data_leakage_attack_responses/convert_to_hf_format.py -f <CSV containing responses>`

Get access to HF by [signing up](https://studio.humanfirst.ai/sign-up/create-account)

If already have access then [sign in](https://studio.humanfirst.ai/sign-up/login)

Upload the HF formatted data into HF tool.

Create a workspace in the tool and link the dataset to the workspace.

Create 2 intents appropriate nad inappropriate.

Push the data samples into respective intents accordingly.

Export the labelled data as JSON format.

Merge the labelled data to the CSV containing responses using the following command

`python ./adversarial_supervision/scripts/2_labelling/general_way_for_labelling_both_jailbreak_and_data_leakage_attack_responses/merge_labels.py -f <CSV containing all the responses> -h <labelled HF JSON data>`

This produces a CSV where all the responses are labelled.

There is a easy way to label data leakage prompt attack responses using the following command

`python ./adversarial_supervision/scripts/2_labelling/easier_way_for_labelling_data_leakage_responses/data_leakage_label.py -f <CSV file containing prompt attack responses>`

# Evaluate the prompt attack success

Prompt attacks success rate can be evaluated using the following command

`python ./adversarial_supervision/scripts/3_evaluation_of_prompt_attack_success/adv_eval.py -f <Labelled CSV file containing responses>`

This produces Attack Success Rate (ASR)

# Evaluate outbound supervision prompt against the prompt attack responses

Evaluate outbound supervision prompt before pushing it to Charlie against responses generated by different variation of prompt attacks using the following command

`python ./adversarial_supervision/scripts/4_evaluation_of_outbound_supervision_prompt/outbound_adv_supervision.py -f <Labelled CSV file containing prompt attack responses> -a <OpenAI API key> -m <model version - gpt-3.5-turbo-0301 | gpt-3.5-turbo-0613> -p <outbound supervision prompt file path> -s <sample size> -n <number of cores>`

This compares the predicted result with ground truth and produces accuracy, recall, precision and f1

# Perform regression test for Charlie

Regerssion test dataset is generated in HF tool and exported as CSV.

Exported CSV is then cleansed using the following command

`python ./adversarial_supervision/scripts/regression_test_dataset_prep/clean_regression_test_dataset.py -f <regression test CSV file path>`

Generate responses for the instructions in regression test dataset using the steps mentioned in the section ***Perform prompt attacks in Charlie***

Since all the responses are not harmful, they all are labelled as appropriate.

Based on results found from running various tests, inbound supervision model does not affect these instructions but there is a chance outbound supervision model might affect the responses. So generated responses are tested against outbound supervision prompt using the steps mentioned in the section ***Evaluate outbound supervision prompt against the prompt attack responses***