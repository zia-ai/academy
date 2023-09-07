# Convert ABCD dataset to prompt completion pair dataset

`python ./fine_tune/1_convert_abcd_dataset_to_prompt_dataset.py -i <ABCD dataset in HF format> [--incremental_prompt] -s <number of samples/conversation> -x <output filepath suffix> [-f <filepath containing intent specific conversation ids>]`

# Convert JSON to JSONL

OpenAI accepts dataset only in jsonl format and it can be done using the following command

`python ./fine_tune/2_convert_json_to_jsonl.py -i <prompt completion pairs json file path>`

# Get intent specific conversation IDs

Get access to HF by [signing up](https://studio.humanfirst.ai/sign-up/create-account)

If already have access then [sign in](https://studio.humanfirst.ai/sign-up/login)

Create a workspace using a demo Academy EX03 or EX04 workspace.

`python ./fine_tune/get_convo_ids_specific_to_intents.py -f <ABCD dataset in HF format> -u <HF username> -p <HF password> -n <HF namespace> -b <workspace id> [-t <bearer token>] -c <chunk for batch predict> -i <intent name>`

Then this produces a text file containing conversation ids pertaining to specified intent

This text file is then be used in the section ***Convert ABCD dataset to prompt completion pair dataset*** to generate prompt completion pairs dataset targetting specific intent

# Fine-tune

First set the openai key as an environment variable

`export OPENAI_API_KEY=<API-KEY>`

Then prepare the prompt-completion pairs dataset in jsonl format to openai standards

`openai tools fine_tunes.prepare_data -f <prompt-completion in jsonl format>`

Finally fine tune a base model

`openai api fine_tunes.create -t <TRAIN_FILE_ID_OR_PATH> -m <BASE_MODEL>`

