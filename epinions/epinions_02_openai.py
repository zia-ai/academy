"""
python epinions_openai.py

"""
# ******************************************************************************************************************120

# standard imports
import json
import os
import time

# 3rd party imports
import click
import pandas
import openai

# custom imports

MODEL="gpt-4o"
TIMEOUT=5
TEMPERATURE = 1.0
MAX_TOKENS= 4096
RETRY_ATTEMPTS=3
BACKOFF_BASE=2


@click.command()
@click.option('-f', '--filename', type=str, required=False,
             default="./data/epinions/epinions_output.json",
             help='Input File Path')
@click.option('-a', '--api_key', type=str, required=True,
             help='OpenAI apikey')
@click.option('-s', '--sample', type=int, required=False,default=0,
             help='How many to sample')
@click.option('-o', '--output_dir', type=str, required=False,default="./data/epinions/json",
             help='Output directory')
@click.option('-t', '--start_at', type=int, required=False,default=0,
             help='Where to start at')
def main(filename: str,
        api_key: str,
        sample: int,
        output_dir: str,
        start_at: int) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # Read input and sample
    df = pandas.json_normalize(json.load(open(filename,mode='r',encoding='utf8'))["examples"])
    if sample > 0:
        df = df.sample(sample,random_state=sample)
    print(df)

    # authorise
    openai.api_key = api_key

    # iterate df
    for i,row in df.iterrows():

        # support restart
        if i < start_at:
            continue

        # merge data and prompt
        prompt = get_prompt(review_id=row["metadata.id"],
                            item=row["metadata.item"],
                            loaded_date=row["metadata.loaded_date"],
                            stars=row["metadata.stars"],
                            paid=row["metadata.paid"],
                            text=row["text"])

        # where we will write success or errors to

        # perform openai call (single thread with retries
        response_content_raw = get_openai_response(prompt)

        # see if actually json and write value or errors
        try:
            response_content = json.loads(response_content_raw)
            output_filename = os.path.join(output_dir,f'{i:06}-{row["metadata.id"]}.json')
            with open(output_filename,mode='w',encoding='utf8') as file_out:
                json.dump(response_content,file_out,indent=2)
                print(f'Wrote to {output_filename}')
        except json.decoder.JSONDecodeError as e:
            output_filename = os.path.join(output_dir,f'{i:06}-{row["metadata.id"]}-error.json')
            with open(output_filename,mode='w',encoding='utf8') as file_out:
                file_out.write(response_content_raw + "\n\n\n" + e.msg + "\n" + str(e.pos))
                print(f'Invalid JSON: {output_filename} {e}')

def get_openai_response(prompt, retries: int = 0) -> dict:
    """Call open"""
    try:
        response = openai.ChatCompletion.create(
            model=MODEL,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            top_p=1,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            response_format = { "type": "json_object" },
            timeout=TIMEOUT
        )
        return response["choices"][0]["message"]["content"]
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f'Retry {retries} exception: {e}')
        retries = retries + 1
        if retries >= RETRY_ATTEMPTS:
            raise RuntimeError("Out of retry attempts") # pylint: disable=raise-missing-from
        time.sleep(BACKOFF_BASE**retries)
        return get_openai_response(prompt,retries)


def get_prompt(review_id: str, item: str, loaded_date: str, stars: str, paid: str, text: str) -> str:
    """Get prompt"""
    content_delimiter = '```'
    content_delimiter_name = "three backticks"
    prompt = f"""
Delimited by {content_delimiter_name} is a users draft of a review for an online opinion site with it's review_id, item_code, date_drafted, the stars_rating given and the amount_paid.

{content_delimiter}
review_id: {review_id}
item_code: {item}
date_drafted: {loaded_date}
stars_rating: {stars}
amount_paid: {paid}
review: {text}
{content_delimiter}
The user has roughly drafted the review in lowercase without punctuation.

Your job is to rewrite the review suitable for an online opinion website with proper casing, punctuation and paragraphs.  Be inspired by the original content and tone in the recreation and pay attention to the level of misspelling or grammar mistakes (or not) in the original to include similar in the output.

From the review also try and infer the category of the item being reviewed (category), the manufacturer of the item (manufacturer), the model number (model) and generate a short one line title (title) for the updated review.  Where you can't reply "unknown"

Please give your answer as correctly formatted JSON in the following format

{content_delimiter}json
{{
   "review_id": <review_id>,
   "item_code": <item_code>,
   "date_drafted": <date_drafted>,
   "stars_rating": <stars_rating>,
   "amount_paid: <amount_paid>,
   "review: <updated_review_text>,
   "category": <category>,
   "manufacturer": <manufacturer>,
   "model": <model>,
   "title": <title>
}}
{content_delimiter}
    """
    return prompt

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
