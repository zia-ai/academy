"""
python openai_usage.py -a api_key

"""
# ******************************************************************************************************************120

# standard imports
import json
import requests

# 3rd party imports
import click
import pandas
import openai

# custom imports


@click.command()
@click.option('-a', '--api_key', type=str, required=True, 
              help='OpenAI apikey')
def main(api_key: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
   
    # Define the API endpoint
    url = 'https://api.openai.com/v1/usage'

    # Make the API request
    headers = {
        'Authorization': f'Bearer {api_key}',
    }
    params = {
        'date': '2024-07-05'
    }
    response = requests.get(url, headers=headers, params=params)

    # Check for successful response
    if response.status_code == 200:
        data = response.json()
        
        # Print the raw data
        output_filename = './data/usage/output.json'
        json.dump(data, open(output_filename,mode='w',encoding='utf8'), indent=2)
        print(f'Wrote to: {output_filename}')

        # make a csv
        output_csv = './data/usage/output.csv'
        df = pandas.json_normalize(data["data"])
        df.to_csv(output_csv)
        print(f'Wrote to: {output_csv}')

        # some stats
        print(df[["project_id","n_requests"]].groupby("project_id").sum())

        
    else:
        print(f'Error: {response.status_code} - {response.text}')

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
