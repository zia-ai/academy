"""
python nuancemixflows_to_csv.py

Extracts Nuance Mix flow components for every intent
"""
# *********************************************************************************************************************

# standard imports
import json
import csv

# 3rd party imports
import click

@click.command()
@click.option('-f', '--flows_file', type=str, required=True, help='path of Dialog.json from NuanceMix')
def main(flows_file: str):
    '''Main Function'''
    # Define the file paths

    csv_file_path = flows_file.replace(".json",".csv")

    # Read the JSON data from the file
    with open(flows_file, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    # Prepare the data for CSV
    csv_data = []
    components = data["data"].get('components', [])

    for component in components:
        if component.get('description') == 'intent':
            csv_data.append({
                'component': component,
                'name': component.get('name', '')
            })

    # Write the data to a CSV file
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=['component', 'name'])
        writer.writeheader()
        for row in csv_data:
            writer.writerow(row)

    print(f"Data successfully written to {csv_file_path}")


if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
