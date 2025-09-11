"""
python weekly_data_comparison.py

compares two weekly CSV files containing intent and input query data
generates a new CSV with columns: intent, week1_utterances, week1_count, 
week2_utterances, week2_count, week2-week1, percentage_variance

"""
# ******************************************************************************************************************120

# standard imports
import csv
import os
from collections import defaultdict

# 3rd party imports
import click


def read_csv_file(file_path: str) -> dict:
    """
    Read a CSV file and return a dictionary with intents as keys and lists of utterances as values.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        dict: Dictionary with intents as keys and lists of utterances as values
    """
    intent_utterances = defaultdict(list)
    row_count = 0

    try:
        # Try different encodings if one fails
        encodings = ['utf-8-sig', 'utf-8', 'latin-1']
        for encoding in encodings:
            try:
                with open(file_path, 'r', newline='', encoding=encoding) as csvfile:
                    # Read a small sample to check if it's readable
                    _ = csvfile.read(1024)
                    csvfile.seek(0)  # Reset file pointer to beginning

                    reader = csv.DictReader(csvfile)

                    # Check if required columns exist
                    if 'Intent' not in reader.fieldnames or 'Input Query' not in reader.fieldnames:
                        print(f"Error: CSV file {file_path} does not have required columns 'Intent' and 'Input Query'")
                        print(f"Available columns: {reader.fieldnames}")
                        continue

                    # Group utterances by intent
                    for row in reader:
                        row_count += 1
                        # Skip rows with empty intent or input query
                        if ('Intent' not in row or 'Input Query' not in row or 
                            not row['Intent'] or not row['Input Query']):
                            continue

                        intent = str(row['Intent']).strip()
                        utterance = str(row['Input Query']).strip()

                        # Only add valid intents and utterances
                        if intent and utterance:
                            intent_utterances[intent].append(utterance)

                # If we got here without exception, break the loop
                print(f"Successfully read {row_count} rows from {file_path} using {encoding} encoding")
                print(f"Found {len(intent_utterances)} unique intents")
                break
            except UnicodeDecodeError:
                # Try the next encoding
                continue
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(f"Error reading file with {encoding} encoding: {e}")
                continue

        return intent_utterances
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error reading file {file_path}: {e}")
        return intent_utterances


def compare_weekly_data(week1_file: str, week2_file: str, output_file: str) -> bool:
    """
    Compare weekly data from two CSV files and generate a comparison CSV.

    Args:
        week1_file (str): Path to the first week's CSV file
        week2_file (str): Path to the second week's CSV file
        output_file (str): Path to the output CSV file
    """
    print(f"Reading data from {week1_file}...")
    week1_data = read_csv_file(week1_file)
    if not week1_data:
        print(f"Error: No valid data found in {week1_file}")
        return False

    print(f"Reading data from {week2_file}...")
    week2_data = read_csv_file(week2_file)
    if not week2_data:
        print(f"Error: No valid data found in {week2_file}")
        return False

    # Get all unique intents from both weeks
    all_intents = set(list(week1_data.keys()) + list(week2_data.keys()))
    print(f"Found {len(all_intents)} unique intents across both files")

    # Prepare data for output
    output_data = []

    for intent in sorted(all_intents):
        week1_utterances = week1_data.get(intent, [])
        week2_utterances = week2_data.get(intent, [])

        week1_count = len(week1_utterances)
        week2_count = len(week2_utterances)

        # Calculate difference and percentage variance
        difference = week2_count - week1_count

        # Handle division by zero for percentage calculation
        if week1_count == 0:
            if week2_count == 0:
                percentage_variance = 0
            else:
                percentage_variance = 100  # Representing infinity as 100% for new intents
        else:
            percentage_variance = (difference / week1_count) * 100

        # Join utterances with a delimiter for CSV output
        # Convert all items to strings to avoid TypeError
        delimiter = "||"  # Use a clear delimiter that's unlikely to appear in the text
        try:
            week1_utterances_str = delimiter.join([str(u) for u in week1_utterances]) if week1_utterances else ""
            week2_utterances_str = delimiter.join([str(u) for u in week2_utterances]) if week2_utterances else ""
        except (TypeError, AttributeError) as e:
            print(f"Error joining utterances for intent '{intent}': {e}")
            week1_utterances_str = ""
            week2_utterances_str = ""

        output_data.append({
            'intent': intent,
            'week1_utterances': week1_utterances_str,
            'week1_count': week1_count,
            'week2_utterances': week2_utterances_str,
            'week2_count': week2_count,
            'week2-week1': difference,
            'percentage_variance': round(percentage_variance, 2)
        })

    # Write output to CSV
    print(f"Writing comparison data to {output_file}...")
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['intent',
                          'week1_utterances',
                          'week1_count',
                          'week2_utterances',
                          'week2_count',
                          'week2-week1',
                          'percentage_variance']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in output_data:
                writer.writerow(row)
        print(f"Comparison complete. Output saved to {output_file}")
        print(f"Total rows written: {len(output_data)}")
        return True
    except (IOError, OSError) as e:
        print(f"Error writing output file: {e}")
        return False


@click.command()
@click.option('-w1', '--week1_file', type=str, required=True, help='Path to the first week CSV file')
@click.option('-w2', '--week2_file', type=str, required=True, help='Path to the second week CSV file')
@click.option('-o', '--output', type=str, required=False, default='weekly_comparison_output.csv',
              help='Path to the output CSV file')
def main(week1_file: str,
         week2_file: str,
         output: str
         ) -> None: # pylint: disable=unused-argument
    """Main Function"""

    # Validate input files
    for file_path in [week1_file, week2_file]:
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist")
            quit()

    success = compare_weekly_data(week1_file, week2_file, output)
    if not success:
        quit()


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
