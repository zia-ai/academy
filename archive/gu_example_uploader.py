"""
python gu_example_uploader.py -f <input_filename>

creates a CSV ready to convert to unlabelled custom JSON format
"""

# standard imports
import datetime
import json

# third party imports
import pandas
import click


@click.command()
@click.option('-i', '--input_file', type=str, default='./data/abcd_v1.1.json', help='Input File')
def main(input_file: str):
    '''Main function'''

    # load JSON
    input_file_obj = open(input_file,mode='r',encoding='utf8')
    input_dict = json.load(input_file_obj)
    assert isinstance(input_dict, dict)

    # when file was run to nearest second
    iso_created = datetime.datetime.now().replace(microsecond=0)

    # Minimum information needed to create unlabelled data
    convo_ids = []
    roles = []
    utterances = []
    timestamps = []

    # conversation ids
    for conversation_id in input_dict.keys():
        for i,utterance in enumerate(input_dict[conversation_id]):
            assert isinstance(utterance,dict)

            # convo id for row
            convo_ids.append(conversation_id)

            # work out person a or b
            if "Person A" in utterance.keys():
                role = "Person A"
            elif "Person B" in utterance.keys():
                role = "Person B"
            else:
                raise RuntimeError("Couldn't map Person A or Person B to a role")
            roles.append(role)
            utterances.append(utterance[role])

            # for timestamps they are used to order, so going to have created at date + seconds based on i
            utterance_timestamp = iso_created + datetime.timedelta(seconds=i)
            utterance_timestamp = utterance_timestamp.isoformat()
            timestamps.append(utterance_timestamp)

    # create a dataframe
    df = pandas.DataFrame(data=zip(convo_ids,roles,utterances,timestamps),columns=["myid","myrole","sometext","mytimestamp"])

    # export to csv
    output_file_name = input_file.replace(".json","_output.csv")
    assert output_file_name != input_file
    df.to_csv(output_file_name,index=False)
    print(f'Wrote to: {output_file_name}')

    print(df)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
