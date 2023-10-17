"""

python strip_annotations -i <HF Workspace JSON>

Strips the entities and parts section from all examples and creates
an output file of _output.json original file

"""
#********************************************************************************************************************120

# standard imports
import json

# third party imports
import click

# Custom imports

@click.command()
@click.option('-i','--input_file_name',type=str,required=True,help='Input json HF workspace')
def main(input_file_name: str):
    """Main function"""

    # read input
    file_obj = open(input_file_name,mode='r',encoding='utf8')
    input_workspace = json.load(file_obj)
    file_obj.close()

    # cycle through deleting entities and parts on all examples
    for example in input_workspace["examples"]:
        assert isinstance(example,dict)
        for section in ["entities","parts"]:
            if section in example.keys():
                del example[section]

    # write output
    output_file_name = input_file_name.replace(".json","_output.json")
    output_obj = open(output_file_name,mode="w",encoding="utf8")
    json.dump(input_workspace,output_obj,indent=2)
    output_obj.close()
    print(f'Wrote to: {output_file_name}')




if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
