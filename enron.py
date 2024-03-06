"""
python enron.py

Takes a directory for a single user from a downloaded copy the Enron Email dataset available here
https://www.cs.cmu.edu/~enron/

Originally released into the public domain by https://www.ferc.gov/ during their investigation.

May 7th 2015 version of data
https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz



"""
# *********************************************************************************************************************

# standard imports
import os
import pathlib
import datetime

# 3rd party imports
import click

# custom imports
import humanfirst

@click.command()
@click.option('-d', '--directory', type=str, required=True, help='root path single user')
def main(directory: str) -> None:
    """Main Function"""

    # build path
    assert os.path.isdir(directory)
    file_list = []
    for path in pathlib.Path(directory).rglob("*."):
        file_list.append(str(path))
    print(f'Length: {len(file_list)}')

    #
    workspace = humanfirst.objects.HFWorkspace()
    for file in file_list:
        with open(file,mode='r',encoding='WINDOWS-1252') as filehandle:
            try:
                text = filehandle.read()
            except UnicodeDecodeError:
                text = ""
                print(f'Couldn\'t read {file} because of encoding')
            example = humanfirst.objects.HFExample(
                text=text,
                id=file,
                created_at=datetime.datetime.now().isoformat(),
                metadata={
                    "file": file
                },
                context=humanfirst.objects.HFContext(file,'conversation','client')
            )
            workspace.add_example(example)

    # write to output
    print("Commencing write")
    filename_out = os.path.join(directory,"output.json")
    file_out = open(filename_out, mode='w', encoding='utf8')
    workspace.write_json(file_out)
    file_out.close()
    print(f"Write complete to {filename_out}")


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
