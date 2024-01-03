"""
 Code Language:   python
 Script:          upload_files_to_hf.py
 Imports:         re, json, click, requests, requests_toolbelt
 Functions:       main(), upload_multipart(), check_status(), replace(),
                  delete_file(), get_conversion_set_id(), get_conversion_source_id()
 Description:     Upload files to HumanFirst
"""
# **********************************************************************************************************************

# standard imports
import os

# third party imports
import click
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-h', '--filepath', type=str, required=True, help='file to upload to HumanFirst')
@click.option('-b', '--upload_name', type=str, default="", help='name assigned to the uploaded file to HumanFirst')
@click.option('-s', '--convoset_name', type=str, required=True, help='Conversation set name / data folder name')
@click.option('-f', '--force_upload', is_flag=True, default=False,
              help='Replaces the existing dataset with the uploaded dataset')
def main(username: str,
         password: str,
         namespace: str,
         filepath: str,
         convoset_name: str,
         upload_name: str,
         force_upload: bool) -> None:
    """Main function"""

    hf_api = humanfirst.apis.HFAPI(username=username, password=password)

    # if conversation set already exists, then it returns the source id of the existing conversation set,
    # otherwise creates a new one
    conversation_source_id = hf_api.create_conversation_set(namespace=namespace, convoset_name=convoset_name)

    if upload_name == "":
        upload_name = os.path.basename(filepath).replace(".json","")
    try:
        hf_api.upload_multipart(namespace, conversation_source_id, filepath, upload_name)
        print(f"File {upload_name} is uploaded to the conversation set {convoset_name}")
    except Exception as err: # pylint: disable=broad-exception-caught
        err = str(err)
        if err.find("file already exists") != -1:
            print("File already exists")
            if force_upload:
                replace(hf_api, namespace, conversation_source_id, filepath, upload_name)
                print("File is replaced with the provided file")
            else:
                print("File is not replaced with the provided file")
        else:
            raise RuntimeError(err) from Exception

def replace(hf_api :humanfirst.apis.HFAPI,
            namespace: str,
            conversation_source_id: str,
            filepath: str,
            upload_name: str) -> None:
    """Replaces the exisitng file with the new file in HumanFirst"""

    hf_api.delete_file(namespace, conversation_source_id, upload_name)
    hf_api.upload_multipart(namespace, conversation_source_id, filepath, upload_name)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
