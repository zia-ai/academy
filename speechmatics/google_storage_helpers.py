"""
python loop_through_gcp.py

Loops through a gcp directory

Setup ADC
Install gcloud https://cloud.google.com/sdk/docs/install
gloud init
gcloud auth application-default login

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import pandas
from google.cloud import storage

# custom imports

def get_blob_df_worklist(
        bucket_name: str, 
        file_type_filter: str, 
        target_type_filter: str = ".json",
        max_results: int = 0
    ) -> pandas.DataFrame:
    """Returns a simple list of blob names (full file path)
    optionally filtering by endswith file_type_filter"""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if max_results > 0:
        all_blobs = bucket.list_blobs(max_results=max_results)
    else:
        all_blobs = bucket.list_blobs()
    blob_dict = {}

    # going to go through twice, check things that need doing, then check they are done.
    for b in all_blobs:
        # https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob
        name = str(b.name)
        root_name = None
        if name.endswith(file_type_filter):
            root_name = name[0:-len(file_type_filter)]
        elif name.endswith(target_type_filter):
            root_name = name[0:-len(target_type_filter)]
        else:
            continue
        if root_name:
            if not root_name in blob_dict.keys():
                blob_dict[root_name] = {
                    "root_name": root_name,
                    "source_name": "",
                    "target_name": "",
                    "done": False
                }
            if name.endswith(file_type_filter):
                blob_dict[root_name]["source_name"] = name
            else:
                blob_dict[root_name]["target_name"] = name
                blob_dict[root_name]["done"] = True
             
    df_return = pandas.json_normalize(blob_dict.values())

    return df_return

def download_blob_to_file(bucket_name: str, blob_name: str, target_file_name: str):
    """Downloads a blob into memory and returns as bytes
    https://cloud.google.com/storage/docs/downloading-objects#client-libraries-download-object"""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(target_file_name)

def upload_file_to_blob(bucket_name: str, destination_blob_name: str, source_file_name: str):
    """Downloads a blob into memory and returns as bytes
    https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-client-libraries
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # sgeneration_match_precondition = 0

    # do the thing
    blob.upload_from_filename(source_file_name)