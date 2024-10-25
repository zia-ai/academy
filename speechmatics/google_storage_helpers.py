"""
Parse GCP Bucket Helper class

Contains methods which helps to read/write blobs in GCp bucket

Setup ADC
Install gcloud https://cloud.google.com/sdk/docs/install
gloud init
gcloud auth application-default login
gcloud config set project PROJECT_ID
gcloud auth application-default set-quota-project PROJECT_ID

If getting 403 access issue when trying to access GCP buckets, then add 'Storage Object Viewer' role in IAM.
"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import google.api_core
import google.api_core.exceptions
import pandas
from google.cloud import storage
import google.auth
from google.auth import impersonated_credentials

# custom imports

class GoogleStorageHelper():
    """
    Helps to read/write blobs in GCp bucket
    """
    def __init__(self, impersonate: bool, impersonate_service_account: str) -> None:
        """Authorization"""

        if impersonate:
            # Obtain the default credentials
            credentials, project = google.auth.default()

            # Impersonate the target service account
            target_credentials = impersonated_credentials.Credentials(
                source_credentials=credentials,
                target_principal=impersonate_service_account,
                target_scopes=['https://www.googleapis.com/auth/cloud-platform']
            )

            # Initialize the storage client with the impersonated credentials
            self.storage_client = storage.Client(credentials=target_credentials, project=project)
        else:
            self.storage_client = storage.Client()

    def get_blob_df_worklist(
          self,
          bucket_name: str,
          file_type_filter: str,
          target_type_filter: str = ".json",
          max_results: int = 0
        ) -> pandas.DataFrame:
        """Returns a simple list of blob names (full file path)
        optionally filtering by endswith file_type_filter"""

        bucket = self.storage_client.bucket(bucket_name)
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


    def download_blob_to_file(self, bucket_name: str, blob_name: str, target_file_name: str):
        """Downloads a blob into memory and returns as bytes
        https://cloud.google.com/storage/docs/downloading-objects#client-libraries-download-object"""

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(target_file_name)


    def upload_file_to_blob(self, bucket_name: str, destination_blob_name: str, source_file_name: str):
        """Downloads a blob into memory and returns as bytes
        https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-client-libraries
        """

        bucket = self.storage_client.bucket(bucket_name)
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


    def list_buckets(self) -> bool:
        """return buckets list"""

        # Get the bucket object
        buckets = self.storage_client.list_buckets()
        return buckets

    def is_bucket_exists(self,bucket_name: str):
        """Check if a GCP bucket exists."""
        # Get the bucket object
        bucket = self.storage_client.get_bucket(bucket_name)

        # if bucket not found, then it throws google.api_core.exceptions.NotFound exception
        # if bucket access forbidden, then it throws google.api_core.exceptions.Forbidden exception
        if bucket.name == bucket_name:
            return 1
        return 0