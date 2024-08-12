"""
python get_blob_list_by_impersonation.py

Loops through a gcp directory

Setup ADC
Install gcloud https://cloud.google.com/sdk/docs/install
gloud init
gcloud auth application-default login

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
from google.cloud import storage # pylint: disable=no-name-in-module
import google.auth
from google.auth import impersonated_credentials

# custom imports

@click.command()
@click.option('-b', '--bucket_name', type=str, required=False, default="", help='Bucket Name')
@click.option('-i', '--impersonate_service_account', type=str, required=True,
              help='Email of the target service account to impersonate')
def main(bucket_name: str, impersonate_service_account: str) -> None:
    """
    Lists all the Blobs present in a bucket if bucket name is provided
    Otherwise lists all the bucket names
    """

    # Obtain the default credentials
    credentials, project = google.auth.default()

    # Impersonate the target service account
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=impersonate_service_account,
        target_scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    # Initialize the storage client with the impersonated credentials
    storage_client = storage.Client(credentials=target_credentials, project=project)

    if bucket_name:
        # List blobs in the specified bucket
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            print(f"Blob: {blob.name}")
    else:
        # List all buckets in the project
        buckets = storage_client.list_buckets()
        for bucket in buckets:
            print(f"Bucket: {bucket.name}")

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    # blob = bucket.blob(source_blob_name)
    # blob.download_to_filename(destination_file_name)

    # print(
    #     "Downloaded storage object {} from bucket {} to local file {}.".format(
    #         source_blob_name, bucket_name, destination_file_name
    #     )
    # )

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
