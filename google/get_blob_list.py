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
import click
from google.cloud import storage

# custom imports

@click.command()
@click.option('-b', '--bucket_name', type=str, required=True, help='Bucket Name')
def main(bucket_name: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    all_blobs = bucket.list_blobs()
    for b in all_blobs:
        # https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob
        print(b.name)
        print(b.bucket)
    quit()

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
