
from lib import Response
import logging
from typing import Union
from google.cloud import storage

class GoogleCloudStorage():
    
    def __init__(self, project_id):
        self.client = storage.Client(project_id)

    def upload(self, bucket_name: str, destination_blob_name: str, source_file_name: str) -> Response:
        """Uploads a file to the bucket."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        try:
            blob.upload_from_filename(source_file_name)
        except Exception as e:
            logging.error("Could not upload  blob, exception occurred.", e)
        
        logging.info(
            "File {} uploaded to to {}.".format(
                source_file_name, destination_blob_name
            )
        )


    def download(self, bucket_name: str, source_blob_name: str, destination_file_name: str) -> Union[Response, bytes]:
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"
        
        bucket = self.client.bucket(bucket_name)

        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        blob = bucket.blob(source_blob_name)
        logging.info(
            f"Blob {source_blob_name} downloading"
        )
        try:
            if destination_file_name:
                blob.download_to_filename(destination_file_name)
                return Response(True, f"Downloaded {source_blob_name} to {destination_file_name}")
            #else:
            #    return blob.download_to_bytes()
        except:
            logging.error(f"Could not download {source_blob_name} to {destination_file_name}")
            return Response(False, f"Could not download {source_blob_name} to {destination_file_name}")