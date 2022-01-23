from azure.storage.blob import BlobServiceClient
import os

# Local path to download azure dataset
AZURE_DEST_FILE = 'C:/Users/jagta/Desktop/downloaded_sales.csv'

class AzureBlobClass(object):

    def __init__(self, connection_str: str, download_path: str):
        self.connection_string = connection_str
        self.download_path = download_path

    def stream_block_blob(self, container_name, file_name):
        # Instantiate a new BlobServiceClient using a connection string - set chunk size to 1MB
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string,
                                                                       max_single_get_size=1024*1024,
                                                                       max_chunk_get_size=1024*1024)

        # Instantiate a new ContainerClient.
        container_client = blob_service_client.get_container_client(container_name)
        try:
            # Instantiate a new source blob client
            blob_client = container_client.get_blob_client(file_name)

            # Download Blob
            with open(self.download_path, "wb+") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
        except Exception as e:
            print("exception encountered when trying to connect with Azure: {}".format(e))

