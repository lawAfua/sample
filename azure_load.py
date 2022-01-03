from azure.storage.blob import BlobServiceClient

# Local path to download azure dataset
AZURE_DEST_FILE = 'local_path/azure.csv'

class AzureBlobClass(object):

    def __init__(self, connection_str):
        self.connection_string = connection_str

    def stream_block_blob(self, name, blob_name):
        # Instantiate a new BlobServiceClient using a connection string - set chunk size to 1MB
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string,
                                                                       max_single_get_size=1024*1024,
                                                                       max_chunk_get_size=1024*1024)

        # Instantiate a new ContainerClient.
        container_client = blob_service_client.get_container_client(name)

        try:
            # Instantiate a new source blob client
            blob_client = container_client.get_blob_client(blob_name)

            # Download Blob
            with open(AZURE_DEST_FILE, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())

        finally:
            # Delete container
            container_client.delete_container()

