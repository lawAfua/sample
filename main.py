import os
from aws_load import AWSS3Class
from azure_load import AzureBlobClass








if __name__ == '__main__':
    print("App started")
    azure_object = AzureBlobClass(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
    container_name = input("Provide name for Azure container")
    azure_object.stream_block_blob(container_name=container_name)