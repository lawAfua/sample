import boto3
import constants
from azure.storage.blob import BlobServiceClient
import requests
import json


class DataLoaders(object):

    # def __init__(self):
    #     pass

    def load_from_db(self):
        pass

    def load_from_cloud(self, cloud_name: str, **kwargs: dict):
        if cloud_name == constants.AWS_CLOUD:
            self._load_from_aws(**kwargs)
            return
        elif cloud_name == constants.AZURE_CLOUD:
            self._load_from_azure(**kwargs)
            return
        else:
            return "cloud_name {} is incorrect".format(cloud_name)

    def initial_fetch(self):
        resp, status_code = self._fetch(constants.FILE_URL)
        if status_code == 200:
            return resp.json()
        return "error: {}".format(resp)
        # graph = obonet.read_obo(constants.FILE_URL)
        # return graph

    def load_from_api(self, go_ids: list):
        for go_id in range(go_ids):
            fetch_url = "{}/{}".format(constants.API_URL, go_id)
            resp, status_code = self._fetch_metadata(fetch_url)
            if status_code == 200:
                return resp
            return "error: {}".format(resp)

    def load_from_file(self, load_path:str, download_path: str):
        try:
            local_go_ids = self._load_from_file(load_path)
            with open(download_path, "w+") as f:
                json.dump(local_go_ids, f)
            return
        except Exception as err:
            return "error occurred when trying to load file from local: {}".format(err)

    @staticmethod
    def _load_from_file(load_path: str):
        try:
            with open(load_path) as f:
                local_go_ids = json.load(f)
            return local_go_ids
        except Exception as err:
            return "error occurred when trying to read from json: {}".format(err)

    @staticmethod
    def _fetch(url):
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp, resp.status_code
        return "error {}".format(resp), resp.status_code

    @staticmethod
    def _load_from_aws(**aws_args: str):
        # Init client
        s3 = boto3.resource('s3', aws_access_key_id=aws_args["aws_key_id"], aws_secret_access_key=aws_args["aws_access_key"])
        try:
            # Downloading file
            s3.Bucket(aws_args["bucket_name"]).download_file(Key=aws_args["name"], Filename=aws_args["download_path"])
        except Exception as e:
            print("error encountered when trying to connect to AWS: {}".format(e))

    @staticmethod
    def _load_from_azure(*azure_args: str):
        # Instantiate a new BlobServiceClient using a connection string - set chunk size to 1MB
        blob_service_client = BlobServiceClient.from_connection_string(azure_args.connection_string,
                                                                       max_single_get_size=1024 * 1024,
                                                                       max_chunk_get_size=1024 * 1024)

        # Instantiate a new ContainerClient.
        container_client = blob_service_client.get_container_client(azure_args.container_name)
        try:
            # Instantiate a new source blob client
            blob_client = container_client.get_blob_client(azure_args.file_name)

            # Download Blob
            with open(azure_args.download_path, "wb+") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
        except Exception as e:
            print("exception encountered when trying to connect with Azure: {}".format(e))