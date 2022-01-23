from json import load
import os
from aws_load import AWSS3Class
from azure_load import AzureBlobClass
from processing_data import ProcessingData

def fetch_vars(loc: str):
    try:
        with open(loc) as var_file:
            return var_file.read()
    except Exception as e:
        print("error occurred when trying to read constant: {}".format(e))



if __name__ == '__main__':
    print("App started")
    print("=========Download dataset from Azure=============")
    print("========Trying to fetch azure connection string from .constants folder=============")
    azure_connection_string = fetch_vars(os.path.join(os.curdir, ".constants", "azure_connection_string.txt"))
    print("========Dataset will be downloaded in current dir under `dataset` folder===========")
    azure_object = AzureBlobClass(azure_connection_string, os.path.join(os.curdir, "dataset", "sales.csv"))
    azure_object.stream_block_blob(container_name="testcontainer", file_name="sales data-set.csv")
    print("========Dataset downloaded from Azure===========")
    print("=========Trying to connect with AWS=============")
    print("========Trying to fetch aws connection string from .constants folder============")
    aws_connection_string = fetch_vars(os.path.join(os.curdir, ".constants", "aws_connection_string.txt"))
    aws_key_id, aws_access_key = [i.strip() for i in aws_connection_string.split("\n", 1)]
    aws_obj = AWSS3Class(bucket_name="cinema-law-dev", aws_key_id=aws_key_id, aws_access_key=aws_access_key)
    aws_obj.download("stores data-set.csv", os.path.join(os.curdir, "dataset", "stores.csv"))
    print("========Dataset downloaded from AWS=============")
    print("========Processing dataset=============")
    process_data_obj = ProcessingData(os.path.join(os.curdir, "dataset", "sales.csv"), os.path.join(os.curdir, "dataset", "stores.csv"))
    process_data_obj.read_dataset()
    process_data_obj.process_store_sales()
    print("=======Datasets processed===========")
    print("=======Trying to insert processed data into local sqllite DB================")
    process_data_obj.load_data_into_db("test.db", "store_sales")
    print("=======Processed data inserted into local sqllite DB================")
    print("App completed !!!")
