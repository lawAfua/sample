from json import load
import os
from processing_data import ProcessingData
from data_dump import DataDump
from data_loaders import DataLoaders
import json
import re

def fetch_vars(loc: str):
    try:
        with open(loc) as var_file:
            return var_file.read()
    except Exception as e:
        print("error occurred when trying to read constant: {}".format(e))


def sort_and_dump(g):
    ids = []
    nodes = g['graphs'][0]['nodes']
    for node in nodes:
        match = re.findall(r'GO_\w+', node['id'])
        if len(match) > 0:
            ids.append(match[0])
    sorted_ids = sorted(ids)
    len_nodes = len(sorted_ids)
    filenames = ["aws_file.json", "azure_file.json", "local_file.json"]
    aws_subset, azure_subset, local_subset = sorted_ids[:33], sorted_ids[33: 67], sorted_ids[67: 100]
    for file_name in filenames:
        with open(os.path.join(os.curdir, "dataset", file_name), "w") as f:
            if file_name.__contains__("aws"):
                d = {i: dict() for i in aws_subset}
                json.dump(d, f)
                # f.writelines(aws_subset)
            elif file_name.__contains__("azure"):
                d = {i: dict() for i in azure_subset}
                json.dump(d, f)
            else:
                d = {i: dict() for i in local_subset}
                json.dump(d, f)


if __name__ == '__main__':
    print("App started")
    loader = DataLoaders()
    # graph = loader.initial_fetch()
    # sort_and_dump(graph)
    # print("=========Loading data from cloud =============")
    # aws_connection_string = fetch_vars(os.path.join(os.curdir, ".constants", "aws_connection_string.txt"))
    # aws_key_id, aws_access_key = [i.strip() for i in aws_connection_string.split("\n", 1)]
    # download_path = os.path.join(os.curdir, "dataset", "aws_file.json")
    # aws_kwargs = {"aws_connection_string": aws_connection_string, "aws_key_id": aws_key_id, "aws_access_key": aws_access_key,
    #               "bucket_name": "cinema-law-dev", "name": "aws_file.json", "download_path": download_path}
    # loader.load_from_cloud("aws", **aws_kwargs)
    # print("===========Data loaded from cloud ============")
    # print("============Loading data from local file ===========")
    # local_load_path = os.path.join(os.curdir, "local_dir", "local_file.json")
    # local_download_path = os.path.join(os.curdir, "dataset", "local_file.json")
    # loader.load_from_file(local_load_path, local_download_path)
    # print("============Data loaded from local file ===========")
    processor = ProcessingData()
    df = processor.read_files_and_process(os.path.join(os.curdir, "dataset"))
    sql_dump = DataDump()
    sql_dump.create_schema()
    sql_dump.dump_data(df, "gene_ontology")
    # print("========Trying to fetch azure connection string from .constants folder=============")
    # azure_connection_string = fetch_vars(os.path.join(os.curdir, ".constants", "azure_connection_string.txt"))
    # print("========Dataset will be downloaded in current dir under `dataset` folder===========")
    # azure_object = AzureBlobClass(azure_connection_string, os.path.join(os.curdir, "dataset", "sales.csv"))
    # azure_object.stream_block_blob(container_name="testcontainer", file_name="sales data-set.csv")
    # print("========Dataset downloaded from Azure===========")
    # print("=========Trying to connect with AWS=============")
    # print("========Trying to fetch aws connection string from .constants folder============")
    # aws_connection_string = fetch_vars(os.path.join(os.curdir, ".constants", "aws_connection_string.txt"))
    # aws_key_id, aws_access_key = [i.strip() for i in aws_connection_string.split("\n", 1)]
    # aws_obj = AWSS3Class(bucket_name="cinema-law-dev", aws_key_id=aws_key_id, aws_access_key=aws_access_key)
    # aws_obj.download("stores data-set.csv", os.path.join(os.curdir, "dataset", "stores.csv"))
    # print("========Dataset downloaded from AWS=============")
    # print("========Processing dataset=============")
    # process_data_obj = ProcessingData(os.path.join(os.curdir, "dataset", "sales.csv"), os.path.join(os.curdir, "dataset", "stores.csv"))
    # process_data_obj.read_dataset()
    # process_data_obj.process_store_sales()
    # print("=======Datasets processed===========")
    # print("=======Trying to insert processed data into local sqllite DB================")
    # process_data_obj.load_data_into_db("test.db", "store_sales")
    # print("=======Processed data inserted into local sqllite DB================")
    print("App completed !!!")
