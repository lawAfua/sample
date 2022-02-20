import pandas as pd
import sqlite3
import requests
import constants
import os
import json


class ProcessingData(object):

    def __init__(self):
        self.dataset = []

    def read_files_and_process(self, folder_path: str):
        for root, _, files in os.walk(folder_path):
            for file in files:
                with open(os.path.join(root, file)) as f:
                    ids_dict = json.load(f)
                    self._process_go_ids(ids_dict)
        return pd.DataFrame(self.dataset)

    def _process_go_ids(self, d: dict):
        for id in d:
            url = constants.METADATA_URL + id
            resp_json = self._fetch_metadata(url)
            if resp_json["label"] is not None:
                self.dataset.append([id, resp_json['label']])
        return

    @staticmethod
    def _fetch_metadata(url):
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        return "error {}".format(resp), resp.status_code

    def read_dataset(self):
        try:
            self.sales_df = pd.read_csv(self.sales_file_path)
            self.stores_df = pd.read_csv(self.stores_file_path)
        except Exception as e:
            print("Error in reading file: {}".format(e))

    def process_store_sales(self):
        reduced_sales_df = self.sales_df[["Store", "Weekly_Sales"]].groupby(["Store"]).agg({'Weekly_Sales': 'sum'})
        self.final_df = self.stores_df.join(reduced_sales_df, on='Store', how='inner')

    def load_data_into_db(self, db_name: str, schema_name: str):
        try:
            with sqlite3.connect(db_name) as con:
                self.final_df.to_sql(name=schema_name, con=con)
        except Exception as e:
            print("error occurred when trying to insert dataframe into DB: {}".format(e))


