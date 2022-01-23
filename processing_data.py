import pandas as pd
import sqlite3


class ProcessingData(object):

    def __init__(self, sales_file_path: str, stores_file_path: str):
        self.sales_file_path = sales_file_path
        self.stores_file_path = stores_file_path
        self.sales_df = pd.DataFrame
        self.stores_df = pd.DataFrame
        self.final_df = pd.DataFrame

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


