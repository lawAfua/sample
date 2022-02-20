import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, create_database

class DataDump(object):

    def __init__(self, host="localhost", schema="test_db", user="root", password="memecrack", port=3306):
        self._host = host
        self._name = schema
        self._port = port
        self._user = user
        self._pass = password

    def _connection_str(self):
        return "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(self._user, self._pass, self._host, self._port, self._name)

    def create_schema(self):
        try:
            connection_str = self._connection_str()
            engine = create_engine(connection_str)
            if not database_exists(engine.url):
                create_database(engine.url)
        except Exception as err:
            return "error occurred when creating database: {}".format(err)

    def dump_data(self, df: pd.DataFrame, table_name: str):
        try:
            connection_str = self._connection_str()
            engine = create_engine(connection_str)
            # with Session(engine) as session:
            df.to_sql(table_name, engine)
        except Exception as e:
            return "error occurred when trying to dump data into mysql: {}".format(e)

