import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
localpath = 'C:/Users/elshu/OneDrive/Documents/Data/AiCore_Projects/multinational-retail-data-centralisation896/db_creds.yaml'
upload_path = 'postgresql://postgres:password@localhost:5432/sales_data'


class DatabaseConnector:

    def __init__(self):
        print("init")

    def read_db_creds(self, localpath):
        with open(localpath) as creds:
            cred_dict = yaml.full_load(creds)
        return cred_dict

    def init_db_engine(self, localpath):
        cred_dict = self.read_db_creds(localpath)
        engine = create_engine(f"postgresql+psycopg2://{cred_dict['RDS_USER']}:{cred_dict['RDS_PASSWORD']}@{cred_dict['RDS_HOST']}:{cred_dict['RDS_PORT']}/{cred_dict['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, localpath):
        engine = self.init_db_engine(localpath)
        inspector = inspect(engine)
        list_of_tables = inspector.get_table_names()
        return list_of_tables

    def upload_to_db(self, df, table_name, upload_path):
        self.engine = create_engine(upload_path)
        df.to_sql(table_name, self.engine)