import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import psycopg2


class DatabaseConnector:

    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name

    def read_db_creds(self):
        with open('C:/Users/elshu/OneDrive/Documents/Data/AiCore_Projects/multinational-retail-data-centralisation896/db_creds.yaml') as creds:
            cred_dict = yaml.full_load(creds)
        return cred_dict

    def init_db_engine(self):
        cred_dict = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{cred_dict['RDS_USER']}:{cred_dict['RDS_PASSWORD']}@{cred_dict['RDS_HOST']}:{cred_dict['RDS_PORT']}/{cred_dict['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        list_of_tables = inspector.get_table_names()
        return list_of_tables

    def upload_to_db(self, df, table_name):
        engine = create_engine('postgresql://postgres:password@localhost:5432/sales_data')
        df.to_sql(table_name, engine)

## NEXT TIME! TRY AND USE THIS ABOVE METHOD TO ADD DATA TO PGADMIN


#        HOST = 'local_host'
#        USER = 'postgres'
#        PASSWORD = 'password'
#        DATABASE = 'sales_data'
#        PORT = 5432
#        with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE, port=PORT) as conn:
#            with conn.cursor() as cur: