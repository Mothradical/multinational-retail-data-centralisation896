import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import requests
import tabula
import ast
import boto3
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"


class DataExtractor:

    def __init__(self, DatabaseConnector):
        self.DatabaseConnector = DatabaseConnector
        
    def read_rds_tables(self, DatabaseConnector, table_name):
        engine = DatabaseConnector.init_db_engine()
        users_df = pd.read_sql_table(table_name, engine)
        return users_df
    
    def retrieve_pdf_data(self, pdf_path):
        dfs = tabula.read_pdf(pdf_path, multiple_tables=True, stream=True, lattice=True, pages='all')
        df = pd.concat(dfs)
        return df

    def list_number_of_stores(self, endpoint, headers_dict):
        response = requests.get(endpoint, headers=headers_dict)
        return response

    def retrieve_stores_data(self):
        x = list(range(0, 451))
        list_of_dicts = []
        for n in x:
            url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{n}"
            headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
            response = requests.get(url, headers=headers)
            data = response.text
            data_dict = ast.literal_eval(data.replace("null", "'N/A'"))
            list_of_dicts.append(data_dict)
        data_df = pd.DataFrame(list_of_dicts)
        return data_df
    
    def extract_from_s3(self, url):
        data = pd.read_csv(url)
        return data