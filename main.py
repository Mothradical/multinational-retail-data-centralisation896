import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import numpy as np
import requests
import tabula
import ast
localpath = 'C:/Users/elshu/OneDrive/Documents/Data/AiCore_Projects/multinational-retail-data-centralisation896/db_creds.yaml'
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
product_details_url = 's3://data-handling-public/products.csv'
sec_det_pth = 'C:/Users/elshu/OneDrive/Documents/Data/AiCore_Projects/multinational-retail-data-centralisation896/security_creds.yaml'
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


class DatabaseConnector:

    def __init__(self):
        '''
        This class is used to connect and upload to a local database
        '''
        print("init")

    def read_db_creds(self, localpath):
        '''
        This method reads a local yaml file from a local filepath
        '''
        with open(localpath) as creds:
            cred_dict = yaml.full_load(creds)
        return cred_dict

    def init_db_engine(self, localpath):
        '''
        This method initialises an sqlalchemy engine using database
        credentials stored in a yaml file
        '''
        cred_dict = self.read_db_creds(localpath)
        engine = create_engine(f"postgresql+psycopg2://{cred_dict['RDS_USER']}:{cred_dict['RDS_PASSWORD']}@{cred_dict['RDS_HOST']}:{cred_dict['RDS_PORT']}/{cred_dict['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, localpath):
        '''
        This method returns a list of tables using database credentials
        stored in a yaml file
        '''
        engine = self.init_db_engine(localpath)
        inspector = inspect(engine)
        list_of_tables = inspector.get_table_names()
        return list_of_tables

    def upload_to_db(self, df, table_name, sec_det_pth):
        '''
        This method transforms a Pandas Dataframe to SQL and
        uploads it to a database as a Table
        '''
        sec_creds = self.read_db_creds(sec_det_pth)
        upload_path = sec_creds['upload_path']
        engine = create_engine(upload_path)
        df.to_sql(table_name, engine)

class DataExtractor:

    def __init__(self):
        print("init")
        
    def read_rds_tables(self, DatabaseConnector, table_name, localpath):
        '''
        This method reads data from a specified table and converts it into a Pandas Dataframe
        '''
        engine = DatabaseConnector.init_db_engine(localpath)
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self, pdf_path):
        '''
        This method reads an online pdf as a list of Pandas Series and converts them into a Dataframe
        '''
        dfs = tabula.read_pdf(pdf_path, multiple_tables=True, stream=True, lattice=True, pages='all')
        df = pd.concat(dfs)
        return df

    def list_number_of_stores(self, DatabaseConnector, sec_det_pth, endpoint):
        '''
        This method finds the 'numer of stores' from a URL
        '''
        headers_dict = DatabaseConnector.read_db_creds(sec_det_pth)
        response = requests.get(endpoint, headers=headers_dict)
        return response.text

    def retrieve_stores_data(self, DatabaseConnector, sec_det_pth):
        '''
        This method retrieves store data from 451 iterations of a URL
        and returns them as a single Pandas Dataframe
        '''
        headers_dict = DatabaseConnector.read_db_creds(sec_det_pth)
        store_numbers = list(range(0, 451))
        list_of_dicts = []
        for store_number in store_numbers:
            url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            headers = headers_dict
            response = requests.get(url, headers=headers)
            data = response.text
            data_dict = ast.literal_eval(data.replace("null", "'N/A'"))
            list_of_dicts.append(data_dict)
        df = pd.DataFrame(list_of_dicts)
        return df
    
    def extract_from_s3(self, product_details_url):
        '''
        This method extracts data from an AWS S3 bucket
        '''
        data = pd.read_csv(product_details_url)
        return data

class DataCleaning:

    def __init__(self):
        print("init")
        '''
        This class contains methods for cleaning data
        '''

    def clean_user_data(self, df):
        '''
        This method is purpose-designed to clean a specific table of user data
        '''
        df = df.replace("NULL", np.nan)
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed', errors='coerce')
        df = df.dropna()
        return df

    def clean_card_data(self, df):
        '''
        This method is purpose-designed to clean a specific table of payment card data
        '''
        df['card_number'] = df['card_number'].astype(str)
        df['card_number'] = df['card_number'].str.replace('?', '')
        df = df.drop_duplicates(subset='card_number', keep=False)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed', errors='coerce')
        df = df.dropna()
        return df
    
    def clean_store_data(self, df):
        '''
        This method is purpose-designed to clean a specific table of store data
        '''
        df = df.drop(columns="lat")
        df = df.replace(to_replace='NULL', value=np.nan)
        df = df.dropna(subset="opening_date")
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', errors='coerce')
        df = df.dropna()
        df['staff_numbers'] = df['staff_numbers'].map(lambda x: ''.join([i for i in x if i.isdigit()]))
        return df
    
    def convert_products_weight(self, df):
        '''
        This method takes a 'weight' column in a Pandas Dataframe and converts
        each record to a numeric value equivalent to their KG value
        '''
        def convert(weight):
            if '*' in weight:
                return eval(weight[:-1])/1000
            elif weight[-2:] == "kg":
                return weight[:-2]
            elif weight[-1:] == "g":
                return float(weight[:-1])/1000
            elif weight[-2:] == "oz":
                return float(weight[:-2])/35.274
            elif weight[-3] == "g":
                return float(weight[:-3])/1000
            else:
                return weight
        
        df['weight'] = df['weight'].astype(str)
        df['weight'] = df['weight'].str.replace('ml', 'g')
        df['weight'] = df['weight'].str.replace('x', '*')
        df['weight'] = df['weight'].apply(convert)
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce', downcast='float')
        return df
    
    def clean_products_data(self, data):
        '''
        This method is purpose-designed to clean a specific table of products data
        '''
        data = data.dropna()
        return data
    
    def clean_orders_data(self, data):
        '''
        This method is purpose-designed to clean a specific table of orders data
        '''
        drop_columns = ['level_0', 'first_name', 'last_name', '1']
        data = data.drop(columns=drop_columns)
        return data

db_con = DatabaseConnector()
data_ext = DataExtractor()
data_clean = DataCleaning()

df = data_ext.retrieve_stores_data(db_con, sec_det_pth)
print(df.info())