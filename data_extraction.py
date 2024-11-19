import pandas as pd
import requests
import tabula
import ast
localpath = 'C:/Users/elshu/OneDrive/Documents/Data/AiCore_Projects/multinational-retail-data-centralisation896/db_creds.yaml'
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
product_details_url = 's3://data-handling-public/products.csv'
headers_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


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

    def list_number_of_stores(self, endpoint, headers_dict):
        '''
        This method finds the 'numer of stores' from a URL
        '''
        response = requests.get(endpoint, headers=headers_dict)
        return response.text

    def retrieve_stores_data(self, headers_dict):
        '''
        This method retrieves store data from 451 iterations of a URL
        and returns them as a single Pandas Dataframe
        '''
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