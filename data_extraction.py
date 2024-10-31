import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import tabula
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"


class DataExtractor:

    def __init__(self, DatabaseConnector):
        self.DatabaseConnector = DatabaseConnector
        
    def read_rds_tables(self, DatabaseConnector):
        engine = DatabaseConnector.init_db_engine()
        users_df = pd.read_sql_table('legacy_users', engine)
        return users_df
    
    def retrieve_pdf_data(self, pdf_path):
        df = tabula.read_pdf(pdf_path, stream=True)
        return df