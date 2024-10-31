import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DataExtractor:

    def __init__(self, DatabaseConnector):
        self.DatabaseConnector = DatabaseConnector
        
    def read_rds_tables(self, DatabaseConnector):
        engine = DatabaseConnector.init_db_engine()
        users_df = pd.read_sql_table('legacy_users', engine)
        return users_df