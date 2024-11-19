import pandas as pd
import numpy as np


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