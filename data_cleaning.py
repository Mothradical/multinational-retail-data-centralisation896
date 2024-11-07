import pandas as pd
import numpy as np

class DataCleaning:

    def __init__(self, df):
        self.df = df # Dataframe
        self.drop_list = drop_list

        drop_list = []

    def clean_user_data(self, df):
        df['join_date'].replace('2006 September 03', '2006-09-03', inplace=True)
        self.drop_list.extend(list(df.index[df['index'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['first_name'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['last_name'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['date_of_birth'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['company'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['email_address'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['address'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['country'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['country_code'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['phone_number'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'NULL']))
        self.drop_list.extend(list(df.index[df['user_uuid'] == 'NULL']))
        drop_set = set(self.drop_list)
        self.drop_list = list(drop_set)
        df = df.drop(self.drop_list)
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df = df.dropna()
        return df

    def clean_card_data(self, card_df):
        card_df = card_df.drop_duplicates(subset='card_number', keep=False)
        card_df = card_df.replace("?4971858637664481", "4971858637664481")
        card_df = card_df.replace("???3554954842403828", "3554954842403828")
        card_df = card_df.replace("??4654492346226715", "4654492346226715")
        card_df = card_df.replace("?3544855866042397", "3544855866042397")
        card_df = card_df.replace("??2720312980409662", "2720312980409662")
        card_df = card_df.replace("??4982246481860", "4982246481860")
        card_df = card_df.replace("?213174667750869", "213174667750869")
        card_df = card_df.replace("????3505784569448924", "3505784569448924")
        card_df = card_df.replace("????3556268655280464", "3556268655280464")
        card_df = card_df.replace("???2604762576985106", "2604762576985106")
        card_df = card_df.replace("???5451311230288361", "5451311230288361")
        card_df = card_df.replace("???4252720361802860591", "4252720361802860591")
        card_df = card_df.replace("?4222069242355461965", "4222069242355461965")
        card_df = card_df.replace("?4217347542710", "4217347542710")
        card_df = card_df.replace("?584541931351", "584541931351")
        card_df = card_df.replace("???4672685148732305", "4672685148732305")
        card_df = card_df.replace("??3535182016456604", "3535182016456604")
        card_df = card_df.replace("????3512756643215215", "3512756643215215")
        card_df = card_df.replace("?2314734659486501", "2314734659486501")
        card_df = card_df.replace("????341935091733787", "341935091733787")
        card_df = card_df.replace("????3543745641013832", "3543745641013832")
        card_df = card_df.replace("??575421945446", "575421945446")
        card_df = card_df.replace("??630466795154", "630466795154")
        card_df = card_df.replace("????38922600092697", "38922600092697")
        card_df = card_df.replace("????34413243759859811", "34413243759859811")
        card_df = card_df.replace("???4814644393449676", "4814644393449676")
        card_df = card_df.replace("????344132437598598", "344132437598598")
        card_df = card_df[pd.to_numeric(card_df['card_number'], errors='coerce').notnull()]
        card_df['card_number'] = pd.to_numeric(card_df['card_number'])
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], format='mixed')
        card_df = card_df.dropna()
        return card_df
    
    def clean_called_clean_store_data(self, df):
        df = df.drop(columns="lat")
        df = df.replace(to_replace='NULL', value=np.nan)
        df = df.dropna(subset="opening_date")
        df['opening_date'].replace('October 2012 08', '2012-10-08', inplace=True)
        df['opening_date'].replace('"July 2015 14', '2015-07-14', inplace=True)
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', errors='coerce')
        df = df.dropna()
        df['staff_numbers'] = df['staff_numbers'].map(lambda x: ''.join([i for i in x if i.isdigit()]))
        return df
    
    def convert_products_weight(self, data):
        def convert(weight):
            if '*' in weight:
                return eval(weight[:-1])/1000
            elif weight[-2:] == "kg":
                return weight
            elif weight[-1:] == "g":
                return float(weight[:-1])/1000
            elif weight[-2:] == "oz":
                return float(weight[:-2])/35.274
            elif weight[-3] == "g":
                return float(weight[:-3])/1000
            else:
                return weight
        
        data['weight'] = data['weight'].astype(str)
        data['weight'] = data['weight'].str.replace('ml', 'g')
        data['weight'] = data['weight'].str.replace('x', '*')
        data['weight'] = data['weight'].apply(convert)
        data['weight'].replace('9GO9NZ5JTL', np.nan, inplace=True)
        data['weight'].replace('Z8ZTDGUZVU', np.nan, inplace=True)
        data['weight'].replace('MX180RYSHX', np.nan, inplace=True)
        data['weight'] = data['weight'].astype(float)
        return data
    
    def clean_products_data(self, data):
        data = data.dropna()
    
    def clean_orders_data(self, data):
        drop_columns = ['level_0', 'first_name', 'last_name', '1']
        data = data.drop(columns=drop_columns)
        return data
