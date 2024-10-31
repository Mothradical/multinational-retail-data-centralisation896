import pandas as pd

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
        self.drop_list.extend(list(df.index[df['join_date'] == 'AHN6EKASH3']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'FYF2FAPZF3']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'QH6Z9ZPX37']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'LYVWXBBI6F']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'DM4Q84QZ03']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'DOKMYDVV6L']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'YBUYH8T6OE']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'SRH5SM36LH']))
        self.drop_list.extend(list(df.index[df['join_date'] == '4JIOCHZY0W']))
        self.drop_list.extend(list(df.index[df['join_date'] == '3CUODA3HTC']))
        self.drop_list.extend(list(df.index[df['join_date'] == '8BAER2328P']))
        self.drop_list.extend(list(df.index[df['join_date'] == 'U9CRKSTONU']))
        self.drop_list.extend(list(df.index[df['join_date'] == '9YLGYDEZNV']))
        self.drop_list.extend(list(df.index[df['join_date'] == '7PF0SMLXII']))
        self.drop_list.append(752)
        drop_set = set(self.drop_list)
        self.drop_list = list(drop_set)
        df = df.drop(self.drop_list)
        df['join_date'] = pd.to_datetime(df['join_date'])
        return df