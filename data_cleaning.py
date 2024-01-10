from data_extraction import DataExtractor
import pandas as pd
import datetime


class DataCleaning:

    def clean_user_data(self): 
       extractor = DataExtractor()
       df = extractor.read_rds_table()
       # Convert data types
       df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format = '%Y-%m-%d', errors = 'coerce')
       df['join_date'] = pd.to_datetime(df['join_date'], format = '%Y-%m-%d', errors = 'coerce')
       df['phone_number'] = df['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex = True)
       # Drop Null, NA values
       df = df.dropna(how = 'any')
       # Drop duplicates
       df = df.drop_duplicates()
       # Set index
       df = df.set_index('index')
       df = df.sort_values(by = 'index', ascending = True)
       return df



# Add instance from Extraction class
# extractor = DataExtractor()
# extractor.read_rds_table()