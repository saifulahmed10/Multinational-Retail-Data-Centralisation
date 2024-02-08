from data_extraction import DataExtractor
import pandas as pd
import datetime
import tabula
import re 
import uuid


class DataCleaning:

    def clean_user_data(self): 
       extractor = DataExtractor()
       user_data = extractor.read_rds_table('legacy_users')
       # Convert data types
       user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format = '%Y-%m-%d', errors = 'coerce')
       user_data['join_date'] = pd.to_datetime(user_data['join_date'], format = '%Y-%m-%d', errors = 'coerce')
       user_data['phone_number'] = user_data['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex = True)
       # Drop Null, NA values
       user_data = user_data.dropna(how = 'any')
       # Drop duplicates
       user_data = user_data.drop_duplicates()
       # Set index
       user_data = user_data.set_index('index')
       user_data = user_data.sort_values(by = 'index', ascending = True)
       return user_data

    def clean_card_data(self):
        extractor = DataExtractor()

        try:
            # Read PDF and return a list of DataFrames
            card_data = tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all')

            # Check if card_data is not None before attempting to clean
            if card_data is not None:
                # Concatenate the list of DataFrames into a single DataFrame
                card_data = pd.concat(card_data, ignore_index=True)

                # Clean data: remove erroneous values, NULL values, or errors with formatting
                card_data = card_data.dropna(how='any')
                # Add other cleaning operations as needed
                # Replace "NULL" values in date_payment_confirmed column with a default date value
                default_date_value = pd.to_datetime('2000-01-01', format='%Y-%m-%d', errors='coerce')
                card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').fillna(default_date_value)

                # Convert the column to type DATE
                card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed']).dt.date

                # Drop duplicates
                card_data.drop_duplicates(subset='card_number', keep='first', inplace=True)

                # Handle strange entries
                strange_entries = ['NB71VBAHJE','WJVMUO4QX6', 'JRPRLPIBZ2', 'TS8A81WFXV', 'JCQMU8FN85', '5CJH7ABGDR', 'DE488ORDXY', 'OGJTXI6X1H', '1M38DYQTZV', 'DLWF2HANZF', 'XGZBYBYGUW', 'UA07L7EILH', 'BU9U947ZGV', '5MFWFBZRM9']
                condition_1 = card_data['card_provider'].isin(strange_entries)
                condition_2 = card_data['card_number'] == 'NULL'
                card_data = card_data.drop(card_data[condition_1 | condition_2].index)

                return card_data

            else:
                print("Unable to retrieve card data.")
                return None

        except Exception as e:
            print(f"Error reading PDF: {e}")
            return None

    def called_clean_store_data(self):
        extractor = DataExtractor()
        store_endpoint =  f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{450}'
        store_data = extractor.retrieve_stores_data(store_endpoint)
        try:
            store_data = store_data.drop(columns=['lat'])

            store_data['continent'] = store_data['continent'].str.replace('ee','')

            return store_data
        except Exception as e:
            print(f"An error has occurred during data cleaning: {e}")
            return None

    def convert_to_kg(self, weight_str):
        if isinstance(weight_str, str):
            match = re.match(r"(\d+(\.\d+)?)\s*x\s*(\d+(\.\d+)?)", weight_str)
            if match:
                try:
                    quantity = float(match.group(1))
                    unit_weight = float(match.group(3))
                    return quantity * unit_weight / 1000
                except ValueError:
                    return None
            else:
                return None
        else:
            return None

    def convert_kg_from_kg(self, weight_str):
        if isinstance(weight_str, str) and 'kg' in weight_str:
            try:
                return float(weight_str.replace('kg', ''))
            except ValueError:
                return None
        else:
            return None

    def convert_kg_from_g(self, weight_str):
        if isinstance(weight_str, str) and 'g' in weight_str:
            try:
                return float(weight_str.replace('g', '')) / 1000  # convert grams to kilograms
            except ValueError:
                return None
        else:
            return None

    def convert_kg_from_ml(self, weight_str):
        if isinstance(weight_str, str) and 'ml' in weight_str:
            try:
                ml_value = float(weight_str.replace('ml', ''))  # extract the numeric value in milliliters
                # Assuming a density of water (1g/ml), convert milliliters to kilograms
                return ml_value * 0.001
            except ValueError:
                return None
        else:
            return None

    def convert_product_weights(self, products_df):
        products_df['weight_in_kg'] = products_df['weight'].apply(lambda x: self.convert_to_kg(x) 
                                                                   if self.convert_to_kg(x) is not None 
                                                                   else (self.convert_kg_from_kg(x) 
                                                                         if self.convert_kg_from_kg(x) is not None 
                                                                         else (self.convert_kg_from_g(x) 
                                                                               if self.convert_kg_from_g(x) is not None 
                                                                               else self.convert_kg_from_ml(x))))

        return products_df

    def clean_products_data(self, products_df):
        # Drop rows with NULL values
        products_df = products_df.dropna()
        
        return products_df

    def clean_orders_data(self, orders_data):
        cleaned_data = orders_data.drop(columns=['first_name', 'last_name', '1', 'level_0'])
        return cleaned_data

    def clean_json_data(self, json_data):
        cleaned_json_data = json_data.dropna(how = 'any')
        cleaned_json_data['date'] = pd.to_datetime(cleaned_json_data[['year', 'month', 'day']], errors = 'coerce')
        def clean_uuid(value):
            try:
                return str(uuid.UUID(value))
            except ValueError:
                return str(uuid.uuid4())

        cleaned_json_data['date_uuid'] = cleaned_json_data['date_uuid'].apply(clean_uuid)
    
        return cleaned_json_data