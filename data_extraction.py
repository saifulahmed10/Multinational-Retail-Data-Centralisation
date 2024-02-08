from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3


class DataExtractor:

    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    
    def __init__(self, yaml_file='db_creds.yaml'):
        self.db_connector = DatabaseConnector(yaml_file)
        
    def read_rds_table(self, table_name):
        engine = self.db_connector.init_db_engine()
        try:
            df = pd.read_sql_table(table_name, engine)  
            data = pd.DataFrame(df)    
            return data
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return None
    
    def retrieve_pdf_data(self, pdf_link):
        pdf_df = tabula.read_pdf(pdf_link, pages='all')
        return pdf_df

    def list_number_of_stores(self):
        api_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        number_stores_url = f"{api_url}"
        try:
            response = requests.get(number_stores_url, headers=self.headers)

            if response.status_code == 200:
                number_of_stores = response.json().get('number_stores')
                return number_of_stores
            else:
                print(f"Failed to retrieve the number of stores. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    
    def retrieve_stores_data(self, store_endpoint):
        try:
            response = requests.get(store_endpoint, headers=self.headers)
            print('Response status code:', response.status_code)
            if response.status_code == 200:
                stores_data = response.json()
                df = pd.DataFrame(stores_data, index = [0])
                return df
            else:
                print(f"Failed to retrieve the store data. Status code: {response.status_code}")
                print('Response content:', response.content.decode('utf-8'))
                return None
        except Exception as e:
            print(f"An error has occurred: {e}")
            return None

    def extract_from_s3(self, s3_address):
        try:
            s3 = boto3.client('s3')

            s3_parts = s3_address.replace('s3://', '').split('/')
            bucket_name = s3_parts[0]
            object_key = '/'.join(s3_parts[1:])

            local_file_path = 'local_s3_file.csv'
            s3.download_file(bucket_name, object_key, local_file_path)

            df = pd.read_csv(local_file_path)

            return df
        except Exception as e:
            print(f"An error occurred while extraction from S3: {e}")
            return None
        
    def extract_json_from_s3(self, s3_url):
        s3 = boto3.client('s3')
        response = requests.get(s3_url)
        data = response.json()
        s3_data = pd.DataFrame(data)
        return s3_data

                



pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

