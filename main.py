from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# Connecting database and listing tables
RDS_CONNECTOR = DatabaseConnector(yaml_file = 'db_creds.yaml')
RDS_CONNECTOR.engine
data = RDS_CONNECTOR.init_db_engine()

# list of the tables
list_of_tables = RDS_CONNECTOR.list_db_tables()

# Extract data for legacy_users
extractor = DataExtractor()
user_data = extractor.read_rds_table('legacy_users')

# Clean data for legacy_users
cleaning = DataCleaning()
cleaned_user_data = cleaning.clean_user_data()

# Upload table to database
RDS_CONNECTOR.upload_to_db(cleaned_user_data, table_name = 'dim_users')

# Extract PDF data
card_data = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# Clean card_data
cleaning = DataCleaning()
cleaned_card_data = cleaning.clean_card_data()

# Upload cleaned_card_data
RDS_CONNECTOR.upload_to_db(cleaned_card_data, table_name = 'dim_card_details')

# API number_of_stores
api_url =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
extractor = DataExtractor()
number_stores_url = f'{api_url}'
number_of_stores = extractor.list_number_of_stores()

# Retrieve store
extractor = DataExtractor()
number_of_stores = extractor.list_number_of_stores()
store_endpoint =  f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{450}'
stores_data_df = extractor.retrieve_stores_data(store_endpoint)

# Clean store data
cleaning = DataCleaning()
cleaned_store_data = cleaning.called_clean_store_data()

# Upload cleaned_store_data
RDS_CONNECTOR.upload_to_db(cleaned_store_data, table_name = 'dim_store_details')

# S3 data extraction
extractor = DataExtractor()
s3_address = 's3://data-handling-public/products.csv'
data_from_s3 = extractor.extract_from_s3(s3_address)

# Convert and clean the product data
extractor = DataExtractor()
s3_address = 's3://data-handling-public/products.csv'
products_df = extractor.extract_from_s3(s3_address)
cleaning = DataCleaning()
product_data = cleaning.convert_product_weights(products_df)
cleaned_product_data = cleaning.clean_products_data(product_data)

# Upload cleaned_product_data
RDS_CONNECTOR.upload_to_db(cleaned_product_data, table_name='dim_product_data')

# Extract the orders data
extractor = DataExtractor()
orders_data = extractor.read_rds_table('orders_table')

# Clean orders data
cleaning = DataCleaning()
cleaned_orders_data = cleaning.clean_orders_data(orders_data)

# Upload cleaned_orders_data
RDS_CONNECTOR.upload_to_db(cleaned_orders_data, table_name='orders_table')

# Extract data from S3
s3_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
extractor = DataExtractor()
data_from_s3_url = extractor.extract_json_from_s3(s3_url)

# Convert and clean the date data
cleaning = DataCleaning()
data_from_s3_url = extractor.extract_json_from_s3(s3_url)
cleaned_json_data = cleaning.clean_json_data(data_from_s3_url)
print(cleaned_json_data)

# Upload cleaned_json_data
RDS_CONNECTOR.upload_to_db(cleaned_json_data, table_name='dim_date_times')







# table_name = 'legacy_users'
# upload = local_connector.upload_to_db(df= data_cleaning, table_name= 'legacy_users')
# cleaning = DataCleaning()
# df = cleaning.clean_user_data()
# dim_user = df.to_sql('legacy_user', local_engine)


# credentials = DatabaseConnector.read_db_creds()