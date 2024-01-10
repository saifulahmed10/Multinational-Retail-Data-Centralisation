from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# Connecting database and listing tables
RDS_CONNECTOR = DatabaseConnector(yaml_file = 'db_creds.yaml')
RDS_CONNECTOR.engine
data = RDS_CONNECTOR.init_db_engine()

# connector = DatabaseConnector(yaml_file = 'db_creds.yaml')
list_of_tables = RDS_CONNECTOR.list_db_tables()

# Extract data for legacy_users
extractor = DataExtractor()
user_data = extractor.read_rds_table('legacy_users')
print(user_data)

# Clean data for legacy_users
cleaning = DataCleaning()
cleaned_user_data = cleaning.clean_user_data()
print(cleaned_user_data)

# Upload table to database
RDS_CONNECTOR.upload_to_db(cleaned_user_data, 'dim_users')
# table_name = 'legacy_users'
# upload = local_connector.upload_to_db(df= data_cleaning, table_name= 'legacy_users')
# cleaning = DataCleaning()
# df = cleaning.clean_user_data()
# dim_user = df.to_sql('legacy_user', local_engine)


# credentials = DatabaseConnector.read_db_creds()