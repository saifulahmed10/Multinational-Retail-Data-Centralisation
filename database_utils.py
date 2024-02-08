import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
import psycopg2


class DatabaseConnector:

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.engine = self.init_db_engine()
    
    def read_db_creds(self):
        with open(self.yaml_file, 'r') as f:
            data = yaml.safe_load(f)
            return data

    def init_db_engine(self):
        my_creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        RDS_USER = my_creds['RDS_USER']
        RDS_PASSWORD = my_creds['RDS_PASSWORD']
        RDS_HOST = my_creds['RDS_HOST']
        RDS_PORT = my_creds['RDS_PORT']
        RDS_DATABASE = my_creds['RDS_DATABASE']
        engine = create_engine(f"postgresql://{self.read_db_creds()['RDS_USER']}:{self.read_db_creds()['RDS_PASSWORD']}@{self.read_db_creds()['RDS_HOST']}:{self.read_db_creds()['RDS_PORT']}/{self.read_db_creds()['RDS_DATABASE']}")
        return engine
        
    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names

    def read_local_creds(self):
        with open (self.yaml_file, 'r') as f:
            data = yaml.safe_load(f)
            return data

    def upload_to_db(self, df, table_name):
        local_engine = create_engine(f"postgresql://{self.read_db_creds()['USER']}:{self.read_db_creds()['PASSWORD']}@{self.read_db_creds()['HOST']}:{self.read_db_creds()['PORT']}/{self.read_db_creds()['DATABASE']}")
        df.to_sql(table_name, local_engine, if_exists = 'replace')
        return df

    



# cleaning = DataCleaning()
# df = cleaning.clean_user_data()
# dim_user = df.to_sql('legacy_user', local_engine)

# connector = DatabaseConnector(yaml_file = 'db_creds.yaml')
# list_of_tables = connector.list_db_tables()
# credentials = DatabaseConnector.read_db_creds()
# postgres_obj = DatabaseConnector(**credentials)
# engine_1 = postgres_obj.init_db_engine()
# list_of_tables = DatabaseConnector('RDS_DATABASE', 'RDS_USER', 'RDS_PASSWORD', 'RDS_HOST', 'RDS_PORT')
# list_of_tables.list_db_tables()



# conn = postgres_obj.connect()