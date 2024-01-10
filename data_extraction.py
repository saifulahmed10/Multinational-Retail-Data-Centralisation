from database_utils import DatabaseConnector
import pandas as pd


class DataExtractor:

    def __init__(self, yaml_file='db_creds.yaml'):
        self.db_connector = DatabaseConnector(yaml_file)
        
    def read_rds_table(self, table_name = 'legacy_users'):
        engine = self.db_connector.init_db_engine()
        try:
            df = pd.read_sql_table('legacy_users', engine)  
            data = pd.DataFrame(df)    
            return data
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return None


# db_connector = DatabaseConnector(yaml_file = 'db_creds.yaml')
# engine = db_connector.init_db_engine()
# extractor = DataExtractor()
# postgres_obj = DatabaseConnector(**credentials)
# engine_1 = postgres_obj.init_db_engine()
# extractor_engine = DataExtractor(engine_1)