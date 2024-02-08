Update your GitHub repository with the latest code changes from your local project. Start by staging your modifications and creating a commit. Then, push the changes to your GitHub repository.
Additionally, document your progress by adding to your GitHub README file. You can refer to the relevant lesson in the prerequisites for this task for more information.
At minimum, your README file should contain the following information:
Project Title
Table of Contents, if the README file is long
A description of the project: what it does, the aim of the project, and what you learned
Installation instructions
Usage instructions
File structure of the project
License information

## Multinational retail data centralisation project

Description:
This project encapsulates all the content learnt at AiCore so far, including Python, GitHub, Pandas, SQL and various other skills likes calling api's and using AWS for retrieve data. The aim of the project is to extract data, clean data and then upload the data to an SQL database called sales_data using PostgreSQL pgadmin. This data will then be queried using SQL statements to find out different metrics and information that a typical project would ask. 

## Table of contents:
- [Classes included and methods] (#classes included and methods)
- [Pandas code for data cleaning] (#pandas code for data cleaning)
- [SQL statements] (#SQL statements)

## Classes included and methods

### DatabaseConnector - this is used to connect the database, using SQLAlchemy and create engine.

### Within this class there is an __init__ method that includes:
self.yaml_file = yaml_file
self.engine = self.init_db_engine()

### Other methods include:
read_db_creds(self): this allows the yaml file to be opened and loaded safely and then it is returned.

def read_db_creds(self):
        with open(self.yaml_file, 'r') as f:
            data = yaml.safe_load(f)
            return data
            
init_db_engine(self): this is where you initilise the engine and allows the connection using the credentials that was provided by AiCore.

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
        
list_db_tables(self): this method lists the tables from the credentials provided in a list.

def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names



# DataExtractor - this class is used to 



