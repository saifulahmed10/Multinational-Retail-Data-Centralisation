# Multinational Retail Data Centralisation

# AiCore Project Overview

This project encapsulates all the content learnt at AiCore so far, including Python, GitHub, Pandas, SQL, and various other skills like calling APIs and using AWS for data retrieval. The aim of the project is to extract, clean, and upload data to an SQL database called sales_data using PostgreSQL pgAdmin. Subsequently, this data will be queried using SQL statements to derive insights and metrics relevant to typical project requirements.

Project Structure
The project consists of several key components:

Data Extraction: Python scripts are employed to extract data from various sources, including APIs and external data files.

Data Cleaning: Pandas is utilized for data cleaning and transformation to ensure the data is in a suitable format for database upload.

Database Upload: The cleaned data is then uploaded to the PostgreSQL database, sales_data, using pgAdmin for seamless integration.

Technologies Used
The project leverages a variety of technologies:

Python: Utilized for scripting data extraction, cleaning, and transformation.

Pandas: Employed for data manipulation and cleaning tasks.

GitHub: Version control and collaboration via GitHub to manage project changes and updates.

PostgreSQL and pgAdmin: Utilized for database management and SQL querying.

AWS: AWS services are used for data retrieval and storage, enhancing the project's scalability and flexibility.

Challenges and Solutions
Throughout the project, challenges such as data integrity and API integration were encountered. By applying the skills learned at AiCore, these challenges were addressed through rigorous data validation techniques and API authentication methodologies, ensuring the integrity and security of the extracted data.

Expected Output
The project aims to derive various metrics and insights from the sales_data database through SQL querying. Examples of these insights include sales trends, customer behavior analysis, and inventory management indicators. Specific SQL queries will be formulated to extract these metrics, providing valuable business intelligence.



 

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

# DataExtractor - this class is used to extract all the data that is obtained through different means.

For example:

Extract yaml file
Extract pdf file from a link
Extracting file by api request
Extracting from s3 bucket
Extracting JSON file from s3 bucket

# DataCleaning - this class is used to clean all the data that has been extracted using pandas as pd.

Different cleaning methods were used for example dropping NULL values, dropping duplicates, adjusting date to fit format, convert column to kg from g and ml.

# SQL statements for star based schema.

SQL statements were used to change the data types of all the dim tables aswell as the orders_table, using the ALTER TABLE [table_name] ALTER COLUMN [column_name] TYPE [data_type] statements.

Primary keys were added to the columns that were present in the dim tables as well as the orders table using ALTER TABLE [table_name] ADD PRIMARY KEY [column_name]

Foreign keys were added to the orders_table column for the corresponding primary key.




