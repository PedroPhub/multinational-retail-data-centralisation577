import os
import requests
from io import StringIO

import pandas as pd
import boto3
import tabula


class DataExtractor:
    def read_rds_table(self, engine, table_name):
        """Read RDS table
        Keyword arguments:
        engine -- engine object that allows connection to the database
        table_name -- name of the table to be read from the RDS database"""
        self.engine = engine
        self.table_name = table_name
        rds_df = pd.read_sql_table(self.table_name, self.engine)
        return rds_df
    
    def retrieve_pdf_data(self, file_link):
        """Retrieve all pages from PDF file
        Keyword arguments:
        file_link -- link for the PDF file"""
        self.file_link = file_link
        cards_df = tabula.read_pdf(self.file_link,pages='all')
        return cards_df
    
    def list_number_of_stores(self, endpoint, header):
        """Returns the number of stores
        Keyword arguments:
        endpoint -- location where the API receives requests
        header -- header details for authentication"""
        self.endpoint = endpoint
        self.header = header
        number_stores = requests.get(self.endpoint, headers=self.header)
        return number_stores.json()

    def retrieve_stores_data(self, endpoint, header):
        """Retrieve stores dataframe
        Keyword arguments:
        endpoint -- location where the API receives requests
        header -- header details for authentication"""
        self.endpoint = endpoint
        self.header = header
        stores_df = requests.get(self.endpoint, headers=self.header)
        return stores_df.json()
    
    def extract_from_s3(self, address):
        """Extract and returns dataframe from S3
        Keyword arguments:
        address -- address to extract data from"""
        self.address = address

        # Connect to S3
        client = boto3.client('s3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY '),
            aws_session_token=os.environ.get('AWS_SESSION_TOKEN ')
        )

        # Separate the bucket and key from address parameter
        path_parts=address.replace("s3://","").split("/")
        bucket=path_parts.pop(0)
        key="/".join(path_parts)

        # Download csv object
        csv_obj = client.get_object(Bucket=bucket, Key=key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        products_df = pd.read_csv(StringIO(csv_string)) 
        return products_df
