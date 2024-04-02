import os
import requests
from io import StringIO
import pandas as pd
import boto3
import tabula
from sqlalchemy import engine


class DataExtractor:
    def __init__(self, engine: engine = None, table_name: str = None, header: dict[str, str] = None) -> None:
        """
        This class provides functionality for extracting data from databases and return it in a structured format
        
        Keyword arguments:
            'engine': engine -- Engine object that allows connection to the database;
            'table_name': str -- Name of the table to be read from the RDS database;
        """ 
        self.engine = engine
        self.table_name = table_name
        self.header = header
    
    def read_rds_table(self) -> pd.DataFrame:
        """
        Read RDS table using the instance table name and engine to connect

        Returns: 
            'rds_df': pd.DataFrame -- DataFrame returned from RDS;
        """
        rds_df = pd.read_sql_table(table_name=self.table_name, con=self.engine)
        return rds_df
    
    @staticmethod
    def retrieve_pdf_data(file_link: str) -> pd.DataFrame:
        """
        Retrieve all pages from PDF file
        
        Keyword arguments: 
            'file_link': str -- URL for the PDF file;

        Returns : 
            'pdf_df': pd.DataFrame -- DataFrame from PDF file;
        """
        pdf_df = tabula.read_pdf(input_path=file_link,pages='all')
        return pdf_df
    
    def list_number_of_stores(self, endpoint: str) -> dict[str, int]:
        """
        Returns the number of stores
        
        Keyword arguments:
            'endpoint': str -- Location where the API receives requests;
        
        Returns:
            'number_stores': dict[str, int] -- Dictionary with operation status code and number of stores;
        """
        self.endpoint = endpoint

        try:
            response = requests.get(self.endpoint, headers=self.header)
            response.raise_for_status()
            number_stores = response.json()
        except requests.exceptions.RequestException as exception:
            print(f"Request failed: {exception}")
            number_stores = None
        finally:
            if response:
                response.close()
        
        return number_stores
    
    def retrieve_stores_data(self, endpoint: str) -> dict:
        """
        Retrieve stores dataframe
        
        Keyword arguments:
            'endpoint': str -- Location where the API receives requests;
        
        Returns:
            'stores_df': dict[*] -- Dictionary with complete data for a store;
        """
        self.endpoint = endpoint

        try:
            response = requests.get(self.endpoint, headers=self.header)
            response.raise_for_status()
            stores_df = response.json()
        except requests.exceptions.RequestException as exception:
            print(f"Request failed: {exception}")
            stores_df = None
        finally:
            if response:
                response.close()

        return stores_df
    
    def extract_from_s3(self, address: str) -> pd.DataFrame:
        """
        Extract and returns dataframe from S3
        
        Keyword arguments:
            'address': str -- Address to extract data from;
        
        Returns:
            'products_df': pd.dataframe -- DataFrame from S3 address;
        """
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
