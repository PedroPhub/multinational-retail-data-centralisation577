import pandas as pd
import yaml
import sqlalchemy


class DatabaseConnector:
    def __init__(self, filename: str = None, dataframe: pd.DataFrame = None, table_name: str = None) -> None:
        """
        This class provides functionality for connecting to databases and retrieve or upload DataFrames.
        
        Keyword arguments:
            'filename': str -- Engine object that allows connection to the database;
            'dataframe': pd.DataFrame -- DataFrame to be uploaded;
            'table_name': str -- Name of the table to be read from the RDS database;
        """
        self.filename = filename
        self.dataframe = dataframe
        self.table_name = table_name
    
    def read_db_creds(self) -> dict[str, any]:
        """
        Reads and returns credentials from db_creds.yaml file
        
        Returns:
            'data': dict -- Dictionary containing the credentials from yaml file;
        """
        with open(self.filename, 'r') as file:
            data = yaml.safe_load(file)
            return data

    def init_db_engine(self) -> sqlalchemy.engine:
        """
        Establish URL and return engine object
        
        Returns:
            'engine': engine -- Engine object using credentials from read_db_creds();
        """
        # Load credentials from YAML file
        credentials = self.read_db_creds()
        
        # Establish the URL to create the engine object
        url_object = sqlalchemy.URL.create(
            "postgresql",
            username=credentials['RDS_USER'],
            password=credentials['RDS_PASSWORD'],
            host=credentials['RDS_HOST'],
            database=credentials['RDS_DATABASE'],
            )
        
        # Create engine object
        engine = sqlalchemy.create_engine(url_object)
        return engine

    def list_db_tables(self) -> list[str]:
        """
        Get list of tables from database
        
        Returns:
            'table_names': list -- List of tables names available on database;
        """
        # Get engine object
        engine = self.init_db_engine()
        # Get table names
        inspector = sqlalchemy.inspect(engine)
        table_names = inspector.get_table_names()
        return table_names

    def upload_to_db(self) -> None:
        """
        Upload dataframe to database
        """
        # Connect to local database
        with open(self.filename, 'r') as file:
            database_link = yaml.safe_load(file)
        engine = sqlalchemy.create_engine(database_link['url'])
        
        self.dataframe.to_sql(self.table_name,engine,if_exists='replace',index=False)