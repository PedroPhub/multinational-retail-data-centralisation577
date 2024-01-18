import yaml
import sqlalchemy


class DatabaseConnector:

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
            return data

    # Establish connection and return engine object
    def init_db_engine(self):
        # Load credentials from YAML file
        db_obj = DatabaseConnector()
        credentials = db_obj.read_db_creds()
        
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

    # Return table list from database
    def list_db_tables(self):
        # Get engine object
        db_obj = DatabaseConnector()
        engine = db_obj.init_db_engine()
        
        # Get table names using inspect
        inspector = sqlalchemy.inspect(engine)
        table_names = inspector.get_table_names()
        return table_names

    def upload_to_db(self, dataframe, table_name):
        self.dataframe = dataframe
        self.table_name = table_name
        
        # Connect to local database
        engine = sqlalchemy.create_engine('postgresql://postgres:core@localhost:5432/sales_data')

        self.dataframe.to_sql(self.table_name,con=engine,if_exists='replace',index=False)
