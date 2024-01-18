import yaml
import sqlalchemy


class DatabaseConnector:

    def read_db_creds(self):
        """Reads and returns credentials from db_creds.yaml file"""
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
            return data

    def init_db_engine(self):
        """Establish URL and return engine object"""
        # Load credentials from YAML file
        connector_obj = DatabaseConnector()
        credentials = connector_obj.read_db_creds()
        
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

    def list_db_tables(self):
        """Return list of tables of database"""
        # Get engine object
        connector_obj = DatabaseConnector()
        engine = connector_obj.init_db_engine()
        
        # Get table names using inspect
        inspector = sqlalchemy.inspect(engine)
        table_names = inspector.get_table_names()
        return table_names

    def upload_to_db(self, dataframe, table_name):
        """Upload dataframe to database
        Keyword arguments:
        dataframe -- dataframe to send to database
        table_name -- name assigned for the table"""
        self.dataframe = dataframe
        self.table_name = table_name
        
        # Connect to local database
        engine = sqlalchemy.create_engine('postgresql://postgres:core@localhost:5432/sales_data')

        self.dataframe.to_sql(self.table_name,con=engine,if_exists='replace',index=False)
