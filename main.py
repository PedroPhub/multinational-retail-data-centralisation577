import pandas as pd
import yaml
from data_handling.database_utils import DatabaseConnector
from data_handling.data_cleaning import DataCleaning
from data_handling.data_extraction import DataExtractor


def read_yaml_data(filename: str) -> dict[str, str]:
    with open(filename, 'r') as file:
        yaml_data = yaml.safe_load(file)
    return yaml_data

def clean_user() -> None:
    # Read the credentials
    rds_connector = DatabaseConnector(filename='db_creds.yaml')
    engine = rds_connector.init_db_engine()

    # Extract RDS table to dataframe
    rds_extractor = DataExtractor(engine=engine, table_name='legacy_users')
    users_df = rds_extractor.read_rds_table()

    # Perform the cleaning of the user data
    clean_users_obj = DataCleaning(users_df)
    clean_user_df = clean_users_obj.clean_user_data()
    print(clean_user_df)
    # Upload of dataframe
    postgres_conn_users = DatabaseConnector(filename='postgres_link.yaml', dataframe=clean_user_df, table_name='dim_users')
    postgres_conn_users.upload_to_db()

def clean_card() -> None:
    # Extract PDF pages from document
    urls = read_yaml_data('links.yaml')
    pdf_extractor = DataExtractor()
    card_df = pdf_extractor.retrieve_pdf_data(urls['s3_pdf_url'])

    # Perform the cleaning of the card data
    clean_card_obj = DataCleaning(card_df)
    clean_card_df = clean_card_obj.clean_card_data()

    # Send to database
    postgres_conn_cards = DatabaseConnector(filename='postgres_link.yaml', dataframe=clean_card_df, table_name='dim_card_details')
    postgres_conn_cards.upload_to_db()

def clean_stores() -> None:
    # Return the number of stores to extract
    api_data = read_yaml_data('API.yaml')
    api_header = {'x-api-key': api_data['api_key']}

    list_stores_obj = DataExtractor(header=api_header)
    number_of_stores = list_stores_obj.list_number_of_stores(endpoint=api_data['number_stores_url'])

    # Create empty store DataFrame and collect data from retrieve_stores_data() for each store number
    store_data_df = pd.DataFrame()
    store_data_obj = DataExtractor(header=api_header)
    for store in range(number_of_stores['number_stores']):
        store_data = store_data_obj.retrieve_stores_data(endpoint=api_data['store_data'].format(store))
        store_data_df = pd.concat([store_data_df, pd.DataFrame([store_data])],ignore_index=True)

    # Perform the cleaning of the stores data
    clean_stores_obj = DataCleaning(dataframe=store_data_df)
    clean_stores_df = clean_stores_obj.clean_store_data()

    # Send to database
    postgres_conn_stores = DatabaseConnector(filename='postgres_link.yaml', dataframe=clean_stores_df, table_name='dim_store_details')
    postgres_conn_stores.upload_to_db()

def clean_products() -> None:
    # Extract data
    urls = read_yaml_data('links.yaml')
    product_data_extractor = DataExtractor()
    products_df = product_data_extractor.extract_from_s3(urls['s3_products_url']) 
    
    # Convert all weights to kg (1 decimal) and clean data
    product_data_cleaner = DataCleaning(products_df)
    products_df = product_data_cleaner.convert_product_weights()  
    clean_products_df = product_data_cleaner.clean_products_data(products_df) 
    
    # Upload of dataframe
    postgres_conn_products = DatabaseConnector(filename='postgres_link.yaml', dataframe=clean_products_df, table_name='dim_products')
    postgres_conn_products.upload_to_db()

def clean_orders() -> None:
    # Read the credentials
    rds_connector = DatabaseConnector(filename='db_creds.yaml')
    engine = rds_connector.init_db_engine()

    # Extract RDS table to dataframe
    rds_extractor = DataExtractor(engine=engine, table_name='orders_table')
    orders_df = rds_extractor.read_rds_table()

    # Perform the cleaning of the orders data
    orders_data_cleaner = DataCleaning(orders_df)
    orders_df = orders_data_cleaner.clean_orders_data()

    # Upload of dataframe
    postgres_conn_products = DatabaseConnector(filename='postgres_link.yaml', dataframe=orders_df, table_name='orders_table')
    postgres_conn_products.upload_to_db()

def clean_date_events() -> None:
    # Download JSON
    data = read_yaml_data('user_cred.yaml')
    key = data['key']
    secret = data['secret']
    urls = read_yaml_data('links.yaml')
    json_url = urls['date_events_url']
    sales_df = pd.read_json(json_url, storage_options={'key':key, 'secret':secret})

    # Clean NULL and gibberish by length of month values
    sales_df = sales_df[sales_df['month'].str.len()<=2] # Values longer than 2 characters will be deleted from dataframe
    # Upload dataframe to database
    postgres_conn_events = DatabaseConnector(filename='postgres_link.yaml', dataframe=sales_df, table_name='dim_date_times')
    postgres_conn_events.upload_to_db()



if __name__ == '__main__':
    # clean_user()
    # clean_card()
    # clean_stores()
    # clean_products()
    # clean_orders()
    # clean_date_events()
    pass
