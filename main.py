import pandas as pd
import yaml

from data_handling.database_utils import DatabaseConnector
from data_handling.data_cleaning import DataCleaning
from data_handling.data_extraction import DataExtractor


extractor_obj = DataExtractor()
cleaning_obj = DataCleaning()
connector_obj = DatabaseConnector()

# MILESTONE 2
# Task 3, Step 2 - Read the credentials
# credentials = connector_obj.read_db_creds()

# Task 3, Step 3 - Read the credentials
# engine = connector_obj.init_db_engine() # read_db_creds() is performed inside this method

# Task 3, Step 4 - List all the tables in the database
# tables_list = connector_obj.list_db_tables()
# print(tables_list)

# Task 3, Step 5 - Extract RDS table to dataframe
# engine = connector_obj.init_db_engine()
# rds_df = extractor_obj.read_rds_table(engine, 'legacy_users')
# print(rds_df)

# Task 3, Step 6-8 - Perform the cleaning of the user data and upload user data to database
# cleaning_obj.clean_user_data() # upload_to_db method called inside this method

# Task 4, Step 2 - Extract PDF pages from document
# card_df = extractor_obj.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# print(card_df)

# Task 4, Step 3 and 4 - Clean the card data and upload card data to database
# cleaning_obj.clean_card_data() # retrieve_pdf_data and upload_to_db methods called inside this method

# Task 5, Step 1 - Return the number of stores to extract
# header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} # API Key
# store_number = extractor_obj.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header)
# print(store_number['number_stores'])

# Task 5, Step 3, 4 and 5 - Retrieve stores data using an API. Clean the data and send to database
# cleaning_obj.clean_store_data() # All methods required are called inside this method

# Task 6, Step 1-4
# object = extractor_obj.extract_from_s3('s3://data-handling-public/products.csv') # Extract data
# kg_df = cleaning_obj.convert_product_weights(object) # Convert all weights to kg (1 decimal) 
# clean_df = cleaning_obj.clean_products_data(kg_df) # Clean remaining dataframe
# connector_obj.upload_to_db(clean_df,'dim_products') # Send to database

# Task 7, Step 1
# tables_list = connector_obj.list_db_tables()
# print(tables_list)

# Task 7, Step 2,3 and 4
# orders_df = cleaning_obj.clean_orders_data() # read_db_table method is called inside this method
# connector_obj.upload_to_db(orders_df,'orders_table') # Send to database

# Task 8
# Download JSON
# with open('user_cred.yaml', 'r') as file: # Load from file for privacy
#             data = yaml.safe_load(file)
# key = data['key']
# secret = data['secret']

# sales_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json', storage_options={'key':key, 'secret':secret})

# Clean NULL and gibberish by length of month values
# sales_df = sales_df[sales_df['month'].str.len()<=2] # Values longer than 2 characters will be deleted from dataframe
# Upload dataframe to database
# connector_obj.upload_to_db(sales_df, 'dim_date_times')



