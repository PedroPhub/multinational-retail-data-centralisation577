import re

import pandas as pd
from dateutil.parser import parse

from .data_extraction import DataExtractor
from .database_utils import DatabaseConnector


class DataCleaning:

    # Clean Nulls, correct date values, incorrectly typed values and rows filled with the wrong information
    def clean_user_data(self):
        # Get engine
        db_obj = DatabaseConnector()
        engine = db_obj.init_db_engine()
        
        # Get users table to clean
        extract_obj = DataExtractor()
        users_df = extract_obj.read_rds_table(engine, 'legacy_users')

        # Clean NULL and gibberish by length of user_uuid (fixed 36 char code)
        users_df = users_df[users_df['user_uuid'].str.len()==36]

        # Correct date_of_birth format
        users_df['date_of_birth'] = users_df['date_of_birth'].apply(parse)
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], errors='coerce')

        # Correct join_date format
        users_df['join_date'] = users_df['join_date'].apply(parse)
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], errors='coerce')

        # Clean phone numbers
        # Fix mispelling on country code
        users_df.loc[users_df['country_code']=='GGB', 'country_code'] = 'GB'
        # Clean plus sign, characters and spaces
        users_df['phone_number'] = users_df['phone_number'].replace({r'\+': '', r'\(': '', r'\)': '', r'-': '', r' ': '',r'\.': ''}, regex=True)
        # Clean office extension on US numbers
        users_df['phone_number'] = users_df['phone_number'].str.split('x').str[0]
        # Clean international code from numbers
        for i in users_df.index:
            # DE, US have 10 digits. GB have 11 digits (1st digit is always 0)
            # Cleaning country code from the phone number, when present
            users_df.at[i, 'phone_number'] = users_df['phone_number'][i][-10:]
            # Adding 0 to the start of GB numbers
            GB_number = False
            GB_number = users_df['country_code'][i]=='GB'
            if GB_number is True:
                users_df.loc[users_df['phone_number']==users_df['phone_number'][i][-10:], 'phone_number'] = '0' + users_df['phone_number'][i]
                
        # Clean email mispelling
        users_df["email_address"] = users_df["email_address"].str.replace('@@','@')

        # Run upload of dataframe
        obj = DatabaseConnector()
        obj.upload_to_db(users_df,'dim_users')


    def clean_card_data(self):
        obj = DataExtractor()
        cards_list_of_df = obj.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        cards_df = pd.concat(cards_list_of_df)

        # Clean NULL values
        cards_df = cards_df.loc[cards_df['card_number'] != 'NULL']

        # Clean gibberish by length of expiry_date
        cards_df = cards_df[cards_df['expiry_date'].str.len()==5]
        
        # Correct date_of_birth format
        cards_df['date_payment_confirmed'] = cards_df['date_payment_confirmed'].apply(parse)
        cards_df['date_payment_confirmed'] = pd.to_datetime(cards_df['date_payment_confirmed'], errors='coerce')

        # Clean '?' from card numbers
        cards_df["card_number"] = cards_df["card_number"].str.replace('?','')

        # Send to database
        obj = DatabaseConnector()
        obj.upload_to_db(cards_df,'dim_card_details')


    def clean_store_data(self):
        store_header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        obj = DataExtractor()
        number_of_stores = obj.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', store_header)
        store_data_df = pd.DataFrame()
        for store in range(number_of_stores['number_stores']):
            store_data = obj.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}'.format(store), store_header)
            store_data_df = pd.concat([store_data_df, pd.DataFrame([store_data])],ignore_index=True)

        # Clean continent mispelling
        store_data_df["continent"] = store_data_df["continent"].str.replace('ee','')

        #Clean gibberish values.
        for i in store_data_df.index:
            Europe_continent = False
            America_continent = False
            Europe_continent = store_data_df['continent'][i]=='Europe'
            America_continent = store_data_df['continent'][i]=='America'
            if Europe_continent is False and America_continent is False:
                store_data_df = store_data_df.drop([i])

        # Correct opening_date format
        store_data_df['opening_date'] = store_data_df['opening_date'].apply(parse)
        store_data_df['opening_date'] = pd.to_datetime(store_data_df['opening_date'], errors='coerce')

        # Correct staff_numbers numbers
        store_data_df['staff_numbers'].replace('\D+', '', regex=True, inplace=True)

        obj = DatabaseConnector()
        obj.upload_to_db(store_data_df,'dim_store_details')


    def convert_product_weights (self, products_df):
        self.products_df = products_df
        self.products_df["weight"] = self.products_df["weight"].str.replace('1160kg','1160g')
        
        # Cycle through the products_df
        for i in self.products_df.index:
            if type(self.products_df['weight'][i]) == str:  # Skip empty cells

                # Operations when value in kilograms
                if self.products_df['weight'][i].find('kg')>0:  # String in kg

                    # When multiplication is required
                    if self.products_df['weight'][i].find('x')>0: # Returns >0 when 'x' is found
                        numbers = re.findall(r'-?\d+\.?\d*', self.products_df['weight'][i])  # Extracts the numbers from the match
                        total_weight = float((numbers[0]*numbers[1])) # Convert to float
                        self.products_df.loc[self.products_df['weight'] == self.products_df['weight'][i],'weight'] = round(total_weight, 1) # Replace the string with the float
                    
                    # When multiplication is not required
                    else:
                        number = re.findall(r'-?\d+\.?\d*', self.products_df['weight'][i])  # Extracts the numbers from the match   
                        total_weight = float(number[0]) # Convert to float
                        self.products_df.loc[self.products_df['weight'] == self.products_df['weight'][i],'weight'] = round(total_weight, 1) # Replace the string with the float

                # Operations when value in g or ml        
                elif (self.products_df['weight'][i].find('g')>0 or 
                      self.products_df['weight'][i].find('ml')>0): # Returns >0 when 'g' or 'ml' is found
                    
                    # When multiplication is required
                    if self.products_df['weight'][i].find('x')>0:   # Returns >0 when 'x' is found
                        numbers = re.findall(r'-?\d+\.?\d*', self.products_df['weight'][i])  # Extracts the numbers from the string
                        total_weight = (float(numbers[0])*float(numbers[1])/1000)   # Convert to float in kg
                        self.products_df.loc[self.products_df['weight'] == self.products_df['weight'][i],'weight'] = round(total_weight, 1) # Replace the string with the float
                    
                    # When multiplication is not required   
                    else:
                        number = re.findall(r'-?\d+\.?\d*', self.products_df['weight'][i]) # Extracts the number from the string
                        total_weight = (float(number[0])/1000) # Convert to float
                        self.products_df.loc[self.products_df['weight'] == self.products_df['weight'][i],'weight'] = round(total_weight, 1) # Replace the string with the float

                # Operations when value in oz
                elif self.products_df['weight'][i].find('oz')>0: # Returns >0 when 'oz' is found
                    numbers = re.findall(r'-?\d+\.?\d*', self.products_df['weight'][i])  # Extracts the number from the string
                    total_weight = (float(numbers[0])/35.274)   # Convert to float
                    self.products_df.loc[self.products_df['weight'] == self.products_df['weight'][i],'weight'] = round(total_weight, 1) # Replace the string with the float

        self.products_df = self.products_df[pd.to_numeric(self.products_df['weight'], errors='coerce').notnull()] # Eliminate NULL and gibberish values
        self.products_df['weight'] = pd.to_numeric(self.products_df['weight'], errors='coerce') # Convert column to float64

        return self.products_df
    
    def clean_products_data(self, products_kg_df):
        # Auxiliar function to add 0's at the start of the EAN when the length is <13
        def pad_string(string):
            return string.zfill(13)
        
        self.products_kg_df = products_kg_df

        # Correct EAN format (13 digits)
        products_kg_df['EAN'] = products_kg_df['EAN'].apply(pad_string) # Add  0's at the start when length is <13 
        products_kg_df['EAN'] = products_kg_df['EAN'].str[-13:] # Use only the last 13 characters of the string

        # Correct date_added format
        products_kg_df['date_added'] = products_kg_df['date_added'].apply(parse)
        products_kg_df['date_added'] = pd.to_datetime(products_kg_df['date_added'], errors='coerce')

        return products_kg_df
    

    def clean_orders_data(self):
        data_conn_obj = DatabaseConnector()
        data_extr_obj = DataExtractor()
        engine = data_conn_obj.init_db_engine( )
        orders_table_df = data_extr_obj.read_rds_table(engine, 'orders_table')
        orders_table_df = orders_table_df.drop(['first_name', 'last_name', '1'], axis=1)

        return orders_table_df