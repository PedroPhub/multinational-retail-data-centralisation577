import re
import pandas as pd
from dateutil.parser import parse


class DataCleaning: 
    def  __init__(self, dataframe: pd.DataFrame = None) -> None:
        """
        This class provides functionality for cleaning Pandas DataFrames.

        Keyword arguments:
            'dataframe': pd.DataFrame -- DataFrame to be cleaned;
        """
        self.dataframe = dataframe
        
    # Auxuliary function to convert a column to type datetime
    def convert_to_datetime(self, dataframe: pd.DataFrame, *columns: str) -> pd.DataFrame:
        """
        Function receives a dataframe and a column(s) name(s) to convert to type datetime.

        Keyword arguments:
            'dataframe': pd.DataFrame -- DataFrame to be modified;
            '*columns': *str -- Column(s) to convert inside the DataFrame;
            
        Returns:
            'dataframe': pd.DataFrame -- Dataframe with columns in timedate format;
        """
        for column in columns:
            dataframe[column] = dataframe[column].apply(parse)
            dataframe[column] = pd.to_datetime(dataframe[column], errors='coerce')
        return dataframe

    # Clean Nulls, correct date values, incorrectly typed values and rows filled with the wrong information
    def clean_user_data(self) -> pd.DataFrame:
        """
        Retrieves user data, cleans the data and upload to database with table name 'dim_users'
        
        Returns:
            'users_df': pd.DataFrame -- Clean users dataframe;
        """      
        # Clean NULL and gibberish by length of user_uuid (fixed 36 char code)
        users_df = self.dataframe[self.dataframe['user_uuid'].str.len()==36]

        # Correct date_of_birth and join_date format
        users_df = self.convert_to_datetime(users_df, 'date_of_birth','join_date')

        # Clean phone numbers
        # Fix mispelling on country code
        users_df.loc[users_df['country_code']=='GGB', 'country_code'] = 'GB'
        # Clean plus sign, characters and spaces
        users_df['phone_number'] = users_df['phone_number'].replace({r'\+': '', r'\(': '', r'\)': '', r'-': '', r' ': '',r'\.': ''}, regex=True)
        # Clean office extension on US numbers
        users_df['phone_number'] = users_df['phone_number'].str.split('x').str[0]
        # Clean international code from numbers
        for index in users_df.index:
            # DE, US have 10 digits. GB have 11 digits (1st digit is always 0)
            # Cleaning country code from the phone number, when present
            users_df.at[index, 'phone_number'] = users_df['phone_number'][index][-10:]
            # Adding 0 to the start of GB numbers
            GB_number = False
            GB_number = users_df['country_code'][index]=='GB'
            if GB_number is True:
                users_df.loc[users_df['phone_number']==users_df['phone_number'][index][-10:], 'phone_number'] = '0' + users_df['phone_number'][index]
                
        # Clean email mispelling
        users_df["email_address"] = users_df["email_address"].str.replace('@@','@')

        return users_df


    def clean_card_data(self) -> pd.DataFrame:
        """
        Retrieves card data, cleans the data and upload to database with table name 'dim_card_details'
        
        Returns:
            'cards_df': pd.DataFrame -- Clean cards dataframe;
        """
        cards_df = pd.concat(self.dataframe)

        # Clean NULL values
        cards_df = cards_df.loc[cards_df['card_number'] != 'NULL']

        # Clean gibberish by length of expiry_date
        cards_df = cards_df[cards_df['expiry_date'].str.len()==5]
        
        # Correct date_payment_confirmed and card_number format
        cards_df = self.convert_to_datetime(cards_df, 'date_payment_confirmed')

        # Clean '?' from card numbers
        cards_df['card_number'] = cards_df['card_number'].astype(str)
        cards_df['card_number'] = cards_df['card_number'].str.replace('?','')

        return cards_df


    def clean_store_data(self) -> pd.DataFrame:
        """
        Retrieves store data, cleans the data and upload to database with table name 'dim_store_details'
        
        Returns:
            'store_data_df': pd.DataFrame -- Clean stores dataframe;
        """
        store_data_df = self.dataframe

        # Clean continent mispelling
        store_data_df["continent"] = store_data_df["continent"].str.replace('ee','')

        #Clean gibberish values.
        for index in store_data_df.index:
            Europe_continent = False
            America_continent = False
            Europe_continent = store_data_df['continent'][index]=='Europe'
            America_continent = store_data_df['continent'][index]=='America'
            if Europe_continent is False and America_continent is False:
                store_data_df = store_data_df.drop([index])

        # Correct opening_date format
        store_data_df = self.convert_to_datetime(store_data_df, 'opening_date')

        # Correct staff_numbers numbers
        store_data_df['staff_numbers'].replace('\D+', '', regex=True, inplace=True)

        return store_data_df

    def convert_product_weights(self) -> pd.DataFrame:
        """
        Returns converted dataframe with column 'weight' in kg, with one decimal
       
        Returns:
            'products_df': pd.DataFrame -- Products dataframe with weight units in kg;
        """
        products_df = self.dataframe
        products_df["weight"] = products_df["weight"].str.replace('1160kg','1160g')
        
        for index in products_df.index:
            if type(products_df['weight'][index]) == str:  # Skip empty cells

                # Operations when value in kilograms
                if products_df['weight'][index].find('kg')>0:  # String in kg

                    # When multiplication is required
                    if products_df['weight'][index].find('x')>0: # Returns >0 when 'x' is found
                        numbers = re.findall(r'-?\d+\.?\d*', products_df['weight'][index])  # Extracts the numbers from the match
                        total_weight = float((numbers[0]*numbers[1])) # Convert to float
                        products_df.loc[products_df['weight'] == products_df['weight'][index],'weight'] = round(total_weight, 1) # Replace the string with the float
                    
                    # When multiplication is not required
                    else:
                        number = re.findall(r'-?\d+\.?\d*', products_df['weight'][index])  # Extracts the numbers from the match   
                        total_weight = float(number[0]) # Convert to float
                        products_df.loc[products_df['weight'] == products_df['weight'][index],'weight'] = round(total_weight, 1) # Replace the string with the float

                # Operations when value in g or ml        
                elif (products_df['weight'][index].find('g')>0 or 
                      products_df['weight'][index].find('ml')>0): # Returns >0 when 'g' or 'ml' is found
                    
                    # When multiplication is required
                    if products_df['weight'][index].find('x')>0:   # Returns >0 when 'x' is found
                        numbers = re.findall(r'-?\d+\.?\d*', products_df['weight'][index])  # Extracts the numbers from the string
                        total_weight = (float(numbers[0])*float(numbers[1])/1000)   # Convert to float in kg
                        products_df.loc[products_df['weight'] == products_df['weight'][index],'weight'] = round(total_weight, 1) # Replace the string with the float
                    
                    # When multiplication is not required   
                    else:
                        number = re.findall(r'-?\d+\.?\d*', products_df['weight'][index]) # Extracts the number from the string
                        total_weight = (float(number[0])/1000) # Convert to float
                        products_df.loc[products_df['weight'] == products_df['weight'][index],'weight'] = round(total_weight, 1) # Replace the string with the float

                # Operations when value in oz
                elif products_df['weight'][index].find('oz')>0: # Returns >0 when 'oz' is found
                    numbers = re.findall(r'-?\d+\.?\d*', products_df['weight'][index])  # Extracts the number from the string
                    total_weight = (float(numbers[0])/35.274)   # Convert to float
                    products_df.loc[products_df['weight'] == products_df['weight'][index],'weight'] = round(total_weight, 1) # Replace the string with the float

        products_df = products_df[pd.to_numeric(products_df['weight'], errors='coerce').notnull()] # Eliminate NULL and gibberish values
        products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce') # Convert column to float64

        return products_df
    
    def clean_products_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean products dataframe (with 'weight' column in kg, 1 decimal) and returns dataframe
        
        Keyword arguments:
            'dataframe': pd.DataFrame -- Products dataframe to clean, with weight converted in kg;
        
        Returns:
            'products_kg_df': pd.DataFrame -- Clean products dataframe;
        """
        # Auxiliar function to add 0's at the start of the EAN when the length is <13
        def pad_string(string):
            return string.zfill(13)
        
        products_kg_df = dataframe

        # Correct EAN format (13 digits)
        products_kg_df['EAN'] = products_kg_df['EAN'].apply(pad_string) # Add  0's at the start when length is <13 
        products_kg_df['EAN'] = products_kg_df['EAN'].str[-13:] # Use only the last 13 characters of the string

        # Correct date_added format
        products_kg_df = self.convert_to_datetime(products_kg_df, 'date_added')

        return products_kg_df
    

    def clean_orders_data(self) -> pd.DataFrame:
        """
        Returns orders dataframe with columns 'first_name', 'last_name' and '1' removed
        
        Returns:
            'orders_table_df': pd.DataFrame -- Clean orders dataframe;
        """
        orders_table_df = self.dataframe
        orders_table_df = orders_table_df.drop(['first_name', 'last_name', '1'], axis=1)

        return orders_table_df