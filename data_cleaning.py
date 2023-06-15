#%%
import pandas as pd
from typing import Union

class DataCleaning:
    """
    This class is for datacleaning data from various sources
    """
    @staticmethod
    def custom_date_parser(date_str: str) -> Union[str, pd.NaT]:
            """
            This nested function is a custom parser to parse dates in card data to proper format.

            Args:
                date_str (str): a date string.

            Returns:
                Union[str, pd.NaT]: returns date in the format YYYY-MM-DD if valid else return pd.NaT to represent missing or non-standard formatted date.
            """
            # for the special date anomaly with "/"
            if "/" in date_str:
                new_date_str = date_str.replace("/", "-")
                return new_date_str
            
            # for all the normal dates
            elif "-" in date_str:
                split_date = date_str.split('-')
                if len(split_date[0]) == 4 and len(split_date[1]) == 2 and len(split_date[2]) == 2:
                    return date_str
            
            else:
                months_map = {"January":"01", 
                        "February":"02", 
                        "March":"03", 
                        "April":"04", 
                        "May":"05", 
                        "June":"06", 
                        "July":"07", 
                        "August":"08", 
                        "September":"09", 
                        "October":"10", 
                        "November":"11", 
                        "December":"12"}
                
                year = month = day = None
                split_date = date_str.split()
                for elem in split_date:
                    if elem in months_map.keys():
                        month = months_map[elem]
                    elif len(elem) == 4:
                        year = elem
                    elif len(elem) == 2:
                        day = elem

                if year and month and day:   
                    return f"{year}-{month}-{day}"

                return pd.NaT

    @staticmethod
    def clean_user_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        This method cleans the user data.
        Note: most of the columns for user data has been cleaned. however, the phone number and address has been ignored.
        Args:
            data (pd.DataFrame): user data to be cleaned

        Returns:
            df (pd.DataFrame): cleaned data
        """
        print("Cleaning User Data!!!!!!!!")

        # keep track of total dropped rows
        total_rows_dropped = 0

        # make a copy
        clean_df = data.copy()

        # drop users with duplicate uuid
        len_before_dropping = len(clean_df)
        clean_df.drop_duplicates(subset=['user_uuid'], inplace=True)
        len_after_dropping = len(clean_df)
        total_rows_dropped += (len_before_dropping - len_after_dropping)
        print(f"Dropped {len_before_dropping - len_after_dropping} rows with duplicate user uuid.")

        # drop null rows
        len_before_dropping = len(clean_df)
        clean_df.dropna(inplace=True)
        len_after_dropping = len(clean_df)
        total_rows_dropped += (len_before_dropping - len_after_dropping)
        print(f"Dropped {len_before_dropping - len_after_dropping} rows with null values.")

        # convert date_of_birth to pandas datetime and drop any rows whose date could not be converted
        len_before_dropping = len(clean_df)
        clean_df['date_of_birth'] = pd.to_datetime(clean_df['date_of_birth'], errors='coerce')
        clean_df.dropna(subset=['date_of_birth'])
        len_after_dropping = len(clean_df)
        total_rows_dropped += (len_before_dropping - len_after_dropping)
        print(f"Dropped {len_before_dropping - len_after_dropping} rows while converting date of birth.")

        # convert join_date to pandas datetime and drop any rows whose date could not be converted
        len_before_dropping = len(clean_df)
        clean_df['join_date'] = pd.to_datetime(clean_df['join_date'], errors='coerce')
        clean_df.dropna(subset=['join_date'])
        len_after_dropping = len(clean_df)
        total_rows_dropped += (len_before_dropping - len_after_dropping)
        print(f"Dropped {len_before_dropping - len_after_dropping} rows while converting join date.")

        # drop the index column
        clean_df.drop(columns=['index'], inplace=True)

        # reorder the columns, make user_uuid the first column
        cols = list(clean_df.columns)
        cols = [cols[-1]] + cols[:-1]
        clean_df = clean_df[cols]

        # remove rows with invalid email
        len_before_dropping = len(clean_df)
        clean_df = clean_df[clean_df['email_address'].str.contains('^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')]
        len_after_dropping = len(clean_df)
        total_rows_dropped += (len_before_dropping - len_after_dropping)
        print(f"Dropped {len_before_dropping - len_after_dropping} rows while checking for valid email.")

        # replace 'GGB' country code to 'GB'
        clean_df['country_code'] = clean_df['country_code'].replace('GGB', 'GB')

        return clean_df
    
    @staticmethod
    def clean_card_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        This method cleans the card data
        
        Args:
            data (pd.DataFrame): card data to be cleaned

        Returns:
            df (pd.DataFrame): cleaned card data
        """            

        print("Cleaning card data!!!!!!!")
        clean_df = data.copy()

        # drop NA values
        len_before_dropping = len(clean_df)
        clean_df.dropna(inplace=True)
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping - len_after_dropping} rows dropped for NA values")

        # drop Null rows
        len_before_dropping = len(clean_df)
        null_values = clean_df.isnull()
        clean_df = clean_df[~null_values.any(axis=1)]
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping - len_after_dropping} rows dropped for Null values")

        # drop rows with string 'NULL'
        len_before_dropping = len(clean_df)
        null_string_values = clean_df.applymap(lambda x:x == 'NULL')
        clean_df = clean_df[~null_string_values.any(axis=1)]
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping - len_after_dropping} rows dropped for NULL string values")

        # drop rows with string 'NAN'
        len_before_dropping = len(clean_df)
        nan_string_values = clean_df.applymap(lambda x:x == 'NaN')
        clean_df = clean_df[~nan_string_values.any(axis=1)]
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping - len_after_dropping} rows dropped for NaN string values")

        ######## card_number column checks
        # convert column to str before replacement
        clean_df['card_number'] = clean_df['card_number'].astype(str)

        # replace all '?' in card_number
        clean_df['card_number'] = clean_df['card_number'].str.replace('?', '')

        # drop rows with non digit values
        len_before_dropping = len(clean_df)
        non_digit_values = clean_df['card_number'].str.isdigit() == False
        clean_df = clean_df[~non_digit_values]
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping - len_after_dropping} rows dropped for non digit values in card_number column")

        ######### card_number length matching card_provider checks
        clean_df['card_length'] = clean_df['card_number'].str.len()

        # 3 anomalies are present when checked with groupby
        # 2 occurences of Discover card with card length 14
        # 1 occurence of Maestro card with card length 9
        # 1 occurence of VISA 16 digit card with card length 14
        discover_anomalies = (clean_df['card_provider'] == 'Discover') & (clean_df['card_length'] == 14)
        maestro_anomalies = (clean_df['card_provider'] == 'Maestro') & (clean_df['card_length'] == 9)
        visa_anomalies = (clean_df['card_provider'] == 'VISA 16 digit') & (clean_df['card_length'] == 14)

        discover_anomalies_index = clean_df[discover_anomalies].index
        maestro_anomalies_index = clean_df[maestro_anomalies].index
        visa_anomalies_index = clean_df[visa_anomalies].index

        clean_df = clean_df.drop(index=discover_anomalies_index)
        clean_df = clean_df.drop(index=maestro_anomalies_index)
        clean_df = clean_df.drop(index=visa_anomalies_index)

        # remove the card length column
        clean_df.drop(columns=['card_length'], inplace=True)

        ######### expiry date has some days as 32
        # replace 32 day value with 01
        clean_df['expiry_date'] = clean_df['expiry_date'].str.replace('/32', '/01')

        ######## date payment confirmed column has some dates not properly formatted
        # format the dates in order using the custom parser
        clean_df['date_payment_confirmed'] = clean_df['date_payment_confirmed'].apply(DataCleaning.custom_date_parser)

        # convert the dates to datetime format
        clean_df['date_payment_confirmed'] = pd.to_datetime(clean_df['date_payment_confirmed'], errors='coerce')

        return clean_df
    
    @staticmethod
    def clean_store_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        This method cleans the stores' data.

        Args:
            data (pd.DataFrame): stores data to be cleaned

        Returns:
            pd.DataFrame: cleaned stores' data
        """
        clean_df = data.copy()

        # drop the lat column
        clean_df.drop(columns=['lat'], inplace=True)

        # change the values in the special row for the online store
        clean_df.loc[clean_df['store_type'] == 'Web Portal', 'address'] = 'Online Address'
        clean_df.loc[clean_df['store_type'] == 'Web Portal', 'longitude'] = '0'
        clean_df.loc[clean_df['store_type'] == 'Web Portal', 'locality'] = 'Online'
        clean_df.loc[clean_df['store_type'] == 'Web Portal', 'latitude'] = '0'

        # drop the row that is none for the latitude column
        len_before_dropping = len(clean_df)
        clean_df = clean_df[~clean_df.isnull().any(axis=1)]
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping-len_after_dropping} None rows dropped for latitude column")

        # drop rows that contains gibberish data for address
        len_before_dropping = len(clean_df)
        clean_df = clean_df[clean_df['address'].str.contains(' ')]
        len_after_dropping = len(clean_df)
        print(f"{len_before_dropping-len_after_dropping} rows dropped for nonsensible address data")

        # convert longitude column to digit
        clean_df['longitude'] = pd.to_numeric(clean_df['longitude'], errors='coerce')

        # remove characters from staff_numbers column
        def custom_staff_num_parser(staff_num: str) -> str:
            """
            custom parser that removes alphabets from staff numbers and keeps only digits.
            parser is limited to scope only for clean_store_data function.

            Args:
                staff_num (str): the staff_num string 

            Returns:
                str: string from staff_num that is only digits

            """
            digit_only_lst = [char for char in staff_num if char.isdigit()]
            return "".join(digit_only_lst)
        
        clean_df['staff_numbers'] = clean_df['staff_numbers'].apply(custom_staff_num_parser)

        # convert staff_numbers column to int
        clean_df['staff_numbers'] = clean_df['staff_numbers'].astype(int)

        # parse opening_date to appropriate format
        clean_df['opening_date'] = clean_df['opening_date'].apply(DataCleaning.custom_date_parser)

        # convert opening_date to datetime
        clean_df['opening_date'] = pd.to_datetime(clean_df['opening_date'], errors='coerce')

        # convert latitude column to numeric
        clean_df['latitude'] = pd.to_numeric(clean_df['latitude'], errors='coerce')

        # remove extra 'ee' from the continent names
        def custom_continent_parser(continent: str) -> str:
            """
            custom parser that only retains continent name that starts after a uppercase char.
            parser is limited in scope only for clean_store_data function.

            Args:
                continent (str): the continent name string

            Returns:
                str: string that only start after an upper case character
            """
            is_char_upper_bool = False
            continent_char_lst = []
            for char in continent:
                if char.isupper():
                    is_char_upper_bool = True
                if is_char_upper_bool:
                    continent_char_lst.append(char)
            return "".join(continent_char_lst)
        
        clean_df['continent'] = clean_df['continent'].apply(custom_continent_parser)

        # drop the index column and reset index
        clean_df.drop(columns=['index'], inplace=True)
        clean_df.reset_index(drop=True, inplace=True)

        # rearrange the columns 
        columns = ["store_code", "store_type", "address", "longitude", "latitude",
           "locality", "country_code", "continent", "opening_date", "staff_numbers"]
        clean_df = clean_df[columns]

        return clean_df



# %%
