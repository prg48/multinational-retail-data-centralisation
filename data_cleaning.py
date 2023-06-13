#%%
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    """
    This class is for datacleaning data from various sources
    """
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

        print()
        print("Cleaning finished!!!!")
        print(f"length of data before cleaning: {len(data)}.")
        print(f"length of data after cleaning: {len(clean_df)}.")
        print(f"Total rows dropped: {total_rows_dropped}.")
        return clean_df
