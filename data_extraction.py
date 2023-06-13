#%%
from database_utils import DatabaseConnector
import pandas as pd
import requests
import tabula
import os

class DataExtractor:
    """
    This class is a utility class to extract data from different sources.
    """
    @staticmethod
    def read_rds_table(db_connector: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """
        This is a static method that returns a pandas dataframe for the specified table_name in db_connector database.

        Args:
            db_connector (DatabaseConnector): an instance of DatabaseConnector.
            table_name (str): the name of the table to extract as a pandas DataFrame.

        Returns:
            dataframe (pd.DataFrame): an instance of pandas DataFrame with the data specified in the table_name.

        Raises:
            ValueError: raises ValueError if the table_name is not in the database db_connector is connected to.
            TypeError: raises TypeError if db_connector is not an instance of DatabaseConnector.
        """
        if isinstance(db_connector, DatabaseConnector):
            db_tables = db_connector.list_db_tables()
            if table_name in db_tables:
                df = pd.read_sql(table_name, db_connector.engine)
                return df
            else:
                raise ValueError(f"'{table_name}' table is not present in the database.")
        else:
            raise TypeError("db_connector should be an instance of class DatabaseConnector.")
        
    @staticmethod
    def retrieve_pdf_data(link: str) -> pd.DataFrame:
        """
        This method retrieves pdf data from a link, converts it to a pandas dataframe and returns it.
        Args:
            link (str): link to the pdf data

        Returns:
            df (pd.DataFrame): dataframe of the pdf data
        """
        # pdf data file save location after download from link
        file_destination = link.split('/')[-1]
        
        # download the pdf with requests
        response = requests.get(link)

        # write the content of the response to the file in the destination
        with open(file_destination, "wb") as destination:
            destination.write(response.content)

        # read the locally saved pdf file with tabula and merge each page df to a single dataframe
        pdf_df_lst = tabula.read_pdf(file_destination, pages='all')
        
        # remove the written file
        os.remove(file_destination)

        return pd.concat(pdf_df_lst, ignore_index=True)
