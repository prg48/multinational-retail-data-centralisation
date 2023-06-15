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
    
    @staticmethod
    def list_number_of_stores(endpoint: str, headers: dict) -> int:
        """
        This method takes an API endpoint and header and returns the number of stores available to get from the API endpoint.

        Args:
            endpoint (str): API endpoint
            headers (dict): key-value pair dictionary for headers

        Returns:
            int: the number of stores available to get for from the API endpoint
        """
        response = requests.get(endpoint, headers=headers)
        return int(response.json()['number_stores'])

    @staticmethod
    def retrieve_stores_data(endpoint: str, num_of_stores: int, headers: dict) -> pd.DataFrame:
        """
        This method takes an API endpoint and header and retrieves all the stores' data specified in the argument.

        Args:
            endpoint (str): API endpoint for store
            num_of_stores (int): number of stores' data to create pandas dataframe for
            headers (dict): key value pair dictionary for headers

        Returns:
            pd.DataFrame: pandas dataframe of all the specified stores' data

        Raises:
            KeyError: raises a KeyError if the keys for the data of stores are not consistent.
        """

        # retrieve the first store data to create a dictionary with relevant keys
        response = requests.get(endpoint + '/' + '0', headers=headers)
        store_keys = response.json().keys()
        all_store_dict = {}
        for key in store_keys:
            all_store_dict[key] = []

        # for each store available to query, retrieve the data and append the corresponding value of the dict to all_store_dict
        # for all stores retrieve the data
        for idx in range(0, num_of_stores):
            response = requests.get(endpoint + "/" + str(idx), headers=headers)
            response_json = response.json()

            # check if all the keys match to our all stores' dictionary
            if len(response_json.keys()) != len(all_store_dict.keys()):
                raise KeyError("The keys of store are not consistent.")
            
            # if keys length matches, append each value of the dictionary to our skeleton dictionary
            for key in all_store_dict.keys():
                all_store_dict[key].append(response_json[key])
        
        # create a dataframe of all stores' dictionary and return it
        return pd.DataFrame(all_store_dict)
    
    @staticmethod
    def extract_from_s3():
        pass
        
