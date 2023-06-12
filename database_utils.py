#%%
import yaml

class DatabaseConnector:
    # This class will be used to connect with and upload data to the database.

    @classmethod
    def read_db_creds(cls, yaml_filepath: str) -> dict:
        """
        This method reads the credentials in a yaml file and return a dictionary of the credentials.
        Args:
            yaml_filepath (str): file path to the yaml file
        
        Returns:
            dict: key value pair from the yaml file
        """
        try:
            with open(yaml_filepath, "r") as file:
                data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print(f"File configuration error: {e}")



#%%
yaml_config = DatabaseConnector.read_db_creds("db_creds.yaml")
type(yaml_config)
# %%
