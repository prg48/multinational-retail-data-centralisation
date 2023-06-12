#%%
import yaml

class DatabaseConnector:
    """
    This class connects with and uploads data to the database.

    Attributes:
        db_creds (dict): dictionary holding the database credentials
    """
    def __init__(self, db_creds) -> None:
        """
        Args:
            db_creds (str): filepath to database credentials
        """
        self.db_creds = self.read_db_creds(db_creds)

    def read_db_creds(self, yaml_filepath: str) -> dict:
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
yaml_config = DatabaseConnector("db_creds.yaml")
yaml_config.db_creds
# %%
help(DatabaseConnector)
# %%
