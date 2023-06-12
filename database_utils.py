#%%
import yaml
from sqlalchemy import create_engine, Engine, inspect

class DatabaseConnector:
    """
    This class connects with and uploads data to the database.

    Attributes:
        db_creds (dict): dictionary holding the database credentials
        engine (SQLAlchemyEngine): a SQLAlchemy engine connected to database specified in db_creds
    """
    def __init__(self, db_creds) -> None:
        """
        Args:
            db_creds (str): filepath to database credentials
        """
        self.db_creds = self.read_db_creds(db_creds)
        self.engine = self.init_db_engine()

    def read_db_creds(self, yaml_filepath: str) -> dict[str, str]:
        """
        This method reads the credentials in a yaml file and return a dictionary of the credentials.
        Args:
            yaml_filepath (str): file path to the yaml file
        
        Returns:
            dict (dict[str, str]): key value pair from the yaml file
        """
        try:
            with open(yaml_filepath, "r") as file:
                data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print(f"File configuration error: {e}")

    def init_db_engine(self) -> Engine:
        """
        This method reads the database credentials attribute of the class and returns a SQLAlchemy database engine

        Returns:
            Engine (SQLAlchemyEngine): A SQLAlchemy engine initialised with the db_creds attribute of the class
        """
        connection_str = f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        return create_engine(connection_str)
    
    def list_db_tables(self) -> list[str]:
        """
        This method returns the list of table names the class engine is connected to.

        Returns:
            table_names (list[str]): list of table names the class engine is connected to
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names()




#%%
connector = DatabaseConnector("db_creds.yaml")
type(connector.list_db_tables())
# %%
help(DatabaseConnector)
# %%
