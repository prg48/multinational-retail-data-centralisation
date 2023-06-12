#%%
from database_utils import DatabaseConnector

class DataExtractor:
    # This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    # The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
    def __init__(self, database_connector: DatabaseConnector) -> None:
        if isinstance(database_connector, DatabaseConnector):
            self.db_connector = database_connector
        else:
            raise TypeError("database_connector should be an instance of DatabaseConnector class")
        

# %%
db_connector = DatabaseConnector("db_creds.yaml")
d = DataExtractor(db_connector)
d.db_connector
# %%
