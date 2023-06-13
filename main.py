#%%
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector('config/db_creds.yaml')
user_df = DataExtractor.read_rds_table(db_connector, 'legacy_users')
cleaned_user_df = DataCleaning.clean_user_data(user_df)
db_connector.upload_to_db(cleaned_user_df, 'dim_users')
# %%
