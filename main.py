from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from alter_data_types import AlterDatabase

def retrieve_data_and_create_card_table():
    """
    This method retrieves card data from public amazon s3 bucket, cleans the data and create a table in local database.
    """
    # retrieve card data
    card_df = DataExtractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

    # clean card data and check whether the card_number columns are unique
    clean_card_df = DataCleaning.clean_card_data(card_df)

    # save to local df
    DatabaseConnector.upload_to_db(clean_card_df, 'dim_card_details')

def retrieve_data_and_create_product_table():
    """
    This method retrieves products data from amazon s3 bucket, cleans it and creates a table in local database.
    """
    # product data links
    s3_address = 's3://data-handling-public/products.csv'

    # extract the products data from s3
    product_df = DataExtractor.extract_from_s3(s3_address)

    # clean the products data
    clean_product_df = DataCleaning.clean_products_data(product_df)

    # save to local db
    DatabaseConnector.upload_to_db(clean_product_df, 'dim_products')

def retrieve_data_and_create_user_table():
    """
    This method retrieves user data from amazon RDS database, cleans it and creates a table in local database.
    """
    # extract the users data
    db_connector = DatabaseConnector('config/db_creds.yaml')
    user_df = DataExtractor.read_rds_table(db_connector, 'legacy_users')

    # clean the users data
    clean_user_df = DataCleaning.clean_user_data(user_df)

    # save to local db
    DatabaseConnector.upload_to_db(clean_user_df, 'dim_users')

def retrieve_data_and_create_store_table():
    """
    This method retrieves store data from amazon API endpoints, cleans it and creates a table in local database.
    """
    # data endpoints
    store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    store_num_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

    # headers
    headers = {
        "x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    # retrieve data
    no_of_stores = DataExtractor.list_number_of_stores(store_num_endpoint, headers=headers)
    store_df = DataExtractor.retrieve_stores_data(store_endpoint, no_of_stores, headers=headers)

    # clean the data
    clean_store_df = DataCleaning.clean_store_data(store_df)

    # save data to local db
    DatabaseConnector.upload_to_db(clean_store_df, 'dim_store_details')

def retrieve_data_and_create_order_table():
    """
    This method retrieves single source of truth orders data from amazon RDS database, cleans it and creates a table in local database
    """
    # initialis an instance of database connector
    db_connector = DatabaseConnector('config/db_creds.yaml')

    # retrieve data
    order_df = DataExtractor.read_rds_table(db_connector, 'orders_table')

    # clean the data
    clean_order_df = DataCleaning.clean_orders_data(order_df)

    # save data to local db
    DatabaseConnector.upload_to_db(clean_order_df, 'orders_table')

def retrieve_data_and_create_date_table():
    """
    This method retrieves date and events data from aws in json format, cleans it and creates a table in local database
    """
    # assign the link to the data
    link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

    # retireve data
    time_df = DataExtractor.retrieve_date_events_data(link)

    # clean the data
    clean_time_df = DataCleaning.clean_date_data(time_df)

    # save data to local db
    DatabaseConnector.upload_to_db(clean_time_df, 'dim_date_times')

def alter_data_and_add_foreign_keys():
    """
    This method alters the data types of tables in local database and adds relevant foreign keys to the orders_table.
    """
    # alter data types
    database_alterer = AlterDatabase()
    database_alterer.alter_all()
    database_alterer.add_foreign_keys()


if __name__ == "__main__":
    print("Tables creation starting. This might take some time......")

    print("Starting dim_card_details table creation.")
    retrieve_data_and_create_card_table()
    print("Finished dim_card_details table creation.\n")

    print("Starting dim_products table creation.")
    retrieve_data_and_create_product_table()
    print("Finished dim_products table creation.\n")

    print("Starting dim_users table creation.")
    retrieve_data_and_create_user_table()
    print("Ending dim_users table creation.\n")

    print("Starting dim_store_details table creation.")
    retrieve_data_and_create_store_table()
    print("Ending dim_store_details table creation.\n")

    print("Starting orders_table table creation.")
    retrieve_data_and_create_order_table()
    print("Ending orders_table table creation.\n")

    print("Starting dim_date_times table creation.")
    retrieve_data_and_create_date_table()
    print("Ending dim_date_times table creation.\n")

    print("Adding foreign keys to the orders_table table.")
    alter_data_and_add_foreign_keys()
    print("Finished adding foreign keys to orders_table table.")