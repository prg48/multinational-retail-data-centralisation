from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

class AlterDatabase:
    """
    This class is to alter the sales_data database tables according to predefined alterations.
    Further, this method also adds foreign keys to the fact table, orders_table.
    This class should only be run after the database creation and poopulation with DatabaseConnector, DatabaseExtractor and DatabaseCleaning class.
    """
    def __init__(self):
        self.local_db_creds = DatabaseConnector.read_db_creds("config/local_db_creds.yaml")
        self.engine = create_engine(f"{self.local_db_creds['DATABASE_TYPE']}+{self.local_db_creds['DB_API']}://{self.local_db_creds['USER']}:{quote_plus(self.local_db_creds['PASSWORD'])}@{self.local_db_creds['HOST']}:{self.local_db_creds['PORT']}/{self.local_db_creds['DATABASE']}") 

    def alter_orders_table(self):
        """
        This method is to alter the orders_table table in the sales_data database.
        This method alters the following column to its corresponding postgresSQL types.

        Columns:
            date_uuid: UUID 
            user_uuid: UUID
            card_number: VARCHAR(?)
            store_code: VARCHAR(?)
            product_code: VARCHAR(?)
            product_quantity: SMALLINT

            ? is integer representing maximum length of the values in that column
        """
        # querying max_length for card_number, store_code and product_code
        with self.engine.connect() as connection:
            results = connection.execute(text(f"""
                                    select max(char_length(card_number::text)),
                                    max(char_length(store_code)),
                                    max(char_length(product_code))
                                        from orders_table
                                    """))
            card_num_max_length, store_code_max_length, product_code_max_length = results.fetchone()

        # altering the table
        try:
            with self.engine.begin() as connection:
                connection.execute(text(f"""
                            ALTER TABLE orders_table
                            ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
                            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
                            ALTER COLUMN card_number TYPE VARCHAR({card_num_max_length}),
                            ALTER COLUMN store_code TYPE VARCHAR({store_code_max_length}),
                            ALTER COLUMN product_code TYPE VARCHAR({product_code_max_length}),
                            ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::smallint
                            """))
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            print(error)
            
    def alter_users_table(self):
        """
        This method is to alter the dim_users_table table in the sales_data database.
        This method alters the following column to its corresponding postgresSQL types.

        Columns:
            first_name: VARCHAR(225) 
            last_name: VARCHAR(225)
            date_of_birth: DATE
            country_code: VARCHAR(?)
            user_uuid: UUID
            join_date: DATE

            ? is integer representing maximum length of the values in that column
        """
        # get country code max length
        with self.engine.connect() as conn:
            results = conn.execute(text(f"""
                                        select max(char_length(country_code))
                                        from dim_users
                                        """))
            country_code_max_length = results.fetchone()[0]

        # alter table
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                                ALTER TABLE dim_users
                                ALTER COLUMN first_name TYPE VARCHAR(255),
                                ALTER COLUMN last_name TYPE VARCHAR(255),
                                ALTER COLUMN date_of_birth TYPE DATE,
                                ALTER COLUMN country_code TYPE VARCHAR({country_code_max_length}),
                                ALTER COLUMN user_uuid TYPE UUID using user_uuid::uuid,
                                ALTER COLUMN join_date TYPE DATE
                                """))
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            print(error)

    def alter_stores_table(self):
        """
        This method is to alter the dim_stores_details table in the sales_data database.
        This method alters the following column to its corresponding postgresSQL types.

        Columns:
            longitude: FLOAT
            locality: VARCHAR(255)
            store_code: VARCHAR(?)
            staff_numbers: SMALLINT
            opening_date: DATE
            store_type: VARCHAR(255) NULLABLE
            latitude: FLOAT
            country_code: VARCHAR(?)
            continent: VARCHAR(255)

            ? is integer representing maximum length of the values in that column
        """
        # get length for store_code and country_code
        with self.engine.connect() as conn:
            results = conn.execute(text(f"""
                                        select max(char_length(store_code)),
                                        max(char_length(country_code))
                                        from dim_store_details
                                        """))
            store_code_max_length, country_code_max_length = results.fetchone()

        # alter table
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                                ALTER TABLE dim_store_details
                                ALTER COLUMN longitude TYPE FLOAT,
                                ALTER COLUMN locality TYPE VARCHAR(255),
                                ALTER COLUMN store_code TYPE VARCHAR({store_code_max_length}),
                                ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
                                ALTER COLUMN opening_date TYPE DATE,
                                ALTER COLUMN store_type TYPE VARCHAR(255),
                                ALTER COLUMN store_type DROP NOT NULL,
                                ALTER COLUMN latitude TYPE FLOAT,
                                ALTER COLUMN country_code TYPE VARCHAR({country_code_max_length}),
                                ALTER COLUMN continent TYPE VARCHAR(255) 
                                """))
        except SQLAlchemyError as e:
            print(e)

    def alter_products_table(self):
        """
        This method is to alter the dim_products table in the sales_data database.
        This method alters the following column to its corresponding postgresSQL types.

        Columns:
            product_price: FLOAT
            weight: FLOAT
            EAN: VARCHAR(?)
            product_code: VARCHAR(?)
            date_added: DATE
            uuid: UUID
            still_available: BOOL
            weight_class: VARCHAR(?)

            ? is integer representing maximum length of the values in that column
        """
        # read the product table using pandas
        product_df = pd.read_sql_table('dim_products', self.engine)

        # add a new weight_class column
        def custom_weight_class_parser(weight: str) -> str:
            """
            custome parser that returns weight class according to the weight values

            Args:
                weight (str): str value for weight

            Returns:
                str: the weight class according to the weight value
            """
            if weight < 2:
                return "Light"
            elif weight >= 2 and weight < 40:
                return "Mid_Sized"
            elif weight >= 40 and weight < 140:
                return "Heavy"
            else:
                return "Truck_Required"
            
        product_df['weight_class'] = product_df['weight(in kg)'].apply(custom_weight_class_parser)

        # change the removed column to still_available and add boolean values to it
        product_df['still_available'] = product_df['removed'].apply(lambda x: True if x == 'Still_avaliable' else False)

        # remove the removed column
        product_df.drop(columns=['removed'], inplace=True)

        # rename the product_price(in £s) and weight(in kg) to product_price and weight
        product_df['product_price'] = product_df['product_price(in £s)']
        product_df['weight'] = product_df['weight(in kg)']
        product_df.drop(columns=['product_price(in £s)', 'weight(in kg)'], inplace=True)

        # save to local database
        DatabaseConnector.upload_to_db(product_df, 'dim_products')

        # get max lengths for EAN, product_code and weight_class
        with self.engine.connect() as conn:
            results = conn.execute(text("""
                select max(char_length("EAN"::text)),
                max(char_length(product_code)),
                max(char_length(weight_class))
                from dim_products
            """))
            EAN_max_length, product_code_max_length, weight_class_max_length = results.fetchone()

        # alter tables
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                                    ALTER TABLE dim_products
                                    ALTER COLUMN product_price TYPE FLOAT,
                                    ALTER COLUMN weight TYPE FLOAT,
                                    ALTER COLUMN "EAN" TYPE VARCHAR({EAN_max_length}),
                                    ALTER COLUMN product_code TYPE VARCHAR({product_code_max_length}),
                                    ALTER COLUMN date_added TYPE DATE,
                                    ALTER COLUMN uuid TYPE UUID USING uuid::uuid, 
                                    ALTER COLUMN still_available TYPE BOOLEAN USING
                                    CASE
                                    WHEN still_available='True' THEN TRUE
                                    WHEN still_available='False' THEN FALSE
                                    END,
                                    ALTER COLUMN weight_class TYPE VARCHAR({weight_class_max_length})
                                    """))
        except SQLAlchemyError as e:
            print(e)

    def alter_date_table(self):
        """
        This method is to alter the dim_date_times table in the sales_data database.
        This method alters the following column to its corresponding postgresSQL types.

        Columns:
            month: VARCHAR(?)
            year: VARCHAR(?)
            day: VARCHAR(?)
            time_period: VARCHAR(?)
            date_uuid: UUID

            ? is integer representing maximum length of the values in that column
        """
        # get max lengths
        with self.engine.connect() as conn:
            results = conn.execute(text("""
                select max(char_length(month)),
                max(char_length(year)),
                max(char_length(day)),
                max(char_length(time_period))
                from dim_date_times
            """))
            month_max_length, year_max_length, day_max_length, time_period_max_length = results.fetchone()

        # alter table
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                                ALTER TABLE dim_date_times
                                ALTER COLUMN month TYPE VARCHAR({month_max_length}),
                                ALTER COLUMN year TYPE VARCHAR({year_max_length}),
                                ALTER COLUMN day TYPE VARCHAR({day_max_length}),
                                ALTER COLUMN time_period TYPE VARCHAR ({time_period_max_length}),
                                ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
                                ALTER COLUMN datetime TYPE DATE
                                """))
        except SQLAlchemyError as e:
            print(e)

    def alter_card_table(self):
        """
        This method is to alter the dim_card_details table in the sales_data database.
        This method alters the following column to its corresponding postgresSQL types.

        Columns:
            card_number: VARCHAR(?)
            expiry_date: VARCHAR(?)
            date_payment_confirmed: DATE

            ? is integer representing maximum length of the values in that column
        """
        # get max_lengths
        with self.engine.connect() as conn:
            results = conn.execute(text("""
                select max(char_length(card_number)),
                max(char_length(expiry_date::text))
                from dim_card_details
            """))
            card_num_max_length, expiry_date_max_length = results.fetchone()

        # alter tables
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                                ALTER TABLE dim_card_details
                                ALTER COLUMN card_number TYPE VARCHAR({card_num_max_length}),
                                ALTER COLUMN expiry_date TYPE VARCHAR({expiry_date_max_length}),
                                ALTER COLUMN date_payment_confirmed TYPE DATE
                                """))
        except SQLAlchemyError as e:
            print(e)

    def add_foreign_key(self, table_name: str, fk_table_name: str,  constraint_name: str, column_name: str):
        """
        This method assigns foreign key to a table column with a key from another table

        Args:
            table_name (str): the table to assign foreign key to
            fk_table_name (str): the table from which foreign key is assigned
            constraint_name (str): foreign key constraint name
            column_name (str): the column name which is assigned as a foreign key and to which foreign key is assigned. the column name needs to be the same in table_name and fk_table_name.
        """
        with self.engine.begin() as conn:
            conn.execute(text(f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {constraint_name}
                FOREIGN KEY ({column_name})
                REFERENCES {fk_table_name}({column_name})
            """))

    def add_foreign_keys(self):
        """
        This method applies foreign key to the columns of orders_table with the primary key from the other tables starting with 'dim'
        """
        self.add_foreign_key('orders_table','dim_card_details', 'fk_orders_table_dim_card_details', 'card_number')
        self.add_foreign_key('orders_table','dim_date_times', 'fk_orders_table_dim_date_times', 'date_uuid')
        self.add_foreign_key('orders_table','dim_products', 'fk_orders_table_dim_products', 'product_code')
        self.add_foreign_key('orders_table','dim_store_details', 'fk_orders_table_dim_store_details', 'store_code')
        self.add_foreign_key('orders_table','dim_users', 'fk_orders_table_dim_users', 'user_uuid')

    def alter_all(self):
        """
        This method alters all the tables according to the hardcoded criteria
        """
        self.alter_orders_table()
        self.alter_date_table()
        self.alter_card_table()
        self.alter_products_table()
        self.alter_stores_table()
        self.alter_users_table()