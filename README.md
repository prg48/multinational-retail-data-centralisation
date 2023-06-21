# multinational-retail-data-centralisation
## Description
The project involves the collection, cleaning, and centralisation of data from various sources into a local Postgres database. The data is collected from multiple sources including Amazon S3 buckets, Amazon RDS databases, and Amazon API endpoints. The collected data is then cleaned and processed using the Pandas library in Python.

The primary goal of the project is to create a single source of truth for data, which can be easily accessed and analyzed. This centralised data system improves the efficiency of data retrieval and analysis.

The data is stored in the following tables in the local Postgres database named `sales_data`.
* `dim_card_details`: card details.
* `dim_products`: product details.
* `dim_users`: users details.
* `dim_store_details`: store details.
* `orders_table`: orders details, fact table.
* `dim_date_times`: date and time details.

The following diagram provides an overview of the data collection, cleaning, and storage process.

![data-centralisation-overview](images/data_centralisation.svg)