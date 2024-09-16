## DWH for multiple sources
The goal is to improve the repository: add a new source and a datamart. At the same time, the data from the new source needs to be linked to the data that already lies in the repository.

Task 
Implement the datamart for calculations with the couriers. In it you must calculate the amounts of payment for each courier for the previous month. 
The settlement date is the 10th of each month (this will not affect your work). From the datamart the business will take the required data in the ready form.
This requires creating a multi-layered DWH and implementing ETL processes that will transform and move the data from the sources to the final data layers.

The composition of the storefront:
id - record ID.
courier_id - ID of the courier that we are enumerating.
courier_name - courier's name.
settlement_year - year of the report.
settlement_month - month of the report, where 1 - January and 12 - December.
orders_count - number of orders for the period (month).
orders_total_sum - total value of orders.
rate_avg - average courier rating according to the users' estimations.
order_processing_fee - the amount withheld by the company for processing the orders which is calculated as orders_total_sum * 0.25.
courier_order_sum - the amount to be transferred to the courier for the delivered orders. For each delivered order the courier should receive some amount depending on the rating (see below).
courier_tips_sum - the amount that users left to the courier as a tip.
courier_reward_sum - the amount to be transferred to the courier. It is calculated as courier_order_sum + courier_tips_sum * 0.95 (5% - commission for the payment processing).
The rules for calculating the courier payment percentage depending on the rating, where r is the average courier rating in the calculated month:
r < 4 - 5% of the order, but not less than 100 RUR;
4 <= r < 4.5 - 7% of the order, but not less than 150 RUR;
4.5 <= r <= 4.9 - 8% of the order, but not less than 175 RUR;
4.9 <= r - 10% of the order, but no less than 200 RUR.

Data on the orders are already in the repository. Courier service data must be taken from the API of the courier service, and then combine them with the data of the order subsystem.
The report is collected by order date.


1. Creating tables for layers in the repository
The design steps are described in the file api_overview.md 

1.1 Courier Settlement datamart
DDL-request for creating the storefront was created. Name of the storefront is cdm.dm_courier_ledger.

1.2 Structure of the DDS layer
In the detail layer you need to decompose data from the delivery subsystem on the "snowflake" model. 
DDL script for forming new DDS-layer structure is created.
1.3 Structure of STG-layer
STG-layer reflects the raw data obtained from the API of the delivery subsystem. 
The data in this layer must be the same as the data that comes from the API. 

1.4 Running DDL scripts
DDL scripts are run, data tables are created. 

2. DAG implementation
Implemented DAGs that fill all the storage layers with data.
2.1 Filling STG layer
A DAG was created that fills the tables of the staging layer with data from API.
There is a lot of data and API gives information in portions. To unload all data from API we use paged reading. 
2.2 Filling DDS-layer
A DAG is created to populate the DDS layer tables with data from the STG layer. 
The DAG fills the measurement tables and fact tables.

3.3 Filling the CDM Layer
A DAG is created to populate the CDM layer.