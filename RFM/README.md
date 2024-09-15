# RFM (Recency Frequency Monetary) analysis

## 1.1. Requirements for the target datamart.

1. What to do:
    Create a datamart for RFM classification of application users. 
    The storefront must be located in the same database in the analysis schema.
    The name of the storefront is `dm_rfm_segments`.

2. Why:
    Segment customers to analyze their loyalty.

3. For what period:
    The datamart needs data from the beginning of 2022.

4. access restrictions:
    There are no access restrictions.

5. Updating the vitrine:
    Updates are not required.

6. Showcase Structure:
    The storefront must consist of the following fields:
    `user_id` - user ID
    `recency` (number from 1 to 5) - how much time has passed since the last order.
    `frequency` (number from 1 to 5) - number of orders.
    `monetary_value` (number from 1 to 5) - the sum of the client's expenses.

7. Source data:
There are two schemes in the base: `production` and `analysis`. 
The `production` scheme contains operational tables:
`orderitems`.
   id
   product_id - id of product
   order_id - id of the order
   name - product name
   price - product price
   discount - discounted price
   quantity - quantity
`orders`
   order_id
   order_ts - time of order
   user_id - user ID
   bonus_payment - bonus payment
   payment - payment amount
   cost - total amount of payment
   bonus_grant - bonus crediting
   status - status
`orderstatuses`
   id
   key - order status 
      Open
      Cooking
      Delivering
      Closed
      Cancelled
`orderstatuslog`
   id
   order_id order ID
   status_id - order status identifier
   dttm - time of order, timestamp
`products`
   id
   name - product name
   price` price - cost of goods
`users`
   id
   name - user name
   login - username

## 1.2 Examine the structure of the source data.

To calculate `recency` you will need fields:
   order_id, user_id, order_ts, status from the table `orders`.

To calculate `frequency` you will need the fields:
   order_id, user_id, order_ts, status from the table `orders`.

To calculate the `monetary_value` will need fields:
   order_id, user_id, status, order_ts, payment from the table `orders`.

## 1.3. Analyze data quality

Checked the tables for duplicates and omissions in the data.
Duplicates in the data:
Performed a check for duplicates on the user_id field in the `orders` table, no duplicates were found;

Missing values in the important fields:
Missing values in the data were not detected. The fields were checked for NULL elements;

Incorrect data types:
The data types are correct;

Incorrect record formats:
Invalid record formats were not detected.

Data quality tools:
Table `orderitems`.
   Field id - Primary key
   Combination of order_id and product_id - Unique key
   Field price - Check price > 0 
   Field discount - Check discount >= 0 and discount <= price
   Field quantity - Check quantity > 0
Table `orders`.
   Field order_id - Primary key
   Field cost - Check cost == bonus_payment + payment
Table `orderstatuses`.
   Field id - Primary key
Table `orderstatuslog`.
   Field id - Primary key
   Combination of order_id and status_id - Unique key
Table `products`.
   Field id - Primary key
   Field price - Check price > 0 
Table `users`
   Field id - Primary key


## 1.4. Prepare datamart


### 1.4.1 Make a VIEW for the tables from the production database.

- views.sql file

### 1.4.2. write a DDL query to create the datamart.

- datamart_ddl.sql file

### 1.4.3. Write an SQL query to fill the storefront

- tmp_rfm.sql file
- datamart_query.sql file
- update_orders_view.sql file