# Designing data storage layers
## Data uploaded via API:
/restaurants
_id - restaurant ID;
name - restaurant name.
example:
{"_id":"626a81cfefa404208fe9abae","name":"Кофейня №1"}

/couriers
_id - the courier ID;
name - the name of the courier.
Example:
{"_id":"00ozjcgn1hskwr6g6p4rrgm","name":"Петр Смирнов"}

/deliveries
order_id - order ID;
order_ts - date and time of creation of the order;
delivery_id - delivery ID;
courier_id - courier ID;
address - delivery address;
delivery_ts - date and time of delivery;
rate - delivery rating, which is put by the buyer: an integer value from 1 to 5;
sum - the sum of the order (in rubles);
tip_sum - the amount of tip left by the buyer to the courier (in rubles).
example:
{"order_id":"63cf1c0df3c879d5f84be97f","order_ts":"2023-01-23 23:45:17.793000","delivery_id":"y5ekeb5v1a2tnvptqqbmz4s",
"courier_id": "o5i2vfcwaj48qtstx2anz08", "address": "Peace Street, 4, sq. 436", "delivery_ts": "2023-01-24 01:22:24.122000", "rate":5, "sum":4589, "tip_sum":688}

## Datamart structure
id - record ID.
courier_id - ID of the courier that we are listing.
courier_name - name of the courier.
settlement_year - report year.
settlement_month - month of the report, where 1 - January and 12 - December.
orders_count - number of orders for the period (month).
orders_total_sum - total value of orders.
rate_avg - average courier rating according to the users' estimations.
order_processing_fee - the amount withheld by the company for processing the orders which is calculated as orders_total_sum * 0.25.
courier_order_sum - the amount to be transferred to the courier for the delivered orders. For each delivered order the courier should receive some amount depending on the rating (see below).
courier_tips_sum - the amount that users left to the courier as a tip.
courier_reward_sum - the amount to be transferred to the courier. It is calculated as courier_order_sum + courier_tips_sum * 0.95 (5% - commission for the payment processing).
The rules for calculating the courier payment percentage depending on the rating, where r is the average courier rating in the calculated month:
r < 4 - 5% of the order, but not less than 100 p;
4 <= r < 4.5 - 7% of the order, but not less than 150 p;
4.5 <= r <= 4.9 - 8% of the order, but not less than 175 р;
4.9 <= r - 10% of the order, but not less than 200 р.

## List of fields, necessary for creation of the shop-window
couriers/_id
couriers/name
deliveries/order_id - for linking to the table with orders
deliveries/order_ts - for calculation of settlement_year and settlement_month
deliveries/rate
deliveries/sum
deliveries/tip

## List of tables in a DDS layer with fields for the storefront
dm_orders:
    odrer_id - order id
    courier_id - courier id from the table dds.dm_couriers
    timestamp_id - the id of time record
    order_sum - order payment amount from the table stg.api_deliveries
    tip_sum - amount of tip from the table stg.api_deliveries
    rate - delivery rating from the table stg.api_deliveries
    
dm_timestamps
dm_couriers