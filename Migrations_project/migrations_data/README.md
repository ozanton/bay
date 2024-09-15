## DWH project work and data model revision
Given - a table with information about orders in an online store.
The task is to do a migration to separate logical tables and then build a datamart on them. This will help optimize the load on the storage and allow analysts tasked with building analysis of business efficiency and profitability to answer point questions about vendor rates, shipping costs to different countries, the number of orders delivered in the last week. If you search for this data in the raw delivery log table, the storage load will not be optimal. You would have to complicate queries, which can lead to errors.
An order in an online store is a set of purchased items and their quantity. Customers are used to receiving orders at one time, so each order from a set of products is formed into a single delivery entity.
It is important for the online store to see that the delivery time is respected, and its cost corresponds to the tariffs. He pays for shipping on his own, and the cost of delivery varies by country - this is the basic amount that the vendor takes into account. Under the contract, he makes an additional profit from the commission from the vendor. 
But now this data is stored in one table, shipping, where there is a lot of duplicated and unsystematic reference information. It basically contains the entire shipping log from the time of checkout to the time the order is released to the customer.

### Data

The shipping table, which represents the shipping sequence listed below:

id - unique identifier of the record in the table.
shipping_id - unique identifier of the delivery.
sale_id - unique identifier of the order. Several shipping_id lines can be bound to one order, i.e. logs with delivery information.
order_id - unique order identifier.
client_id - unique identifier of the client.
payment_amount - payment amount (i.e. duplicated information).
state_datetime - updating time of the order status.
product_id - unique product ID.
description - the full description of the product.
vendor_id - unique identifier of the vendor. Multiple sale_id and multiple delivery lines can be attached to one vendor.
name_category - the name of product category.
base_country - country of manufacturer.
status
The shipping status in the shipping table by the given shipping_id. The values can take the in_progress (delivery is in progress) or finished (delivery is completed.
state - the intermediate points of the order, which are changed in accordance with the updating of information about the delivery by state_datetime.
booked - ordered;
fulfillment - the order is delivered to the shipping warehouse;
queued - the order is in the queue to start delivery;
transition - order delivery has started;
pending - the order has been delivered to the pickup point and is waiting to be picked up;
recieved - the customer picked up the order;
returned - the buyer returned the order after picking it up.
shipping_plan_datetime - planned date of delivery.
hours_to_plan_shipping - the planned number of hours for shipping.
shipping_transfer_description - line with transfer_type and transfer_model values written with :. Example entry - 1p:car.
transfer_type - delivery type. 1p means that the company is responsible for shipping, 3p means that the vendor is responsible for shipping.
transfer_model - delivery model, i.e. the way the order is delivered to the point: "car" - by car, "train" - by train, "ship" - by ship, "airplane" - by plane, "multiple" - combined delivery.
shipping_transfer_rate - the percentage of shipping cost for the vendor depending on the type and model of delivery which is charged by the online store to cover the costs.
shipping_country - the country of delivery, considering the tariff description for each country.
shipping_country_base_rate - shipping tax for the country, which is a percentage of the payment_amount.
vendor_agreement_description - the line that contains agreement_id, agreement_number, agreement_rate, agreement_commission written with delimiter ":" (colon without quotes). Example entry - 12:vsp-34:0.02:0.023.
  
agreement_id - agreement identifier.
agreement_number - agreement number in accounting.
agreement_rate - tax rate for the vendor's shipping cost.
agreement_commission - commission, i.e. the share in the payment which is the company's income from the transaction.
Initial shipping log table data

'''
shipping_id,sale_id,order_id,client_id,payment,state_datetime,product_id,description,vendor_id,name_category,base_country,status,state,shipping_plan_datetime,hours_to_plan_shipping,shipping_transfer_description,shipping_transfer_rate,shipping_country,shipping_country_base_rate,vendor_agreement_description
1,1,4973,46738,6.06,2021-09-05 06:42:34.249,148,food&healh vendor_1 from norway,1,food&healh,norway,in_progress,booked,2021-09-15 16:43:42.434645,250.02,1p:train,0.025,russia,0.03,0:vspn-4092:0.14:0.02
2,2,20857,192314,21.93,2021-12-06 22:27:48.306,108,food&healh vendor_1 from germany,1,food&healh,germany,in_progress,booked,2021-12-12 10:49:50.468177,132.37,1p:train,0.025,usa,0.02,1:vspn-366:0.13:0.01
3,3,14315,132542,3.1,2021-10-26 10:33:16.659,150,food&healh vendor_1 from usa,1,food&healh,usa,in_progress,booked,2021-10-27 10:33:16.659000,24.0,1p:airplane,0.04,norway,0.04,2:vspn-4148:0.01:0.01
4,4,6682,61662,8.57,2021-09-13 16:21:32.421,174,food&healh vendor_3 from russia,3,food&healh,russia,in_progress,booked,2021-09-21 10:14:30.148733,185.87,1p:train,0.025,germany,0.01,3:vspn-3023:0.05:0.01
5,5,25922,238974,1.5,2021-12-29 14:47:46.141,135,food&healh vendor_3 from russia,3,food&healh,russia,in_progress,booked,2022-01-02 21:21:08.844221,102.55,1p:train,0.025,norway,0.04,3:vspn-3023:0.05:0.01
6,6,14166,131226,3.73,2021-10-31 07:05:50.404,75,food&healh vendor_1 from norway,1,food&healh,norway,in_progress,booked,2021-11-01 07:05:50.404000,24.0,1p:airplane,0.04,germany,0.01,0:vspn-4092:0.14:0.02
7,7,10021,92756,5.27,2021-10-06 23:27:52.573,76,food&healh vendor_1 from russia,1,food&healh,russia,in_progress,booked,2021-10-07 23:27:52.573000,24.0,3p:airplane,0.035,germany,0.01,4:vspn-1909:0.03:0.03
8,8,3839,36640,4.79,2021-09-02 02:42:48.067,76,food&healh vendor_1 from russia,1,food&healh,russia,in_progress,booked,2021-09-03 18:37:43.059556,39.9,1p:train,0.025,germany,0.01,4:vspn-1909:0.03:0.03
9,9,5597,52507,5.58,2021-09-08 04:47:59.753,76,food&healh vendor_1 from russia,1,food&healh,russia,in_progress,booked,2021-09-10 01:29:58.337788,44.68,1p:train,0.025,germany,0.01,4:vspn-1909:0.03:0.03
10,10,23388,215915,8.61,2021-12-18 09:41:19.969,149,food&healh vendor_1 from russia,1,food&healh,russia,in_progress,booked,2021-12-28 14:16:05.720697,244.57,1p:train,0.025,norway,0.04,4:vspn-1909:0.03:0.03
'''
## Features of the data:
About 2% of orders are executed late, and this is the norm for vendors. Vendor #3, which has a percentage of 10%, is most likely unreliable.
About 2% of orders do not reach the clients.
It is also normal that about 1.5% of the orders are returned by the clients. However, vendor number 21 has 50% of returns. He sells electronics and laptops - evidently his products are defective and worth examining.

## Task.
1. Create a directory of shipping_country_rates from the data specified in shipping_country and shipping_country_base_rate, make the primary key of the table a serial id, i.e. a serial identifier of each line. The directory should consist of unique pairs of fields from the shipping table.
2. Create a directory of vendor shipping rates by shipping_agreement from the vendor_agreement_description row data, separated by ":" (colon without quotation marks).
The field names: 
 
agreement_id,
agreement_number,
agreement_rate,
agreement_commission.
Make agreement_id the primary key.

3. Create a shipping_transfer type guide from the shipping_transfer_description line with a ":" delimiter (colon without quotes). 
 The field names are: 
transfer_type,
transfer_model,
shipping_transfer_rate .
Make serial id the primary key of the table.

Create a table shipping_info, a directory of country commissions, with unique shipping_id and link it with the created directories shipping_country_rates, shipping_agreement, shipping_transfer and constant shipping information shipping_plan_datetime, payment_amount, vendor_id.

4. Create a shipping_status table and include information from the shipping log (status , state). Add there computable information on actual delivery time shipping_start_fact_datetime, shipping_end_fact_datetime . Reflect for each unique shipping_id its final delivery status.

5. Create a shipping_datamart view based on ready-made tables for analytics and include:
shipping_id
vendor_id
transfer_type - delivery type from the shipping_transfer table
full_day_at_shipping - the number of full days of shipping.
Calculated as: shipping_end_fact_datetime -- shipping_start_fact_datetime
is_delay - the status indicating whether the delivery is overdue.
is_shipping_finish - status showing if delivery is complete. 
delay_day_at_shipping - The number of days the delivery was overdue.
payment_amount - the amount of the user's payment
vat - total delivery tax
profit - the total profit of the company from delivery.
It is calculated as: payment_amount *∗ agreement_commission

Summary view of the table
file target_schema_data.png