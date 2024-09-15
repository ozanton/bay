## ETL and data preparation automation
Step 1

Input data.
New increments with information about sales come via API, specification to which is given below, and contain the order status (shipped/refunded).
API specification
POST /generate_report

Generated report contains four files:
custom_research.csv,
user_order_log.csv,
user_activity_log.csv,
price_log.csv.

The generated increment contains four files:
custom_research_inc.csv,
user_order_log_inc.csv,
user_activity_log_inc.csv,
price_log_inc.csv.

Adapt the pipelining for the current task:
To account for shipped and refunded statuses in mart.f_sales storefront. All data in the storefront should be considered shipped.
Update pipeline with statuses and backward compatibility.

Step 2
The customer retention study requires creating a datamart that should reflect the following information:
The period under consideration is weekly.
Customer retention:
new - the number of customers who have placed an order in the examined period;
returning - the number of clients who placed more than one order in the examined period;
refunded - the number of clients who returned the order during the period.
Revenue and refunded for each customer category.

Thanks to datamart it is possible to find out, which categories of goods keep the clients better.

Scheme of the mart.f_customer_retention datamart:
- new_customers_count - the number of new clients (those who have made only one 
order for the considered period of time).
- returning_customers_count - the number of returning clients (those who have made only one order within the period being considered,
that have only made several orders at a period of time under consideration).
- refunded_customer_count - the number of clients who returned during the period under consideration. 
the period under consideration.
- period_name - weekly.
- period_id - the period identifier (the number of week or month).
- item_id - the category identifier of the product.
- new_customers_revenue - the income from the new clients.
- returning_customers_revenue - the income from the returned clients.
- customers_refunded - the number of returned clients. 

The task is to fill mart.f_customer_retention with data on "returning customers" by weeks.

Step 3
The task is to meet the idempotency condition, restart the pipeline and make sure that there are no duplicates in the mart.f_sales and mart.f_customer_retention windows after the restart.


