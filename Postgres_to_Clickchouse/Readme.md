# Project overview:

1. Loading data in Postgres into staging layer .
2. Analytical database design. Creating a data vault model in Clickhouse.
3. Answering business questions with an analytic database.


Loading ATS call data into the database and call distribution among call center operators of a medical center for analyzing the uniform distribution of the load on operators and analyzing customer acquisition through marketing channels for the purpose of dynamic pricing.

dag_ATS_call - pipeline for loading ATS call data.

Raw data layer in Postgres:

raw_ats_data - the table stores data received from the source in unprocessed form in JSONb format.
calls - the table stores transformed data from JSONb for further loading and analysis.
calls_status - data processing status.

dag_data_upcall_psql - pipeline for updating call data.

dag_data_psql_to_ch - pipeline for reloading data into the receiver table in the Clickhouse analytical 

database.

The normalized data layer in Clickhouse contains tables of date and time values, call handlers and operators, as well as a fact table with call data - calls.

dag_data_upсall_ch - update pipeline for transforming and normalizing call data.

Data mart analytics layer in Clickhouse for dashboards:

hourly:
dm_calls_comparison_day
dm_calls_comparison_hourly_weekly

dag_dm_call_ch_hourly - data mart update pipeline every hour

daily:
dm_calls_comparison_weekly
dm_calls_comparison_monthly_monthly
dm_calls_comparison_weekly_weekly
dm_monthly_call_counts
dm_comparison_year_monthly
dm_daily_missed_and_callback_calls
dm_daily_recorded_people_comparison
dm_call_distribution_by_ad_channel
dm_call_distribution_by_operator

dag_dm_call_ch_daily - daily data mart update pipeline.