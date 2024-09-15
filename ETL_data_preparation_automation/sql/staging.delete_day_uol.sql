delete from staging.user_order_log
where date_time::date = '{{ds}}'::date;