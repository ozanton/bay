ALTER TABLE staging.user_order_log ADD COLUMN status varchar(10);
UPDATE staging.user_order_log SET status='shipped' WHERE status IS NULL;

ALTER TABLE mart.f_sales ADD COLUMN status varchar(10);