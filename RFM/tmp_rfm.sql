with 
rfm_row as (SELECT user_id,
                    current_date - MAX(order_ts::date) AS R,
       				COUNT(order_id) AS F,
       				SUM(payment) AS M
		     FROM analysis.orders
             WHERE status = '4'
             GROUP BY user_id)
SELECT user_id,
	   NTILE(5) OVER(ORDER BY R) as recency,
	   NTILE(5) OVER(ORDER BY F) as frequency,
	   NTILE(5) OVER(ORDER BY M) as monetary_value
FROM rfm_row
order by user_id