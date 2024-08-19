
--Insert value to data mart:

-- The number of zones for today at the current hour, compared to the previous day (also at this hour).
   
INSERT into dm_calls_comparison_day 
SELECT
    calls_today,
    calls_previous_day,
    CASE 
        WHEN calls_previous_day = 0 THEN NULL
        ELSE (calls_today - calls_previous_day) * 100.0 / calls_previous_day
    END AS percentage_change
FROM (
    SELECT
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
         WHERE ct.hour_24 = toHour(now()) 
           AND cd.full_date = today()) AS calls_today,
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
         WHERE ct.hour_24 = toHour(now() - INTERVAL 1 DAY)
           AND cd.full_date = today() - INTERVAL 1 DAY) AS calls_previous_day
);

-- the number of calls today and the same day last week, as well as the percentage change

INSERT into dm_calls_comparison_weekly
SELECT
    calls_today,
    calls_same_day_last_week,
    CASE 
        WHEN calls_same_day_last_week = 0 THEN NULL
        ELSE (calls_today - calls_same_day_last_week) * 100.0 / calls_same_day_last_week
    END AS percentage_change
FROM (
    SELECT
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
         WHERE ct.hour_24 = toHour(now()) 
           AND cd.full_date = today()) AS calls_today,
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
         WHERE ct.hour_24 = toHour(now())
           AND cd.full_date = today() - INTERVAL 1 WEEK) AS calls_same_day_last_week
);

-- The number of calls today at the current hour, compared to the number of calls on the same day of the week at the current hour.

INSERT into dm_calls_comparison_hourly_weekly
SELECT
    calls_today AS calls_current_hour_today,
    calls_same_hour_same_day_last_week AS calls_same_hour_same_day_last_week,
    CASE 
        WHEN calls_same_hour_same_day_last_week = 0 THEN NULL
        ELSE (calls_today - calls_same_hour_same_day_last_week) * 100.0 / calls_same_hour_same_day_last_week
    END AS percentage_change
FROM (
    SELECT
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
         WHERE ct.hour_24 = toHour(now()) 
           AND cd.full_date = today()) AS calls_today,
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
         WHERE ct.hour_24 = toHour(now()) 
           AND cd.full_date = today() - INTERVAL 1 WEEK) AS calls_same_hour_same_day_last_week
);

-- Number of calls during the week compared to the previous week.

INSERT into dm_calls_comparison_weekly_weekly
SELECT
    calls_current_week AS calls_this_week,
    calls_last_week AS calls_last_week,
    CASE 
        WHEN calls_last_week = 0 THEN NULL
        ELSE (calls_current_week - calls_last_week) * 100.0 / calls_last_week
    END AS percentage_change
FROM (
    SELECT
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         WHERE toStartOfWeek(cd.full_date) = toStartOfWeek(today())) AS calls_current_week,
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         WHERE toStartOfWeek(cd.full_date) = toStartOfWeek(today() - INTERVAL 1 WEEK)) AS calls_last_week
);

-- Number of calls during the month compared to the previous month.

INSERT into dm_calls_comparison_monthly_monthly
SELECT
    calls_current_month AS calls_this_month,
    calls_last_month AS calls_last_month,
    CASE 
        WHEN calls_last_month = 0 THEN NULL
        ELSE (calls_current_month - calls_last_month) * 100.0 / calls_last_month
    END AS percentage_change
FROM (
    SELECT
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         WHERE toStartOfMonth(cd.full_date) = toStartOfMonth(today())) AS calls_current_month,
        (SELECT COUNT(*) 
         FROM calls AS c
         INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
         WHERE toStartOfMonth(cd.full_date) = toStartOfMonth(today() - INTERVAL 1 MONTH)) AS calls_last_month
);

-- Number of calls by month

INSERT into dm_monthly_call_counts (year, month, call_count)
SELECT
    cd.year AS year,
    cd.month_name AS month,
    COUNT(*) AS call_count
FROM calls AS c
INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
WHERE cd.year = toYear(today())
GROUP BY cd.year, cd.month_name
ORDER BY cd.month_name;

-- Comparison of the Number of Calls by Months of the Current Year to the Previous One

INSERT into dm_comparison_year_monthly (month, year, call_count_current_year, call_count_previous_year, percentage_change)
WITH 
    current_year_calls AS (
        SELECT
            cd.month_name AS month,
            cd.year AS year,
            COUNT(*) AS call_count_current_year
        FROM calls AS c
        INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
        WHERE cd.year = toYear(today())
        GROUP BY cd.month_name, cd.year
    ),    
    previous_year_calls AS (
        SELECT
            cd.month_name AS month,
            cd.year AS year,
            COUNT(*) AS call_count_previous_year
        FROM calls AS c
        INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
        WHERE cd.year = toYear(today()) - 1
        GROUP BY cd.month_name, cd.year
    )    
SELECT
    c.month AS month,
    c.year AS year,
    c.call_count_current_year AS call_count_current_year,
    COALESCE(p.call_count_previous_year, 0) AS call_count_previous_year,
    CASE 
        WHEN COALESCE(p.call_count_previous_year, 0) = 0 THEN 
            CASE 
                WHEN COALESCE(c.call_count_current_year, 0) = 0 THEN NULL
                ELSE 100.0 
            END
        ELSE ((COALESCE(c.call_count_current_year, 0) - COALESCE(p.call_count_previous_year, 0)) * 100.0) / COALESCE(p.call_count_previous_year, 1)
    END AS percentage_change
FROM current_year_calls c
LEFT JOIN previous_year_calls p ON c.month = p.month
ORDER BY
    toMonth(
        parseDateTimeBestEffort(
            concat(c.year, '-', 
                   case c.month
                       WHEN 'January' THEN '01'
                       WHEN 'February' THEN '02'
                       WHEN 'March' THEN '03'
                       WHEN 'April' THEN '04'
                       WHEN 'May' THEN '05'
                       WHEN 'June' THEN '06'
                       WHEN 'July' THEN '07'
                       WHEN 'August' THEN '08'
                       WHEN 'September' THEN '09'
                       WHEN 'October' THEN '10'
                       WHEN 'November' THEN '11'
                       WHEN 'December' THEN '12'
                   END, '-01')
        )
    ) ASC;

-- Number of missed calls by day during the month.

INSERT into dm_daily_missed_and_callback_calls (day, missed_call_count, callback_call_count)
WITH 
    days_in_month AS (
        SELECT 
            full_date AS day
        FROM cd_date
        WHERE 
            month = month(today())
            AND year = year(today())
    ),
    missed_calls_per_day AS (
        SELECT 
            cd.full_date AS day,
            COUNT(*) AS missed_call_count
        FROM calls AS c
        INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
        INNER JOIN cd_adchannels AS ac ON c.ad_channel_id = ac.ad_channel_id
        WHERE 
            month(cd.full_date) = month(today())
            AND year(cd.full_date) = year(today())
            AND c.call_answered = 0
            AND c.call_result IS NOT NULL
            AND ac.ad_channel_name != 'spam'
        GROUP BY cd.full_date
    ),
    callback_calls_per_day AS (
        SELECT 
            cd.full_date AS day,
            COUNT(*) AS callback_call_count
        FROM calls AS c
        INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
        INNER JOIN cd_adchannels AS ac ON c.ad_channel_id = ac.ad_channel_id
        WHERE 
            month(cd.full_date) = month(today())
            AND year(cd.full_date) = year(today())
            AND c.call_answered = 0
            AND c.outgoing_call = 1
            AND c.call_result IS NOT NULL
            AND ac.ad_channel_name != 'spam'
        GROUP BY cd.full_date
    )
SELECT
    d.day,
    COALESCE(m.missed_call_count, 0) AS missed_call_count,
    COALESCE(c.callback_call_count, 0) AS callback_call_count
FROM days_in_month d
LEFT JOIN missed_calls_per_day m ON d.day = m.day
LEFT JOIN callback_calls_per_day c ON d.day = c.day
ORDER BY d.day;

-- Number of people registered by day during the month, compared with the previous month.

INSERT into dm_daily_recorded_people_comparison (day, recorded_people_current_month, recorded_people_previous_month, percentage_change)
WITH 
    current_month_data AS (
        SELECT 
            cd.full_date AS day,
            COUNT(*) AS recorded_people_current_month
        FROM calls AS c
        INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
        WHERE 
            month(cd.full_date) = month(today())  
            AND year(cd.full_date) = year(today())  
            AND c.call_answered = 1  
            AND c.call_result = 1  
        GROUP BY cd.full_date
    ),
    previous_month_data AS (
        SELECT 
            cd.full_date AS day,
            COUNT(*) AS recorded_people_previous_month
        FROM calls AS c
        INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
        WHERE 
            month(cd.full_date) = month(today()) - 1  
            AND year(cd.full_date) = CASE 
                WHEN month(today()) = 1 THEN year(today()) - 1 
                ELSE year(today()) 
            END  
            AND c.call_answered = 1  
            AND c.call_result = 1  
        GROUP BY cd.full_date
    )
SELECT
    c.day,
    COALESCE(c.recorded_people_current_month, 0) AS recorded_people_current_month,
    COALESCE(p.recorded_people_previous_month, 0) AS recorded_people_previous_month,
    CASE 
        WHEN COALESCE(p.recorded_people_previous_month, 0) = 0 THEN NULL
        ELSE (COALESCE(c.recorded_people_current_month, 0) - COALESCE(p.recorded_people_previous_month, 0)) * 100.0 / COALESCE(p.recorded_people_previous_month, 1)
    END AS percentage_change
FROM current_month_data c
LEFT JOIN previous_month_data p ON c.day = p.day
ORDER BY c.day;
