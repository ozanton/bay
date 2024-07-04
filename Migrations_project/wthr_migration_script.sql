
--dict_date
INSERT INTO dict_date (date_id, full_date, day, month, year, day_of_week, day_name, month_name, is_weekend)
SELECT
    TO_CHAR(date::DATE, 'YYYYMMDD')::INTEGER AS date_id,
    date::DATE AS full_date,
    EXTRACT(DAY FROM date::DATE) AS day,
    EXTRACT(MONTH FROM date::DATE) AS month,
    EXTRACT(YEAR FROM date::DATE) AS year,
    EXTRACT(DOW FROM date::DATE) AS day_of_week,
    TO_CHAR(date::DATE, 'FMDay') AS day_name,
    TO_CHAR(date::DATE, 'FMMonth') AS month_name,
    CASE WHEN EXTRACT(DOW FROM date::DATE) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM
    generate_series('2023-01-01'::DATE, '2027-12-31'::DATE, INTERVAL '1 day') AS date;
    
--dict_time   
INSERT INTO dict_time (full_time, full_time_24, hour, hour_24, minute, second, am_pm, time_id)
SELECT
    full_time,
    CAST(TO_CHAR(full_time, 'HH24:MI:SS') AS TIME) AS full_time_24,
    EXTRACT(HOUR FROM full_time) AS hour,
    EXTRACT(HOUR FROM full_time) AS hour_24,
    EXTRACT(MINUTE FROM full_time) AS minute,
    EXTRACT(SECOND FROM full_time) AS second,
    TO_CHAR(full_time, 'AM') AS am_pm,
    TO_CHAR(full_time, 'HH24MISS')::INTEGER AS time_id
FROM (
    SELECT
        full_time,
        ROW_NUMBER() OVER () AS row_num
    FROM
        generate_series('2023-01-01 00:00:00'::timestamp, '2023-01-01 23:59:59'::timestamp, '1 second') AS full_time
) subquery;

--wthr_temp
INSERT INTO wthr_temp (id_city, date_id, time_id, temp_c)
SELECT 
    wtc.id_city,
    d.date_id,
    t.time_id,
    wtc.temp_c
FROM weatherapi_current wtc
JOIN dict_date d ON d.full_date = CAST(wtc.time AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wtc.time AS TIME);

--wthr_hum
INSERT INTO wthr_hum (id_city, date_id, time_id, humidity)
SELECT 
    wtc.id_city,
    d.date_id,
    t.time_id,
    wtc.humidity
FROM weatherapi_current wtc
JOIN dict_date d ON d.full_date = CAST(wtc.time AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wtc.time AS TIME);

--wthr_wind
INSERT INTO wthr_wind (id_city, date_id, time_id, wind_kph)
SELECT 
    wtc.id_city,
    d.date_id,
    t.time_id,
    wtc.wind_kph 
FROM weatherapi_current wtc
JOIN dict_date d ON d.full_date = CAST(wtc.time AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wtc.time AS TIME);

--wthr_winddir
INSERT INTO wthr_winddir (id_city, date_id, time_id, wind_direction)
SELECT 
    wtc.id_city,
    d.date_id,
    t.time_id,
    wtc.wind_direction
FROM weatherapi_current wtc
JOIN dict_date d ON d.full_date = CAST(wtc.time AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wtc.time AS TIME);

--wthr_cloud
INSERT INTO wthr_cloud (id_city, date_id, time_id, cloud)
SELECT 
    wtc.id_city,
    d.date_id,
    t.time_id,
    wtc.cloud
FROM weatherapi_current wtc
JOIN dict_date d ON d.full_date = CAST(wtc.time AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wtc.time AS TIME);

--TRUNCATE TABLE wthr_temp, wthr_hum, wthr_wind, wthr_winddir, wthr_cloud;

