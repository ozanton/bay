--drop view if exists weather_datamart;

create view weather_datamart as
(WITH 
cloudiness_mapping AS (
  SELECT 
    unnest(ARRAY[0, 25, 75, 100]) AS cloudiness_percent,
    unnest(ARRAY['Clear', 'Partly Cloudy', 'Cloudy', 'Overcast']) AS cloudiness_type
),
daily_weather AS (
  SELECT 
    dd.full_date AS date,
    dc.name_city,
    MIN(wt.temp_c) AS min_temp_c,
    ROUND(avg(wt.temp_c)::numeric, 1) AS avg_temp_c,
    MAX(wt.temp_c) AS max_temp_c,
    ROUND(AVG(wh.humidity)::numeric, 2) AS avg_humidity,
    ROUND(AVG((ww.wind_kph * 0.27777777777777777777777777777778))::numeric, 2) AS avg_wind_ms,
    CASE 
      WHEN MAX(wd.wind_direction) = 0 THEN 'Calm'
      ELSE (
        SELECT direction
        FROM (
          SELECT 
            unnest(ARRAY[0, 45, 90, 135, 180, 225, 270, 315, 360]) AS degrees,
            unnest(ARRAY['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N']) AS direction
        ) AS wind_direction_mapping
        WHERE degrees <= MAX(wd.wind_direction)
        ORDER BY degrees DESC
        LIMIT 1
      )
    END AS prevailing_wind_direction_str,
    MAX(wd.wind_direction) AS prevailing_wind_direction,
    (SELECT cloudiness_type
     FROM cloudiness_mapping cm
     WHERE cm.cloudiness_percent <= MAX(wc.cloud)
     ORDER BY cm.cloudiness_percent DESC
     LIMIT 1) AS prevailing_cloudiness_type,
    MAX(wc.cloud) AS prevailing_cloudiness_percent
  FROM wthr_temp wt
  JOIN dict_cities dc ON wt.id_city = dc.id_city
  JOIN dict_date dd ON wt.date_id = dd.date_id
  JOIN wthr_hum wh ON wt.id_city = wh.id_city AND wt.date_id = wh.date_id AND wt.time_id = wh.time_id
  JOIN wthr_wind ww ON wt.id_city = ww.id_city AND wt.date_id = ww.date_id AND wt.time_id = ww.time_id
  JOIN wthr_winddir wd ON wt.id_city = wd.id_city AND wt.date_id = wd.date_id AND wt.time_id = wd.time_id
  JOIN wthr_cloud wc ON wt.id_city = wc.id_city AND wt.date_id = wc.date_id AND wt.time_id = wc.time_id
  WHERE 
    dd.full_date >= current_date AND dd.full_date < current_date + INTERVAL '1 day'
  GROUP BY dd.full_date, dc.name_city
)
SELECT 
  date,
  name_city,
  min_temp_c,
  avg_temp_c,
  max_temp_c,
  avg_humidity,
  avg_wind_ms,  
  prevailing_wind_direction,
  prevailing_wind_direction_str,
  prevailing_cloudiness_percent,
  prevailing_cloudiness_type
FROM daily_weather
ORDER BY date, name_city);