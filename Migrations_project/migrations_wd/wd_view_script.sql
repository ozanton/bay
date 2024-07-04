--DROP VIEW IF EXISTS wd_wind_daily, wd_temp_daily, wd_dm_daily;

create view wd_wind_daily as (WITH WindData AS (
    SELECT 
        dc.name_city,
        dd.full_date AS date,
        CAST(CASE WHEN ww.wind_u = 0.0 THEN 0.00001
                WHEN ww.wind_u = -0.0 THEN -0.00001
                ELSE ww.wind_u
            END AS DECIMAL(10, 1)
        ) AS wind_u,
        CAST(CASE WHEN ww.wind_v = 0.0 THEN 0.00001
                WHEN ww.wind_v = -0.0 THEN -0.00001
                ELSE ww.wind_v
            END AS DECIMAL(10, 1)
        ) AS wind_v,
        CAST(SQRT(POW(CASE WHEN ww.wind_u = 0.0 THEN 0.00001
                        WHEN ww.wind_u = -0.0 THEN -0.00001
                        ELSE ww.wind_u
                    END::DOUBLE PRECISION, 2
                ) + POW(CASE WHEN ww.wind_v = 0.0 THEN 0.00001
                        WHEN ww.wind_v = -0.0 THEN -0.00001
                        ELSE ww.wind_v
                    END::DOUBLE PRECISION, 2
                )
            ) AS DECIMAL(10, 1)
        ) AS wind_speed,
        CASE 
            WHEN dt.hour_24 = 12 THEN 90 - CAST(ATAN(ww.wind_v / ww.wind_u) / PI() * 180 AS INTEGER)
            ELSE NULL
        END AS wind_dir
    FROM 
        wd_wind ww
    JOIN 
        dict_cities dc ON ww.id_city = dc.id_city
        join dict_date dd on ww.date_id = dd.date_id
        join dict_time dt on ww.time_id = dt.time_id
)
SELECT 
    name_city,
    date,
    MIN(wind_speed) AS min_wind_speed,
    MAX(wind_speed) AS max_wind_speed,
    (SELECT DISTINCT wind_dir FROM WindData wd WHERE wd.name_city = WindData.name_city AND wd.date = WindData.date AND wind_dir IS NOT NULL LIMIT 1) AS wind_dir
FROM 
    WindData
WHERE 
    date >= current_date AND date < current_date + INTERVAL '1 day'
GROUP BY 
    name_city, date);
    
create view wd_temp_daily as (select 
	dc.name_city,
    dd.full_date AS date,
    MIN(ROUND(CAST(temp - 273.15 AS NUMERIC), 1)) AS min_temp_c,
    AVG(ROUND(CAST(temp - 273.15 AS NUMERIC), 1)) AS avg_temp_c,
    MAX(ROUND(CAST(temp - 273.15 AS NUMERIC), 1)) AS max_temp_c,
    MIN(ROUND(CAST(dewpoint - 273.15 AS NUMERIC), 1)) AS min_dewpoint,
    MAX(ROUND(CAST(dewpoint - 273.15 AS NUMERIC), 1)) AS max_dewpoint,
    CAST(MAX(gust) AS numeric(5, 1)) AS max_gust,
    CAST(AVG(wr.rh) AS INTEGER) AS avg_relative_humidity,
    SUM(wp.past3hprecip) AS total_precip,
	CAST(AVG(pw.pressure) AS numeric(10, 1)) AS avg_pressure,
    max(pt.ptype) as prevalier_ptype,
    CAST(AVG((lclouds + mclouds + hclouds)) AS NUMERIC(5, 1)) AS avg_clouds
from wd_temp wt 
join dict_cities dc on wt.id_city = dc.id_city 
	join dict_date dd on wt.date_id = dd.date_id
        join dict_time dt on wt.time_id = dt.time_id
    left join wd_dewpoint dw on wt.id_city = dw.id_city and wt.date_id = dw.date_id and wt.time_id = dw.time_id
    left join wd_gust wg on wt.id_city = wg.id_city and wt.date_id = wg.date_id and wt.time_id = wg.time_id
    left join wd_precip wp on wt.id_city = wp.id_city and wt.date_id = wp.date_id and wt.time_id = wp.time_id
    left join wd_rh wr on wt.id_city = wr.id_city and wt.date_id = wr.date_id and wt.time_id = wr.time_id
    left join wd_pressure pw on wt.id_city = pw.id_city and wt.date_id = pw.date_id and wt.time_id = pw.time_id
    left join wd_ptype pt on wt.id_city = pt.id_city and wt.date_id = pt.date_id and wt.time_id = pt.time_id
    left join wd_clouds wc on wt.id_city = wc.id_city and wt.date_id = wc.date_id and wt.time_id = wc.time_id
WHERE 
    dd.full_date >= current_date AND dd.full_date < current_date + INTERVAL '1 day'
GROUP BY dc.name_city, dd.full_date
order by dd.full_date desc);

create view wd_dm_daily as (select wtd.name_city,
    wtd.date,
    wtd.min_temp_c,
    wtd.avg_temp_c,
    wtd.max_temp_c,
    wtd.min_dewpoint,
    wtd.max_dewpoint,
    wwd.min_wind_speed,
    wwd.max_wind_speed,
    wwd.wind_dir,
    wtd.max_gust,
    wtd.avg_relative_humidity,
    wtd.total_precip,
    wtd.avg_pressure,
    wtd.prevalier_ptype,
    wtd.avg_clouds 
from wd_temp_daily wtd
join wd_wind_daily wwd 
	on wtd.name_city = wwd.name_city and wtd.date = wwd.date
order by wtd.date desc);

INSERT INTO wd_dm_alldays (name_city, date, min_temp_c, avg_temp_c, max_temp_c, min_dewpoint, max_dewpoint, min_wind_speed, max_wind_speed, wind_dir, max_gust, avg_relative_humidity, total_precip, avg_pressure, prevalier_ptype, avg_clouds)
select * 
from wd_dm_daily