

--select * from dict_date
--select * from dict_time
--------------------------------------------  

INSERT INTO wd_temp (id_city, date_id, time_id, temp)
SELECT 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.temp
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME);   


INSERT INTO wd_dewpoint (id_city, date_id, time_id, dewpoint)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.dewpoint
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 
                
INSERT INTO wd_precip (id_city, date_id, time_id, past3hprecip, past3hsnowprecip, past3hconvprecip)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.past3hprecip,
    wf.past3hsnowprecip, 
    wf.past3hconvprecip
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 
                
INSERT INTO wd_wind (id_city, date_id, time_id, wind_u, wind_v)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.wind_u,
    wf.wind_v
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 
                
INSERT INTO wd_gust (id_city, date_id, time_id, gust)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.gust
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 
                
INSERT INTO wd_ptype (id_city, date_id, time_id, ptype)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.ptype
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 

               
INSERT INTO wd_rh (id_city, date_id, time_id, rh)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.rh
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 
                
INSERT INTO wd_pressure (id_city, date_id, time_id, pressure)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.pressure
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME); 
                
INSERT INTO wd_clouds (id_city, date_id, time_id, lclouds, mclouds, hclouds)
select 
    wf.id_city,
    d.date_id,
    t.time_id,
    wf.lclouds,
    wf.mclouds,
    wf.hclouds
FROM windy_forecast wf
JOIN dict_date d ON d.full_date = CAST(wf."timestamp" AS DATE)
JOIN dict_time t ON t.full_time_24 = CAST(wf."timestamp" AS TIME);