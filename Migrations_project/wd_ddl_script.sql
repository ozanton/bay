
--Creating tables new scheme (target scheme wd)

--drop table if exists wd_temp,
--                     wd_dewpoint,
--                     wd_precip, 
--                     wd_wind,
--                     wd_gust,
--					   wd_ptype,
--					   wd_rh,
--					   wd_pressure,
--					   wd_clouds;
                    
select * from dict_date;

select * from dict_time;

CREATE TABLE wd_temp (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    temp float NOT NULL,            
    CONSTRAINT wd_temp_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_dewpoint (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    dewpoint float NOT NULL,            
    CONSTRAINT wd_dewpoint_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_precip (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    past3hprecip float NOT null, 
    past3hsnowprecip float NOT null,
    past3hconvprecip float NOT NULL,            
    CONSTRAINT wd_precip_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_wind (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    wind_u float NOT NULL,
    wind_v float NOT NULL, 
    CONSTRAINT wd_wind_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_gust (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    gust float NOT NULL,            
    CONSTRAINT wd_gust_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_ptype (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    ptype float NOT NULL,            
    CONSTRAINT wd_ptype_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_rh (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    rh float NOT NULL,            
    CONSTRAINT wd_rh_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_pressure (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    pressure float NOT NULL,            
    CONSTRAINT wd_pressure_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wd_clouds (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    lclouds float NOT NULL,
    mclouds float NOT NULL,
    hclouds float NOT NULL,
    CONSTRAINT wd_clouds_pkey PRIMARY KEY (id_city, date_id, time_id)      
);


--select * from wd_temp
--select * from wd_dewpoint
--select * from wd_precip
--select * from wd_wind
--select * from wd_gust
--select * from wd_ptype
--select * from wd_rh
--select * from wd_pressure
--select * from wd_clouds
