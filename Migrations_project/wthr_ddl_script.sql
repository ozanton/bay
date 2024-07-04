
--Creating tables new scheme (target scheme)

--drop table if exists wthr_temp,
--                     wthr_hum,
--                     wthr_wind, 
--                     wthr_winddir,
--                     wthr_cloud;
                    
CREATE TABLE dict_date (
    date_id SERIAL PRIMARY KEY,       
    full_date DATE NOT NULL,          
    day SMALLINT NOT NULL,            
    month SMALLINT NOT NULL,          
    year SMALLINT NOT NULL,           
    day_of_week SMALLINT NOT NULL,    
    day_name VARCHAR(9) NOT NULL,     
    month_name VARCHAR(9) NOT NULL,   
    is_weekend BOOLEAN NOT NULL       
);

CREATE TABLE dict_time (
    time_id SERIAL PRIMARY KEY,     
    full_time TIME NOT NULL,        
    full_time_24 TIME NOT NULL,		
    hour SMALLINT NOT NULL,         
    hour_24 SMALLINT NOT null,      
    minute SMALLINT NOT NULL,       
    second SMALLINT NOT NULL,       
    am_pm VARCHAR(2) NOT NULL       
);

CREATE TABLE wthr_temp (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    temp_c float NOT NULL,            
    CONSTRAINT wthr_temp_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wthr_hum (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    humidity float NOT NULL,            
    CONSTRAINT wthr_hum_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wthr_wind (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    wind_kph float NOT NULL,            
    CONSTRAINT wthr_wind_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wthr_winddir (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    wind_direction float NOT NULL,            
    CONSTRAINT wthr_winddir_pkey PRIMARY KEY (id_city, date_id, time_id)      
);

CREATE TABLE wthr_cloud (
    id_city integer not null,
	date_id integer NOT NULL,       
    time_id integer NOT NULL,          
    cloud float NOT NULL,            
    CONSTRAINT wthr_cloud_pkey PRIMARY KEY (id_city, date_id, time_id)      
);


--select * from wthr_temp
--select * from wthr_hum
--select * from wthr_wind
--select * from wthr_winddir
--select * from wthr_cloud