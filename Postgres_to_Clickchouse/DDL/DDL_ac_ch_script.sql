use ats_calls;

-- Таблица приемник данных из Postgres

CREATE TABLE external_calls
(
    call_id UInt32,
    date Date,
    time String, 
    caller_number String,
    operator String,
    receiver_number String,
    caller_name String,
    call_duration Int32,
    call_answered UInt8, 
    call_result UInt8, 
    ad_channel String,
    outgoing_call UInt8, 
    outgoing_call_duration Int32
)
ENGINE = MergeTree()
ORDER BY call_id;

--Нормализованные данные в ClickHouse
-- Таблица для уникальных мобильных операторов
drop table cd_operators;

CREATE TABLE cd_operators
(
    operator_id UInt32,
    operator_name String,
    PRIMARY KEY (operator_id)
) ENGINE = MergeTree()
ORDER BY operator_id;

-- Таблица для уникальных каналов рекламы

CREATE TABLE cd_adchannels
(
    ad_channel_id UInt32,
    ad_channel_name String,
    PRIMARY KEY (ad_channel_id)
) ENGINE = MergeTree()
ORDER BY ad_channel_id;

-- Таблица для имен операторов, принимающих звонки

CREATE TABLE cd_callernames
(
    caller_name_id UInt32,    
    caller_name String,       
    PRIMARY KEY (caller_name_id)
) ENGINE = MergeTree()
ORDER BY caller_name_id;

-- Таблица для хранения времени и даты
DROP table cd_time;

CREATE TABLE cd_time
(
    time_id UInt32,                  
    full_time String,             
    full_time_24 String,           
    hour UInt8,                      
    hour_24 UInt8,                   
    minute UInt8,                    
    second UInt8,                    
    am_pm String                     
) ENGINE = MergeTree()
ORDER BY (time_id);

CREATE TABLE cd_date (
    date_id UInt32,                   
    full_date Date,                  
    day UInt8,                       
    month UInt8,                     
    year UInt16,                     
    day_of_week UInt8,              
    day_name String,                
    month_name String,               
    is_weekend UInt8,                
    PRIMARY KEY (date_id)            
) ENGINE = MergeTree()
ORDER BY (date_id);

-- Создание таблицы фактов calls в ClickHouse
CREATE TABLE calls (
    call_id UInt32,
    date_id UInt32,
    time_id UInt32,
    caller_number String,
    operator_id UInt32,
    receiver_number String,
    caller_name_id UInt32,
    call_duration Int32,
    call_answered UInt8,
    call_result UInt8,
    ad_channel_id UInt32,
    outgoing_call UInt8,
    outgoing_call_duration Int32
) ENGINE = MergeTree() ORDER BY call_id;

