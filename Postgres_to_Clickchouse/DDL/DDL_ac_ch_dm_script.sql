

-- Comparison of the number of calls for the current and previous days at the current hour

CREATE TABLE IF NOT EXISTS dm_calls_comparison_day (
    calls_today UInt64,          
    calls_previous_day UInt64,   
    percentage_change Float64    
) 
ENGINE = MergeTree()
ORDER BY calls_today;  


-- Comparison of the number of calls for the current day with the same day of the previous week

CREATE TABLE IF NOT EXISTS dm_calls_comparison_weekly (
    calls_today UInt64,                  
    calls_same_day_last_week UInt64,     
    percentage_change Float64            
) 
ENGINE = MergeTree()
ORDER BY calls_today;  


-- The number of calls today at the current hour, compared to the number of calls on the same day of the week at the current hour.

CREATE TABLE IF NOT EXISTS dm_calls_comparison_hourly_weekly (
    calls_current_hour_today UInt64,         
    calls_same_hour_same_day_last_week UInt64, 
    percentage_change Float64                
) 
ENGINE = MergeTree()
ORDER BY calls_current_hour_today;  

-- Number of calls during the week compared to the previous week.

CREATE TABLE IF NOT EXISTS dm_calls_comparison_weekly_weekly (
    calls_this_week UInt64,       
    calls_last_week UInt64,       
    percentage_change Float64     
) 
ENGINE = MergeTree()
ORDER BY calls_this_week;

-- Number of calls during the month compared to the previous month.

CREATE TABLE IF NOT EXISTS dm_calls_comparison_monthly_monthly (
    calls_this_month UInt64,      
    calls_last_month UInt64,      
    percentage_change Float64     
) 
ENGINE = MergeTree()
ORDER BY calls_this_month;  

-- Number of calls by month

CREATE TABLE IF NOT EXISTS dm_monthly_call_counts (
    year UInt16,            
    month String,           
    call_count UInt64       
) 
ENGINE = MergeTree()
ORDER BY (year, month);  

-- Comparison of the Number of Calls by Months of the Current Year to the Previous One

CREATE TABLE IF NOT EXISTS dm_comparison_year_monthly (
    month String,                     
    year UInt16,                      
    call_count_current_year UInt64,   
    call_count_previous_year UInt64,  
    percentage_change Float64         
) 
ENGINE = MergeTree()
ORDER BY (year, month); 

-- Number of missed calls by day during the month.

CREATE TABLE IF NOT EXISTS dm_daily_missed_and_callback_calls (
    day Date,                         
    missed_call_count UInt64,         
    callback_call_count UInt64        
) 
ENGINE = MergeTree()
ORDER BY day; 

-- Number of people registered by day during the month, compared with the previous month.

CREATE TABLE IF NOT EXISTS dm_daily_recorded_people_comparison (
    day Date,                              
    recorded_people_current_month UInt64,  
    recorded_people_previous_month UInt64, 
    percentage_change Float32              
) 
ENGINE = MergeTree()
ORDER BY day;  


