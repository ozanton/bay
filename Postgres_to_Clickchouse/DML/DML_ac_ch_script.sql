--Наполнение таблиц:
INSERT INTO cd_date
WITH
    toDayOfWeek(toDate('2022-01-01') + toIntervalDay(number)) AS day_of_week,
    toMonth(toDate('2022-01-01') + toIntervalDay(number)) AS month_number
SELECT
    number + 1 AS date_id,
    toDate('2022-01-01') + toIntervalDay(number) AS full_date,
    dayOfMonth(toDate('2022-01-01') + toIntervalDay(number)) AS day,
    month_number AS month,
    year(toDate('2022-01-01') + toIntervalDay(number)) AS year,
    day_of_week,
    case day_of_week
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        when 7 then 'Sunday'
    end AS day_name,
    case month_number
        when 1 then 'January'
        when 2 then 'February'
        when 3 then 'March'
        when 4 then 'April'
        when 5 then 'May'
        when 6 then 'June'
        when 7 then 'July'
        when 8 then 'August'
        when 9 then 'September'
        when 10 then 'October'
        when 11 then 'November'
        when 12 then 'December'
    end AS month_name,
    if(day_of_week IN (6, 7), 1, 0) AS is_weekend
FROM numbers(toInt32(toDate('2024-12-31') - toDate('2022-01-01')) + 1);


INSERT INTO cd_time
SELECT
    number AS time_id,
    concat(
        lpad(toString(((toHour(toDateTime(number)) - 1) % 12) + 1), 2, '0'), ':',
        lpad(toString(toMinute(toDateTime(number))), 2, '0'), ':',
        lpad(toString(toSecond(toDateTime(number))), 2, '0')
    ) AS full_time, -- 12-часовой формат без AM/PM
    concat(
        lpad(toString(toHour(toDateTime(number))), 2, '0'), ':',
        lpad(toString(toMinute(toDateTime(number))), 2, '0'), ':',
        lpad(toString(toSecond(toDateTime(number))), 2, '0')
    ) AS full_time_24, 
    ((toHour(toDateTime(number)) - 1) % 12) + 1 AS hour, 
    toHour(toDateTime(number)) AS hour_24, 
    toMinute(toDateTime(number)) AS minute, 
    toSecond(toDateTime(number)) AS second, 
    if(toHour(toDateTime(number)) < 12, 'AM', 'PM') AS am_pm 
FROM
(
    SELECT
        number
    FROM system.numbers
    LIMIT 86400 
) AS subquery
WHERE number < 86400; --

INSERT INTO cd_operators (operator_id, operator_name)
SELECT
    rowNumberInAllBlocks() AS operator_id,
    operator
FROM (SELECT DISTINCT operator
    FROM external_calls); 
   
INSERT INTO cd_callernames (caller_name_id, caller_name)
SELECT
    rowNumberInAllBlocks() + 9 AS caller_name_id,  
    caller_name
FROM (SELECT DISTINCT caller_name
    FROM external_calls) AS unique_callernames;
   
INSERT INTO cd_adchannels (ad_channel_id, ad_channel_name)
SELECT
    rowNumberInAllBlocks() + 19 AS ad_channel_id,  
    ad_channel_name
FROM (SELECT DISTINCT ad_channel AS ad_channel_name
    FROM external_calls) AS unique_adchannels;


INSERT INTO external_calls (
	call_id, 
	date, 
    time,
	caller_number, 
	operator, 
	receiver_number, 
	caller_name, 
	call_duration, 
	call_answered, 
	call_result, 
	ad_channel, 
	outgoing_call, 
	outgoing_call_duration) FROM postgresql(host:port, db, tab, logg, pass);
    VALUES

INSERT INTO calls(call_id, date_id, time_id, caller_number, operator_id, receiver_number, caller_name_id,
    call_duration, call_answered, call_result, ad_channel_id, outgoing_call, outgoing_call_duration)
SELECT 
    ec.call_id, 
    cd.date_id, 
    ct.time_id, 
    ec.caller_number, 
    co.operator_id, 
    ec.receiver_number, 
    cn.caller_name_id, 
    ec.call_duration, 
    ec.call_answered, 
    ec.call_result, 
    ac.ad_channel_id, 
    ec.outgoing_call, 
    ec.outgoing_call_duration
FROM external_calls AS ec
INNER JOIN cd_date AS cd 
    ON ec.date = cd.full_date
INNER JOIN cd_time AS ct 
    ON substring(ec.time, 1, 8) = ct.full_time_24 
LEFT JOIN cd_adchannels AS ac 
    ON ec.ad_channel = ac.ad_channel_name
LEFT JOIN cd_operators AS co 
    ON ec.operator = co.operator_name
LEFT JOIN cd_callernames AS cn 
    ON ec.caller_name = cn.caller_name;



















   

