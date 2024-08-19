
INSERT INTO calls (
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
    outgoing_call_duration
)
SELECT
    (json_data->>'datetime')::timestamp::date AS date,
    (json_data->>'datetime')::timestamp::time AS time,
    json_data->>'caller_number' AS caller_number,
    json_data->>'operator' AS operator,
    json_data->>'receiver_number' AS receiver_number,
    json_data->>'caller_name' AS caller_name,
    (json_data->>'call_duration')::integer AS call_duration,
    (CASE 
        WHEN json_data->>'call_answered' IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'on', 'off') 
        THEN (json_data->>'call_answered')::boolean
        ELSE NULL
    END) AS call_answered,
    (CASE 
        WHEN json_data->>'call_result' IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'on', 'off') 
        THEN (json_data->>'call_result')::boolean
        ELSE NULL
    END) AS call_result,
    json_data->>'ad_channel' AS ad_channel,
    (CASE 
        WHEN json_data->>'outgoing_call' IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'on', 'off') 
        THEN (json_data->>'outgoing_call')::boolean
        ELSE NULL
    END) AS outgoing_call,
    (json_data->>'outgoing_call_duration')::integer AS outgoing_call_duration
FROM
    raw_ats_data,
    jsonb_array_elements(raw_data::jsonb) AS json_data;

-- Вставка данных в таблицу call_status:
   
WITH expanded_data AS (
    SELECT
        c.call_id,
        r.id AS raw_id, -- id записи в raw_ats_data
        (json_data->>'datetime')::timestamp AS datetime
    FROM
        raw_ats_data r,
        jsonb_array_elements(r.raw_data::jsonb) AS json_data
    JOIN
        calls c
    ON
        c.datetime = (json_data->>'datetime')::timestamp
)
INSERT INTO call_status (call_id, status, error_message)
SELECT
    call_id,
    'processed' AS status,
    NULL AS error_message
FROM
    expanded_data;

   
 --Обновление статуса в таблице raw_ats_data:  
   
UPDATE raw_ats_data
SET processing_status = 'processed'
WHERE processing_status = 'unprocessed';  