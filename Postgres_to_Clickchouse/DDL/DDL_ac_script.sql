TRUNCATE table calls;
drop TABLE calls;

CREATE TABLE raw_ats_data (
    id SERIAL PRIMARY KEY,                          
    raw_data JSONB NOT NULL,                        -- Сырые данные в формате JSONB
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- Время загрузки данных
    processing_status VARCHAR(50) DEFAULT 'unprocessed', -- Статус обработки данных
    error_message TEXT                              -- Сообщение об ошибке при обработке данных
);

-- Таблица для звонков:
CREATE TABLE calls (
    call_id SERIAL PRIMARY KEY,
    date DATE,
    time TIME WITH TIME ZONE,
    caller_number VARCHAR,
    operator VARCHAR,
    receiver_number VARCHAR,
    caller_name VARCHAR,
    call_duration INTEGER,
    call_answered BOOLEAN,
    call_result BOOLEAN,
    ad_channel VARCHAR,
    outgoing_call BOOLEAN,
    outgoing_call_duration INTEGER
);

-- Таблица для статусов обработки:
CREATE TABLE call_status (
    id INTEGER,
    status VARCHAR,
    error_message TEXT
);

