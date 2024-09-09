from pyflink.table import EnvironmentSettings, TableEnvironment

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(settings)
table_env.get_config().set("parallelism.default", "1")

table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS default_catalog.default_database.cdr_table (
        CUSTOMER_ID STRING,
        PRIVATE_IP STRING,
        START_REAL_PORT INT,
        END_REAL_PORT INT,
        START_DATETIME TIMESTAMP(3),
        END_DATETIME TIMESTAMP(3),
        WATERMARK FOR START_DATETIME AS START_DATETIME - INTERVAL '20' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'cdr',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test-consumer-group',
        'format' = 'csv',
        'csv.field-delimiter' = ',',
        'csv.ignore-parse-errors' = 'true',  
        'scan.startup.mode' = 'earliest-offset'
    )
""")

table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS default_catalog.default_database.netflow_table (
        NETFLOW_DATETIME TIMESTAMP(3),
        SOURCE_ADDRESS STRING,
        SOURCE_PORT INT,
        IN_BYTES INT,
        WATERMARK FOR NETFLOW_DATETIME AS NETFLOW_DATETIME - INTERVAL '20' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'netflow',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test-consumer-group',
        'format' = 'csv',
        'csv.field-delimiter' = ',',
        'csv.ignore-parse-errors' = 'true', 
        'scan.startup.mode' = 'earliest-offset'
    )
""")

table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS default_catalog.default_database.result_table (
        CUSTOMER_ID STRING,
        total_bytes BIGINT,
        PRIMARY KEY (CUSTOMER_ID) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'result',
        'properties.bootstrap.servers' = 'localhost:9092',
        'key.format' = 'json',
        'value.format' = 'json'
    )
""")

table_env.execute_sql("""
    INSERT INTO default_catalog.default_database.result_table
    SELECT 
        c.CUSTOMER_ID, 
        SUM(n.IN_BYTES) AS total_bytes 
    FROM default_catalog.default_database.cdr_table AS c
    JOIN default_catalog.default_database.netflow_table AS n
    ON c.PRIVATE_IP = n.SOURCE_ADDRESS
    GROUP BY c.CUSTOMER_ID, TUMBLE(n.NETFLOW_DATETIME, INTERVAL '1' MINUTE)
""")
