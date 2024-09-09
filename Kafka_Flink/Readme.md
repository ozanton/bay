# Overview

Using Apache Flink to read data from two different Kafka topics and then merge them based on certain features or criteria.

CDR topic data contains information about the client activity on the internet connection, including the client ID, its IP address, connection port, start IP address, end IP address, start date-time, and end date-time, which are used for various telecommunications and network analysis. Data characteristics and types:

cdr
|-- CUSTOMER_ID: string
|-- PRIVATE_IP: string
|-- START_REAL_PORT: Integer
|-- END_REAL_PORT: Integer
|-- START_DATETIME: timestamp
|-- END_DATETIME: timestamp

NetFlow topic data (a network protocol used to collect and analyze network traffic data from Internet routers that includes flow start time, duration, source address, destination address, source port, destination port, outgoing bytes) that can provide insight into network usage, security, and performance by tracking the flow of data packets on the network.

netflow
|-- NETFLOW_DATETIME: string
|-- SOURCE_ADDRESS: string
|-- SOURCE_PORT: Integer
|-- IN_BYTES: integer

Stream processing is done in real time using Apache Kafka and Apache Flink. The processed data is then sent to a third Kafka topic, the "result". This process is eventually followed by the creation of reports and visualizations based on specific requirements.

result
|-- CUSTOMER_ID: string
|-- SUM IN_BYTES: integer
