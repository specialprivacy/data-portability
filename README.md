# Data portability

## Description
This docker container hosts a service handling all data subject requests regarding portability, erasure and rectification.

## Configuration
* KAFKA_TIMEOUT: The number of milliseconds to wait for a Kafka connection, by default "5000".
* DATA_PORTABILITY_REQUESTS_TOPIC: The topic on which the data subject requests are sent, by default "data-portability-requests".
* KAFKA_BROKER_LIST: The URL where the Kafka service can be accessed, by default "localhost:9092".
